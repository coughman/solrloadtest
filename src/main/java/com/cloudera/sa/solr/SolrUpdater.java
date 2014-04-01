package com.cloudera.sa.solr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrInputDocument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SolrUpdater {
	static Logger logger = Logger.getLogger(SolrUpdater.class);
	
	static ArrayList<JsonNode> records = new ArrayList<JsonNode>(); 

	private static final String NATIVE_SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"; // e.g. 2007-04-26T08:05:04.789Z
	private static final SimpleDateFormat SOLR_DATE_FORMAT = new SimpleDateFormat(NATIVE_SOLR_DATE_FORMAT);
	
	@Option(name="-mode", aliases="-m", usage="solr server implementation, either cloud (default) or concurrent")
	private String solrServerMode = "cloud";
	
	@Option(name="-zk", usage="zookeeper host")
	private String zkHost = "localhost:2181/solr";
	
	@Option(name="-c", usage="name of Solr collection")
	private String collection;
	
	@Option(name="-f", usage="path of JSON file with data", required=true)
	private File JSONFile;
	
	@Option(name="-t", usage="number of threads, default: 1 thread")
	private int numThreads = 1;
	
	@Option(name="-n", usage="total number of solr documents to update", required=true)
	private int numRecords;
	
	@Option(name="-solr", usage="solr url with collection and shard specified", depends={"-mode"})
	private String solrUrl;
	
	@Option(name="-q", usage="queue/batch size, default: 1000")
	private int batchSize = 1000;
	
	@Option(name="-s", usage="solr schema", required=true)
	private File solrSchemaFile;
	
	static AtomicInteger numRunningThreads = new AtomicInteger(0);

	private static Map<String, String> fieldTypeMap;
	
	public static void main(String[] args) throws IOException, SolrServerException, InterruptedException {
		new SolrUpdater().doMain(args);				
	}

	public static void printExample() {
		System.out.println("\nExample using Solr Cloud (default):");
		System.out.println("java -jar SolrLoadTest.jar " + SolrUpdater.class.getName() + " -zk <zkhost:2181/solr> -c <collection> -f <data_file> -t 10 -n 10000");
		System.out.println("\nExample using Concurrent Solr Server:");
		System.out.println("java -jar SolrLoadTest.jar " + SolrUpdater.class.getName() + " -solr <solr_host:8983/solr/collection_shard1_replica1> -f <data_file> -t 10 -n 10000");
	}
	
	public void doMain(String[] args) throws IOException, SolrServerException, InterruptedException {
		CmdLineParser parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
			
			if (solrServerMode.equals("cloud"))
			{
				if (StringUtils.isEmpty(this.collection)) {
					System.err.println("must specify collection (-c) param in cloud mode\n");
					parser.printUsage(System.err);			
					printExample();
					return;
				}
			}
			else {
				if (StringUtils.isEmpty(this.solrUrl)) {
					System.err.println("must specify solr url (-solr) param\n");
					parser.printUsage(System.err);			
					printExample();
					return;
				}					
			}
		
			loadInputFileIntoMemory(this.JSONFile);
			fieldTypeMap = loadSolrSchema(solrSchemaFile);
			
			if (solrServerMode.equals("cloud"))
				doCloudSolrUpdate();
			else
				doConcurrentUpdate(numRecords, solrUrl, batchSize, numThreads);
								
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);		
			printExample();
		} catch (Exception e) {
			System.err.println("failed to parse solr schema from file: " + solrSchemaFile);
			parser.printUsage(System.err);		
			printExample();
		}
	}
	
	private void doCloudSolrUpdate() throws SolrServerException, IOException, InterruptedException {
		logger.info("using CloudSolrServer class for indexing via zookeeper: " + this.zkHost);
		CloudSolrServer solrServer = new CloudSolrServer(this.zkHost);
		solrServer.setDefaultCollection(this.collection);
		
		SolrPingResponse response = solrServer.ping();
		logger.debug("solr server response: " + response);
		
		int numRecordsLeft = this.numRecords;
		
		if (numRecords < numThreads) {
			logger.error("number of records must be larger than number of threads.  Aborting...");
			return;
		}
		
		int numRecordsPerThread = (int) Math.ceil(1.0 * numRecords / numThreads);
		logger.debug("number of records per thread: " + numRecordsPerThread);
		
		int offset = 0;
		
		for (int i = 0; i < numThreads; i++) {
			offset = i * numRecordsPerThread;
			
			Thread t = new Thread(new SolrUpdateRunnable(zkHost, 
					(numRecordsLeft > numRecordsPerThread) ? numRecordsPerThread : numRecordsLeft, offset, this.collection, this.batchSize));
			t.start();
			numRecordsLeft-=numRecordsPerThread;
			numRunningThreads.incrementAndGet();
		}
		
		logger.info("waiting for update threads to finish...");
		while (numRunningThreads.get() > 0) {
			System.out.print(".");
			Thread.sleep(5000);
		}
		System.out.println();
		
		try {
			logger.info("all updates finished, begin committing");
			long startTime = Calendar.getInstance().getTimeInMillis();				
			solrServer.commit();
			logger.info("finished committing.  Commit elapsed time: " + (Calendar.getInstance().getTimeInMillis() - startTime)/1000.0 + "s");		
		} catch (Exception e) {
			logger.error("failed to commit", e);
		}
		finally {
			solrServer.shutdown();
		}		
		
	}
	
	static class SolrUpdateRunnable implements Runnable {
		String zkHost;
		static Logger logger = Logger.getLogger(SolrUpdateRunnable.class);
		CloudSolrServer solrServer;
		int numRecords;
		int offset;
		int batchSize;
		
		public SolrUpdateRunnable(String zkHost, int numRecords, int offset, String collectionName, int batchSize) throws MalformedURLException
		{
			this.zkHost = zkHost;
			this.solrServer = new CloudSolrServer(zkHost);
			this.solrServer.setDefaultCollection(collectionName);
			this.numRecords = numRecords;
			this.offset = offset;
			this.batchSize = batchSize;
		}
		
		public void run() {
			ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(batchSize);
			int currentIndex = (this.offset < records.size()) ? this.offset : this.offset % records.size();
			long startTime = Calendar.getInstance().getTimeInMillis();

			for (int i = 0; i < numRecords; i++) {
				SolrInputDocument doc = parseDocument(records.get(currentIndex++));
				if (doc == null) continue;
				
				docs.add(doc);
				
				if (docs.size() >= batchSize)
				{
					addDocs(docs);			
					docs.clear();
				}
			
				if (currentIndex == records.size())
					currentIndex = 0;				
			}

			addDocs(docs);			
			logger.info("added " + numRecords + " docs in " + (Calendar.getInstance().getTimeInMillis() - startTime)/1000.0 + "s");
			
			numRunningThreads.decrementAndGet();			
		}

		private void addDocs(ArrayList<SolrInputDocument> docs) {
			if (docs.size() == 0) return;
			
			try {
				long startTime = Calendar.getInstance().getTimeInMillis();
				solrServer.add(docs);
				long elapsedTime = Calendar.getInstance().getTimeInMillis() - startTime;
				logger.debug("added " + docs.size() + " documents in one batch. Elapsed time: " + elapsedTime/1000.0 + "s");
			} catch (Exception e) {
				logger.error("failed to add solr docs", e);
			}
		}
		
	}
	
	/**
	 * Update solr docs using Solr ConcurrentUpdateSolrServer class
	 * @param totalRecords
	 * @param numThreads 
	 * @param queueSize 
	 * @param solrUrl 
	 * @throws SolrServerException 
	 * @throws IOException
	 */
	static void doConcurrentUpdate(int totalRecords, String solrUrl, int queueSize, int numThreads) throws SolrServerException, IOException {
		logger.info("using ConcurrentUpdateSolrServer class for indexing connecting to Solr: " + solrUrl);

		ConcurrentUpdateSolrServer solrServer = new ConcurrentUpdateSolrServer(solrUrl, queueSize, numThreads);
		SolrPingResponse response = solrServer.ping();
		logger.debug("response status: " + response.getStatus());
		
		int currentIndex = 0;
		ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(totalRecords);

		long startTime = Calendar.getInstance().getTimeInMillis();
		
		for (int i = 0; i < totalRecords; i++) {
			SolrInputDocument doc = parseDocument(records.get(currentIndex++));

			logger.trace("adding solr doc:" + doc);
			docs.add(doc);
				
			if (currentIndex == records.size())
				currentIndex = 0;
		}
		
		try {
			solrServer.add(docs);
			long elapsedTime = Calendar.getInstance().getTimeInMillis() - startTime;
			logger.info("finished adding " + totalRecords + " documents");
			logger.info("Update elapsed time: " + elapsedTime/1000.0 + "s");			
		} catch (Exception e) {
			logger.error("failed to add docs", e);
		}
		
		try {	
			logger.info("begin committing");
			startTime = Calendar.getInstance().getTimeInMillis();
			solrServer.commit();
			logger.info("finished committing.  Commit elapsed time: " + (Calendar.getInstance().getTimeInMillis() - startTime)/1000.0 + "s");		
		} catch (Exception e) {
			logger.error("failed to commit", e);
		}
		finally {
			solrServer.shutdown();
		}
	}

	private static String convertTime(long unixTimeInSeconds) {
		return SOLR_DATE_FORMAT.format(new Date(unixTimeInSeconds * 1000));
	}
	
	private static SolrInputDocument parseDocument(JsonNode node) {
		SolrInputDocument d = new SolrInputDocument();
		
		for (Map.Entry<String, String> e : fieldTypeMap.entrySet()) {
			String field = e.getKey();
			String type = e.getValue();

			if (type.equals("string"))
				d.addField(field, node.path(field).asText());
			else if (type.equals("int"))
				d.addField(field, node.path(field).asInt());
			else if (type.equals("long"))
				d.addField(field, node.path(field).asLong());
			else if (type.equals("uuid")) {
				// generates a new uuid if it's absent
				if (StringUtils.isEmpty(node.path(field).asText()))
					d.addField(field, UUID.randomUUID().toString());
				else
					d.addField(field, node.path(field).asText());
			}
			else if (type.equals("date") || type.equals("tdate"))
				d.addField(field, convertTime(node.path(field).asLong()));
			else
			{
				logger.error("unrecognized type for field: " + field + " type:" + type);
				return null;
			}			
		}
		
		return d;
	}
	
	private static Map<String, String> loadSolrSchema(File schemaFile) throws SAXException, IOException, ParserConfigurationException, XPathExpressionException {
		HashMap<String, String> nameTypeMap = new HashMap<String, String>();
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document d = builder.parse(schemaFile);
		
		XPathFactory xPathFactory = XPathFactory.newInstance();
		XPath xPath = xPathFactory.newXPath();
		XPathExpression expression = xPath.compile("/schema/fields/field");
		NodeList list = (NodeList)expression.evaluate(d, XPathConstants.NODESET);
		
		for (int i = 0; i < list.getLength(); i++) {
			Node n = list.item(i);
			NamedNodeMap attributeMap = n.getAttributes();
			String name = attributeMap.getNamedItem("name").getNodeValue();
			String type = attributeMap.getNamedItem("type").getNodeValue();
			if (!name.equals("_version_") && !type.equals("point"))
				nameTypeMap.put(name, type);
		}
		
		return nameTypeMap;
	}
	
	private static void loadInputFileIntoMemory(File inputFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		ObjectMapper mapper = new ObjectMapper();
		
		String line;
		int count = 0;
		while ((line = reader.readLine()) != null) {
			if (line.endsWith(","))
				line = line.substring(0, line.length()-1);
			
			JsonNode node = mapper.readTree(line);
			logger.trace("parsed JSON node: " + node);
			records.add(node);
			count++;
		}
		logger.debug("loaded " + count + " JSON records");
		reader.close();
	}	

}
