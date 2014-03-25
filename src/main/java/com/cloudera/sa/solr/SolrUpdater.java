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
import java.util.UUID;

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
	
	@Option(name="-q", usage="queue/batch size for concurrent mode, default: 1000", depends={"-mode"})
	private int queueSize = 1000;
	

	public static void main(String[] args) throws IOException, SolrServerException {
		new SolrUpdater().doMain(args);				
	}

	public static void printExample() {
		System.out.println("\nExample using Solr Cloud (default):");
		System.out.println("java -jar SolrLoadTest.jar " + SolrUpdater.class.getName() + " -zk <zkhost:2181/solr> -c <collection> -f <data_file> -t 10 -n 10000");
		System.out.println("\nExample using Concurrent Solr Server:");
		System.out.println("java -jar SolrLoadTest.jar " + SolrUpdater.class.getName() + " -solr <solr_host:8983/solr/collection_shard1_replica1> -f <data_file> -t 10 -n 10000");
	}
	
	public void doMain(String[] args) throws IOException, SolrServerException {
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
			
			if (solrServerMode.equals("cloud"))
				doCloudSolrUpdate();
			else
				doConcurrentUpdate(numRecords, solrUrl, queueSize, numThreads);
					
			logger.info("waiting for threads to die...");
			
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);		
			printExample();
		}
	}
	
	private void doCloudSolrUpdate() throws SolrServerException, IOException {
		logger.info("using CloudSolrServer class for indexing via zookeeper: " + this.zkHost);
		CloudSolrServer server = new CloudSolrServer(this.zkHost);
		server.setDefaultCollection(this.collection);
		
		SolrPingResponse response = server.ping();
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
					(numRecordsLeft > numRecordsPerThread) ? numRecordsPerThread : numRecordsLeft, offset, this.collection));
			t.start();
			numRecordsLeft-=numRecordsPerThread;
		}
		
	}
	
	static class SolrUpdateRunnable implements Runnable {
		String zkHost;
		static Logger logger = Logger.getLogger(SolrUpdateRunnable.class);
		CloudSolrServer solrServer;
		int numRecords;
		int offset;
		
		public SolrUpdateRunnable(String zkHost, int numRecords, int offset, String collectionName) throws MalformedURLException
		{
			this.zkHost = zkHost;
			this.solrServer = new CloudSolrServer(zkHost);
			this.solrServer.setDefaultCollection(collectionName);
			this.numRecords = numRecords;
			this.offset = offset;
		}
		
		public void run() {
			ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(numRecords);
			int currentIndex = (this.offset < records.size()) ? this.offset : this.offset % records.size();

			for (int i = 0; i < numRecords; i++) {
				SolrInputDocument doc = parseDocument(records.get(currentIndex++));
				docs.add(doc);
				if (currentIndex == records.size())
					currentIndex = 0;				
			}
			long startTime;
			
			try {
				startTime = Calendar.getInstance().getTimeInMillis();
				solrServer.add(docs);
				long elapsedTime = Calendar.getInstance().getTimeInMillis() - startTime;
				logger.info("finished adding " + numRecords + " documents");
				logger.info("Update elapsed time: " + elapsedTime/1000.0 + "s");
			} catch (Exception e) {
				logger.error("failed to add solr docs", e);
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
		
		d.addField("id", UUID.randomUUID().toString());

		d.addField("log_type", node.path("log_type").asText());
		d.addField("ctime", convertTime(node.path("ctime").asLong()));
		d.addField("user_account_id", node.path("user_account_id").asLong());
		d.addField("source_player_id", node.path("source_player_id").asLong());

		d.addField("build_id", node.path("build_id").asLong());
		d.addField("realm_id", node.path("realm_id").asLong());
		d.addField("world_id", node.path("world_id").asInt());
		d.addField("zone_id", node.path("zone_id").asInt());
		d.addField("world_occurrence_id", node.path("world_occurrence_id").asInt());

		d.addField("xloc", node.path("xloc").asInt());
		d.addField("yloc", node.path("yloc").asInt());
		d.addField("zloc", node.path("zloc").asInt());
		
		d.addField("player_level", node.path("player_level").asInt());
		d.addField("action", node.path("action").asInt());
		d.addField("action_id", node.path("action_id").asLong());
		d.addField("current_full", node.path("current_full").asInt());
		d.addField("current_partial", node.path("current_partial").asInt());
		d.addField("quest_reward", node.path("quest_reward").asInt());

		return d;
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
