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

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SolrUpdater {
	static Logger logger = Logger.getLogger(SolrUpdater.class);
	
	static ArrayList<JsonNode> records = new ArrayList<JsonNode>(); 

	private static final String NATIVE_SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"; // e.g. 2007-04-26T08:05:04.789Z
	private static final SimpleDateFormat SOLR_DATE_FORMAT = new SimpleDateFormat(NATIVE_SOLR_DATE_FORMAT);
	
	public static void main(String[] args) throws IOException, SolrServerException {
		if (args.length != 7) {
			System.err.println("synopsis:\njava -cp SolrLoadTest.jar com.cloudera.sa.solr.SolrUpdater <zk_host> <solr_url> <json_data_file> <num_threads> <solr_queue_size> <total_num_records>");
			return;
		}
		
		String zkHost = args[0];
		//String solrUrl = args[1];
		String collectionName = args[2];
		String inputFile = args[3];
		int numThreads = Integer.parseInt(args[4]);
		//int queueSize = Integer.parseInt(args[5]);
		int totalRecords = Integer.parseInt(args[6]);
		
		loadInputFileIntoMemory(inputFile);	
		
		doCloudSolrUpdate(totalRecords, numThreads, zkHost, collectionName);
		//doConcurrentUpdate(totalRecords, solrUrl, queueSize, numThreads);
				
		logger.info("waiting for threads to die...");
	}

	static void doCloudSolrUpdate(int numRecords, int numThreads, String zkHost, String collectionName) throws MalformedURLException {
		logger.info("using CloudSolrServer class for indexing...");
		int numRecordsLeft = numRecords;
		
		if (numRecords < numThreads) {
			logger.error("number of records must be larger than number of threads.  Aborting...");
			return;
		}
		
		int numRecordsPerThread = numRecords / numThreads;
		for (int i = 0; i < numThreads; i++) {
			Thread t = new Thread(new SolrUpdateRunnable(zkHost, 
					(numRecordsLeft > numRecordsPerThread) ? numRecordsPerThread : numRecordsLeft, collectionName));
			t.start();
			numRecordsLeft-=numRecordsPerThread;
		}
	}
	
	static class SolrUpdateRunnable implements Runnable {
		String zkHost;
		static Logger logger = Logger.getLogger(SolrUpdateRunnable.class);
		CloudSolrServer solrServer;
		int numRecords;
		
		public SolrUpdateRunnable(String zkHost, int numRecords, String collectionName) throws MalformedURLException
		{
			this.zkHost = zkHost;
			this.solrServer = new CloudSolrServer(zkHost);
			this.solrServer.setDefaultCollection(collectionName);
			this.numRecords = numRecords;
		}
		
		public void run() {
			ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(numRecords);
			int currentIndex = 0;

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
	 * @throws IOException
	 */
	static void doConcurrentUpdate(int totalRecords, String solrUrl, int queueSize, int numThreads) {
		logger.info("using ConcurrentUpdateSolrServer class for indexing...");

		ConcurrentUpdateSolrServer solrServer = new ConcurrentUpdateSolrServer(solrUrl, queueSize, numThreads);
		
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

	private static void loadInputFileIntoMemory(String inputFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(new File(inputFile)));
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
