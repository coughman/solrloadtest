package com.cloudera.sa.solr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SolrUpdater {
	static Logger logger = Logger.getLogger(SolrUpdater.class);
	
	static ArrayList<JsonNode> records = new ArrayList<JsonNode>(); 
	static ConcurrentUpdateSolrServer solrServer = null;

	private static final String NATIVE_SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"; // e.g. 2007-04-26T08:05:04.789Z
	private static final SimpleDateFormat SOLR_DATE_FORMAT = new SimpleDateFormat(NATIVE_SOLR_DATE_FORMAT);
	
	public static void main(String[] args) throws IOException, SolrServerException {
		if (args.length != 5) {
			System.err.println("synopsis:\njava -cp SolrLoadTest.jar com.cloudera.sa.solr.SolrUpdater <solr_url> <json_data_file> <num_threads> <solr_queue_size> <total_num_records>");
			return;
		}
		int numThreads = Integer.parseInt(args[2]);
		int queueSize = Integer.parseInt(args[3]);
		int totalRecords = Integer.parseInt(args[4]);
		
		String inputFile = args[1];
		
		loadInputFileIntoMemory(inputFile);
		solrServer = new ConcurrentUpdateSolrServer(args[0], queueSize, numThreads);
		
		long startTime = Calendar.getInstance().getTimeInMillis();
		
		doConcurrentUpdate(totalRecords);
		
		long elapsedTime = Calendar.getInstance().getTimeInMillis() - startTime;
		logger.info("finished adding " + totalRecords + " documents");
		logger.info("Update elapsed time: " + elapsedTime/1000.0 + "s");
		
		logger.info("begin committing");
		startTime = Calendar.getInstance().getTimeInMillis();
		solrServer.commit();
		logger.info("finished committing.  Commit elapsed time: " + (Calendar.getInstance().getTimeInMillis() - startTime)/1000.0 + "s");
		logger.info("waiting for threads to die...");
	}

/*	private static void doCloudSolrUpdate(int numRecords, int numThreads) {
		for (int i = 0; i < numThreads; i++) {
			Thread t = new Thread(new SolrUpdateRunnable());
			t.start();
					
		}
	}
	
	static class SolrUpdateRunnable implements Runnable {
		static Logger logger = Logger.getLogger(SolrUpdateRunnable.class);
		
		public void run() {
			// TODO Auto-generated method stub
			
		}
		
	}*/
	
	/**
	 * Update solr docs using Solr ConcurrentUpdateSolrServer class
	 * @param totalRecords
	 * @throws IOException
	 */
	private static void doConcurrentUpdate(int totalRecords)
			throws IOException {
		
		int currentIndex = 0;

		for (int i = 0; i < totalRecords; i++) {
			SolrInputDocument doc = parseDocument(records.get(currentIndex++));
			try {
				logger.trace("adding solr doc:" + doc);
				solrServer.add(doc);
			} catch (SolrServerException e) {
				logger.error("failed to add document:" + doc, e);
			}
			
			if (currentIndex == records.size())
				currentIndex = 0;
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
