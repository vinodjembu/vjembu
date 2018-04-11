package com.datastax.delta;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

public class TriggerDelta {

	public static void main(String[] args) {
		// DseSession session = createConnection(prop);
		Properties prop = getProperties(
				"//Users//vinodjembu//workspace//bootcamp//delta//src//main//resources//delta.properties");
		int numerofRecords = Integer.parseInt(prop.getProperty("num_record"));
		DseSession writesession = createWriteConnection(prop);
		DseSession readsession = createReadConnection(prop);

		createKeyspaceAndTables(writesession, prop);
		findDelta(writesession, readsession, numerofRecords);
		System.exit(0);

	}

	private static Properties getProperties(String filepath) {
		Properties prop = new Properties();

		try {
			FileInputStream input = new FileInputStream(filepath);
			// load a properties file
			prop.load(input);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return prop;
	}

	/**
	 * Return Local Cassandra Session
	 * 
	 * @return
	 */
	private static DseSession createConnection(Properties prop) {
		DseCluster cluster = null;
		cluster = DseCluster.builder().addContactPoint(prop.getProperty("localSeed")).build();
		DseSession session = cluster.connect();
		return session;

	}

	private static DseSession createWriteConnection(Properties prop) {
		DseCluster cluster = null;
		cluster = DseCluster.builder().addContactPoint(prop.getProperty("writeSeed")).build();
		DseSession session = cluster.connect();
		System.out.println("The keyspaces known by Connection are: " + cluster.getMetadata().getKeyspaces().toString());
		return session;

	}

	private static DseSession createReadConnection(Properties prop) {
		DseCluster cluster = null;
		cluster = DseCluster.builder().addContactPoint(prop.getProperty("readSeed")).build();
		DseSession session = cluster.connect();
		return session;

	}

	private static void createKeyspaceAndTables(DseSession session, Properties prop) {
		// Local Keyspace
		// Statement createKS = new SimpleStatement(
		// "CREATE KEYSPACE IF NOT EXISTS delta WITH replication = {'class':
		// 'SimpleStrategy', 'replication_factor': 1}");

		// DROP KEYPSACE if EXIST
		Statement dropKS = new SimpleStatement("DROP KEYSPACE IF  EXISTS delta ");
		System.out.println(dropKS);

		Statement createKS = new SimpleStatement(
				"CREATE KEYSPACE IF NOT EXISTS delta WITH replication = {'class': 'NetworkTopologyStrategy', '"
						+ prop.getProperty("writeDC") + " ': 3, '" + prop.getProperty("readDC") + "' :3}");
		System.out.println(createKS);

		session.execute(createKS);

		Statement createTable = new SimpleStatement(
				"CREATE TABLE If NOT EXISTS delta.latencyfind ( id bigInt,writeTime bigInt, PRIMARY KEY(id)) ;");
		createTable.setConsistencyLevel(ConsistencyLevel.ALL);
		System.out.println(createTable);
		session.execute(createTable);
	}

	private static void findDelta(DseSession writesession, DseSession readsession, int numerofRecords) {

		ExecutorService executor = Executors.newFixedThreadPool(5);
		for (int i = 0; i < 3; i++) {
			if (i == 0) {
				Runnable insertWorker = new InsertDelta(writesession, numerofRecords);
				executor.execute(insertWorker);
			}
			Runnable readWorker = new ReadDelta(readsession, new Random().nextInt(numerofRecords));
			executor.execute(readWorker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");

	}
}
