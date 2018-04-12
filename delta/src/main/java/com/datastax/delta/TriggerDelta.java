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
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

/**
 * Find the latency replicated copies of data between two DC with different
 * Region
 * 
 * @author vinodjembu
 *
 */
public class TriggerDelta {

	public static void main(String[] args) {

		//// Users//vinodjembu//Documents//git-repo//delta//src//main//resources//delta.properties
		// Load properties with arguments
		Properties prop = getProperties(
				"//Users//vinodjembu//Documents//git-repo//delta//src//main//resources//delta.properties");
		int numerofRecords = Integer.parseInt(prop.getProperty("num_record"));
		DseSession writesession = createWriteConnection(prop);
		DseSession readsession = createReadConnection(prop);

		createKeyspaceAndTables(writesession, prop);
		findDelta(writesession, readsession, numerofRecords);
		System.exit(0);

	}

	/**
	 * Load the property file
	 * 
	 * @param filepath
	 * @return
	 */
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
	 * Return Cassandra Write Session
	 * 
	 * @param prop
	 * @return
	 */
	private static DseSession createWriteConnection(Properties prop) {
		DseCluster cluster = null;
		cluster = DseCluster.builder().addContactPoint(prop.getProperty("writeSeed"))
				.withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(prop.getProperty("writeDC"))
						.withUsedHostsPerRemoteDc(2).allowRemoteDCsForLocalConsistencyLevel()

						.build())
				.build();
		DseSession session = cluster.connect();
		return session;

	}

	/**
	 * Return Cassandra Read Session
	 * 
	 * @param prop
	 * @return
	 */
	private static DseSession createReadConnection(Properties prop) {
		DseCluster cluster = null;
		cluster = DseCluster.builder().addContactPoint(prop.getProperty("readSeed"))
				.withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(prop.getProperty("readDC"))
						.withUsedHostsPerRemoteDc(2).allowRemoteDCsForLocalConsistencyLevel().build())
				.build();
		DseSession session = cluster.connect();
		return session;

	}

	/**
	 * Create Delta Keyspace and Latency Find Table
	 * 
	 * @param session
	 * @param prop
	 */
	private static void createKeyspaceAndTables(DseSession session, Properties prop) {
		// Local Keyspace
		// Statement createKS = new SimpleStatement(
		// "CREATE KEYSPACE IF NOT EXISTS delta WITH replication = {'class':
		// 'SimpleStrategy', 'replication_factor': 1}");

		// DROP KEYPSACE if EXIST
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

	/**
	 * Spin Write and Read thread for different DC to find latency between DC
	 * 
	 * @param writesession
	 * @param readsession
	 * @param numerofRecords
	 */
	private static void findDelta(DseSession writesession, DseSession readsession, int numerofRecords) {

		ExecutorService executor = Executors.newFixedThreadPool(5);
		for (int i = 0; i < 4; i++) {
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
