package com.datastax.delta;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseSession;

/**
 * Insert records into one of the region
 * 
 * @author vinodjembu
 *
 */
public class InsertDelta implements Runnable {
	DseSession session;
	int numerofRecords;

	public InsertDelta(DseSession session, int numerofRecords) {
		this.session = session;
		this.numerofRecords = numerofRecords;
	}

	public void run() {

		String name = Thread.currentThread().getName();
		System.out.println("Write Thread --> " + name);

		for (int i = 1; i < numerofRecords; i++) {

			Statement statement = new SimpleStatement("INSERT INTO delta.latencyfind" + " (id,writeTime )" + "VALUES ("
					+ i + "," + System.nanoTime() + ")");
			statement.setConsistencyLevel(ConsistencyLevel.ONE);
			session.execute(statement);

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		System.out.println("Records Inserted Completed");
	}
}
