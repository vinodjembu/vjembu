package com.datastax.delta;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.dse.DseSession;

/**
 * Read records from one of the region
 * 
 * @author vinodjembu
 *
 */
public class ReadDelta implements Runnable {
	DseSession session;
	int record_no;

	public ReadDelta(DseSession session, int record_no) {
		this.session = session;
		this.record_no = record_no;
	}

	public void run() {

		String name = Thread.currentThread().getName();
		System.out.println("Read Thread --> " + name + " : Record No :" + record_no);

		Statement read;
		boolean isAvailable = true;
		while (isAvailable) {
			read = new SimpleStatement("SELECT writeTime FROM delta.latencyfind where id = " + record_no + ";");
			read.setConsistencyLevel(ConsistencyLevel.ONE);
			ResultSet rs = session.execute(read);

			List<Row> allResults = rs.all();

			for (Row row : allResults) {
				long delta = System.nanoTime() - (row.getLong("writeTime"));
				System.out.println("Delta difference for Record No " + record_no + " in msecs -->  "
						+ TimeUnit.MILLISECONDS.convert(delta, TimeUnit.NANOSECONDS));
				isAvailable = false;
			}
		}

		System.out.println("Exit tht Thread --> " + name);
	}
}
