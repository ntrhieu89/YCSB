package com.yahoo.ycsb.db;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A background thread that simulates database failures. Database is assumed to
 * have crashed during the provided intervals. Applications shouldn't contact
 * database if the {@link #failed} is true. Precision is second.
 * 
 * @author haoyuh
 *
 */
public class DBSimulator implements Callable<Void> {

	private final AtomicBoolean failed;
	private final long[] invtervals;
	private int index;
	private long now;

	private boolean isRunning = true;

	/**
	 * @param failed
	 *            A flag indicating the status of database. True means database
	 *            has crashed. False means database is alive.
	 * @param invtervals
	 *            [start, end, start, end...]. Database is considered as failed
	 *            during the given interval. Start and end are relative time to
	 *            when this class is created. Time unit is second.
	 */
	public DBSimulator(AtomicBoolean failed, long[] invtervals) {
		super();
		this.failed = failed;
		this.invtervals = invtervals;
		this.now = System.currentTimeMillis();
		System.out.println("created db state simulator");
	}

	@Override
	public Void call() throws Exception {

		while (isRunning) {
			long start = invtervals[index];
			long end = invtervals[index + 1];

			long now = (System.currentTimeMillis() - this.now) / 1000;

			if (now >= start && !failed.get()) {
				System.out.println("Crash at " + System.nanoTime());
				failed.set(true);
			} else if (now >= end) {
				System.out.println("Back at " + System.nanoTime());
				failed.set(false);
				System.out.println("#########dbfailed " + failed.get());
				index += 2;
				if (index >= invtervals.length) {
					break;
				}
			}
			Thread.sleep(1000);
		}
		return null;
	}

	public void shutdown() {
		isRunning = false;
	}

	public long[] getInvtervals() {
		return invtervals;
	}
}
