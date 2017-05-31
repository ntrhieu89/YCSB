package com.yahoo.ycsb.db;


public class ExponentialBackoff {

	private final int maxBackoffTime;
	private final int baseBackoffTime;
	private int currentBackoff;


	public ExponentialBackoff(int maxBackoffTime, int baseBackoffTime) {
		super();
		this.maxBackoffTime = maxBackoffTime;
		this.baseBackoffTime = baseBackoffTime;
		this.currentBackoff = this.baseBackoffTime;
	}

	public void backoff() {
		try {
			Thread.sleep(currentBackoff);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		currentBackoff = Math.min(currentBackoff * 2, this.maxBackoffTime);
	}

	public void reset() {
		this.currentBackoff = this.baseBackoffTime;
	}
	
	public static void main(String[] args) {
		ExponentialBackoff b = new ExponentialBackoff(1000, 100);
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.backoff();
		System.out.println(b.currentBackoff);
		b.reset();
		b.backoff();
		System.out.println(b.currentBackoff);
		
	}
}
