package com.yahoo.ycsb.db;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisLease {

	private static final long JITTER_BASE = 30;
	private static final int JITTER_RANGE = 20;

	private final Random random;
	private final HashShardedJedis redisClient;
	private final int clientId;
	private final String caller;
	private static final AtomicInteger global_id = new AtomicInteger();
	public static final AtomicInteger numberOfBackOffs = new AtomicInteger();

	public RedisLease(HashShardedJedis redisClient, String caller) {
		super();
		this.redisClient = redisClient;
		this.clientId = global_id.incrementAndGet();
		this.random = new Random();
		this.caller = caller;
	}

	public void acquireTillSuccess(int id, String key) {
		int tries = 0;
		while (!acquireLease(id, key)) {
			try {
				Thread.sleep(JITTER_BASE + random.nextInt(JITTER_RANGE));
			} catch (Exception e) {
				e.printStackTrace();
			}
			numberOfBackOffs.incrementAndGet();
//			if (tries >= 10) {
//				System.out.println("acquire lease on key " + key);
//			}
			tries++;
		}
	}

	public boolean acquireLease(int id, String key) {
		return redisClient.setnx(id, key, caller + String.valueOf(this.clientId)) == 1;
	}

	public void releaseLease(int id, String key, List<String> keys) {
		keys.clear();
		keys.add(key);
		keys.add(caller + String.valueOf(this.clientId));
		if ((long) redisClient.evalsha(id, RedisLuaScripts.RELEASE_LEASE, keys, Collections.<String> emptyList()) == 0) {
			
		}
//	  redisClient.del(id, key);
	}

}
