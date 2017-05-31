package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.db.TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS;
import static com.yahoo.ycsb.db.TardisClientConfig.STATS_EW_WORKER_TIME_BETWEEN_CHECKING_EW;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class RedisEWStatsWatcher implements Callable<Void> {
	private final HashShardedJedis redisClient;

	private boolean isRunning = true;

	public RedisEWStatsWatcher(HashShardedJedis client) {
		this.redisClient = client;
	}

	@Override
	public Void call() throws Exception {

		System.out.println("Start EWStatsWatcher...");

		List<Integer> keys = new ArrayList<>();

		for (int i = 0; i < NUM_EVENTUAL_WRITE_LOGS; i++) {
			keys.add(i);
		}

		while (isRunning) {
			try {
				Map<String, Long> values = redisClient.mscard(keys, TardisClientConfig.KEY_EVENTUAL_WRITE_LOG);
				long timestamp = System.currentTimeMillis();
				System.out.println("total_dirty_keys:" + timestamp + ":" + values.values().stream().mapToLong(i -> i.longValue()).sum());
				try {
					Thread.sleep(STATS_EW_WORKER_TIME_BETWEEN_CHECKING_EW);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				System.out.println("EW failed");
				e.printStackTrace();
			}
		}
		return null;
	}

	public void shutdown() {
		isRunning = false;
	}

}

