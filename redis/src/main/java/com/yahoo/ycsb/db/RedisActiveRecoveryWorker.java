package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.db.TardisClientConfig.KEY_BUFFERED_WRITES_LOG;
import static com.yahoo.ycsb.db.TardisClientConfig.NUM_EVENTUAL_WRITE_LOGS;
import static com.yahoo.ycsb.db.TardisClientConfig.bufferedWriteKey;
import static com.yahoo.ycsb.db.TardisClientConfig.ewKey;
import static com.yahoo.ycsb.db.TardisClientConfig.ewLeaseKey;
import static com.yahoo.ycsb.db.TardisClientConfig.keyFromBufferedWriteKey;
import static com.yahoo.ycsb.db.TardisClientConfig.leaseKey;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisActiveRecoveryWorker implements Callable<Void> {
	private final HashShardedJedis redisClient;
	private final RedisLease leaseClient;
	private final RedisRecoveryEngine recovery;
	private final Random r;
	private final int alpha;
	private final ExponentialBackoff backoff = new ExponentialBackoff(1000, 100);

	private boolean isRunning = true;

	private final List<String> luaKeys = new ArrayList<>();

	public RedisActiveRecoveryWorker(AtomicBoolean isDatabaseFailed, HashShardedJedis memcachedClient,
			RedisLease leaseClient, RedisRecoveryEngine recovery, int totalNumberOfARWorker, int alpha) {
		super();
		this.redisClient = memcachedClient;
		this.leaseClient = leaseClient;
		this.recovery = recovery;
		this.r = new Random(System.nanoTime());
		this.alpha = alpha;
		System.out.println("created AR worker");
	}

	@Override
	public Void call() throws Exception {

		while (isRunning) {

			int start = this.r.nextInt(NUM_EVENTUAL_WRITE_LOGS);
			int ewIndex = start;
			do {

				if (!isRunning) {
					break;
				}

				String ew = ewKey(ewIndex);

				Set<String> dirtyUserIds = redisClient.smembers(redisClient.getServerIndex(ewIndex), ew);

				if (dirtyUserIds == null || dirtyUserIds.isEmpty()) {
					ewIndex = (ewIndex + 1) % NUM_EVENTUAL_WRITE_LOGS;
					continue;
				}

				Set<String> recoveredKeys = new HashSet<>();

				List<String> dirtyKeys = RandomUtility.randomSampling(dirtyUserIds, alpha);
				
				for (String dirtyKey : dirtyKeys) {

					if (!isRunning) {
						break;
					}

					int serverId = redisClient.getKeyServerIndex(dirtyKey);

					String logKey = bufferedWriteKey(dirtyKey);
					boolean pw = redisClient.exists(serverId, logKey);

					if (!pw) {
						recoveredKeys.add(dirtyKey);
						continue;
					}

					if (leaseClient.acquireLease(serverId, leaseKey(dirtyKey))) {
						RecoveryResult ret = recovery.recover(RecoveryCaller.AR, dirtyKey);

						if (!RecoveryResult.FAIL.equals(ret)) {
							recoveredKeys.add(dirtyKey);
						}

						leaseClient.releaseLease(serverId, leaseKey(dirtyKey), luaKeys);

						if (RecoveryResult.FAIL.equals(ret)) {
							backoff.backoff();
						} else if (RecoveryResult.SUCCESS.equals(ret)) {
							backoff.reset();
						}
					}
				}

				if (recoveredKeys.isEmpty()) {
					ewIndex = (ewIndex + 1) % NUM_EVENTUAL_WRITE_LOGS;
					continue;
				}

				try {
					leaseClient.acquireTillSuccess(redisClient.getServerIndex(ewIndex), ewLeaseKey(ewIndex));

					luaKeys.clear();
					luaKeys.add(ew);

					Set<String> newEW = new HashSet<>();
					
					Map<String, Boolean> results = redisClient.mexists(luaKeys, ewIndex, new ArrayList<>(recoveredKeys),
							KEY_BUFFERED_WRITES_LOG, newEW);

						// remove new users that are dirty from the recovered
						// set
						results.forEach((userLog, numberOfBufferedWrites) -> {
							if (numberOfBufferedWrites) {
								recoveredKeys.remove(keyFromBufferedWriteKey(userLog));
							}
						});

						// remove recovered users from EW list
						newEW.removeAll(recoveredKeys);
						if (newEW.isEmpty()) {
							redisClient.del(redisClient.getServerIndex(ewIndex), ew);
						} else if (!recoveredKeys.isEmpty()) {
							redisClient.srem(redisClient.getServerIndex(ewIndex), ew, recoveredKeys.toArray(new String[0]));
						}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					leaseClient.releaseLease(redisClient.getServerIndex(ewIndex), ewLeaseKey(ewIndex), luaKeys);
				}
				ewIndex = (ewIndex + 1) % NUM_EVENTUAL_WRITE_LOGS;
			} while (ewIndex != start);
			sleep();
		}
		return null;
	}

	static final Set<String> convertListToSet(List<String> list) {
		Set<String> set = new HashSet<>(list);
		return set;
	}

	private void sleep() {
		
		if (TardisClientConfig.RECOVERY_WORKER_SLEEP_TIME == 0) {
			return;
		}
		
		try {
			Thread.sleep(TardisClientConfig.RECOVERY_WORKER_SLEEP_TIME);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void shutdown() {
		isRunning = false;
	}
}
