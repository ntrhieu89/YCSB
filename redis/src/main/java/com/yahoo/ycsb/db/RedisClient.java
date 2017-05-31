/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

	private HashShardedJedis jedis;
	private RedisLease lease;
	private MongoDbClient mongo;
	private List<String> luaKeys = new ArrayList<>();
	private String phase;
	private RedisRecoveryEngine recovery;

	private static final AtomicBoolean isDBFailed = new AtomicBoolean(false);
	private static final AtomicBoolean initLock = new AtomicBoolean(false);
	private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
	private static RedisEWStatsWatcher watcher;
	private static List<RedisActiveRecoveryWorker> workers = new ArrayList<>();
	private static DBSimulator dbSimulator;

	private static ExecutorService threads;

	public static final String REDIS_HOSTS_PROPERTY = "redis.hosts";
	public static final String MONGO_HOST_PROPERTY = "mongo.host";
	public static final String AR_WORKER_PROPERTY = "ar";
	public static final String DB_FAIL_WORKER_PROPERTY = "dbfail";
	public static final String ALPHA_PROPERTY = "alpha";
	public static final String PHASE_PROPERTY = "phase";
	public static final String SUCCESS_WRITE_PROPERTY = "successWrite";

	@Override
	public void init() throws DBException {
	  
	  System.out.println("############# Version 5.3");

		INIT_COUNT.incrementAndGet();

		Properties props = getProperties();
		
		props.forEach((k, v) -> {
			System.out.println(k + "=" + v);
		});

		if (!props.containsKey(REDIS_HOSTS_PROPERTY)) {
			throw new RuntimeException("redis hosts not specified");
		}

		if (!props.containsKey(MONGO_HOST_PROPERTY)) {
			throw new RuntimeException("mongo hosts not specified");
		}

		if (!props.containsKey(AR_WORKER_PROPERTY)) {
			throw new RuntimeException("ar not specified");
		}

		if (!props.containsKey(ALPHA_PROPERTY)) {
			throw new RuntimeException("alpha not specified");
		}

		if (!props.containsKey(PHASE_PROPERTY)) {
			throw new RuntimeException("phase not specified");
		}

		phase = props.getProperty(PHASE_PROPERTY);

		int alpha = Integer.parseInt(props.getProperty(ALPHA_PROPERTY));

		this.mongo = new MongoDbClientDelegate(props.getProperty(MONGO_HOST_PROPERTY), isDBFailed);
		this.mongo.init();
		
		String[] urls = props.getProperty(REDIS_HOSTS_PROPERTY).split(",");
		
		jedis = new HashShardedJedis(urls);
		lease = new RedisLease(jedis, "client");
		recovery = new RedisRecoveryEngine(jedis, mongo);

		synchronized (initLock) {
			if (initLock.get()) {
				return;
			}
			
			jedis.loadScript();
			
			if ("load".equals(phase)) {
				this.mongo.dropDatabase();
			}
			
			if (props.containsKey(SUCCESS_WRITE_PROPERTY)) {
				TardisClientConfig.measureSuccessWrites = Boolean.parseBoolean(props.getProperty(SUCCESS_WRITE_PROPERTY));
				isDBFailed.set(true);
			}

			threads = Executors.newFixedThreadPool(120);

			if (props.containsKey(DB_FAIL_WORKER_PROPERTY)) {
				String[] ints = props.getProperty(DB_FAIL_WORKER_PROPERTY).split(",");
				long[] intervals = new long[ints.length];
				for (int i = 0; i < ints.length; i++) {
					intervals[i] = Integer.parseInt(ints[i]);
				}
				dbSimulator = new DBSimulator(isDBFailed, intervals);
				threads.submit(dbSimulator);
			}

			int ar = Integer.parseInt(props.getProperty(AR_WORKER_PROPERTY));
			for (int i = 0; i < ar; i++) {
				HashShardedJedis jedis = new HashShardedJedis(urls);
				RedisLease lease = new RedisLease(jedis, "client");
				RedisActiveRecoveryWorker worker = new RedisActiveRecoveryWorker(isDBFailed, jedis, lease,
						new RedisRecoveryEngine(new HashShardedJedis(urls), mongo), ar, alpha);
				workers.add(worker);
				threads.submit(worker);
			}
			watcher = new RedisEWStatsWatcher(new HashShardedJedis(urls));
			threads.submit(watcher);
			initLock.set(true);
		}
	}

	public void cleanup() throws DBException {
		if (INIT_COUNT.decrementAndGet() == 0) {
			System.out.println("clean up!!!");
			if ("load".equals(phase)) {
				System.out.println("##### insert user into database");
				this.mongo.insertMany();
			} else {
				System.out.println("##### drop database");
//				this.mongo.dropDatabase();
			}
			
			System.out.println("# of backoffs "  + RedisLease.numberOfBackOffs.get());

			workers.forEach(i -> {
				i.shutdown();
			});
			if (dbSimulator != null) {
				dbSimulator.shutdown();
			}
			watcher.shutdown();
			threads.shutdown();
		}
		jedis.close();
		this.mongo.cleanup();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		
		if (TardisClientConfig.measureSuccessWrites) {
			return Status.OK;
		}
		
		int id = jedis.getKeyServerIndex(key);
		if (fields == null) {
			StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(id, TardisClientConfig.normalKey(key)));
		} else {
			String[] fieldArray = (String[]) fields.toArray(new String[fields.size()]);
			List<String> values = jedis.hmget(id, TardisClientConfig.normalKey(key), fieldArray);

			Iterator<String> fieldIterator = fields.iterator();
			Iterator<String> valueIterator = values.iterator();

			while (fieldIterator.hasNext() && valueIterator.hasNext()) {
				result.put(fieldIterator.next(), new StringByteIterator(valueIterator.next()));
			}
			assert !fieldIterator.hasNext() && !valueIterator.hasNext();
		}

		if (result.isEmpty() && !TardisClientConfig.measureSuccessWrites) {
//			System.out.println("BUG!!!!");
			return recover(key, result);
		}
		return result.isEmpty() ? Status.ERROR : Status.OK;
	}

	public Status recover(String key, HashMap<String, ByteIterator> result) {
		int id = jedis.getKeyServerIndex(key);
		lease.acquireTillSuccess(id, TardisClientConfig.leaseKey(key));
		try {
			if (RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.READ, key))) {
				return Status.ERROR;
			}
			HashMap<String, String> mongoResult = new HashMap<>();
			this.mongo.read(TardisClientConfig.normalKey(key), mongoResult);

			StringByteIterator.putAllAsByteIterators(result, mongoResult);
			jedis.hmset(id, TardisClientConfig.normalKey(key), mongoResult);
			return Status.OK;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lease.releaseLease(id, TardisClientConfig.leaseKey(key), luaKeys);
		}
		return Status.ERROR;
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		int id = jedis.getKeyServerIndex(key);
		
		HashMap<String, String> fields = StringByteIterator.getStringMap(values); 
		
		Object response = jedis.hmset(id, TardisClientConfig.normalKey(key), fields);
		this.mongo.insert(TardisClientConfig.normalKey(key), fields);

		if (!"OK".equals(response)) {
			System.out.println("insert failed, " + table + "," + key);
			System.exit(0);
		}
		return Status.OK;
	}

	@Override
	public Status delete(String table, String key) {
		int id = jedis.getKeyServerIndex(key);
		return jedis.del(id, TardisClientConfig.normalKey(key)) == 0 ? Status.ERROR : Status.OK;
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {

		try {
			int id = jedis.getKeyServerIndex(key);
			HashMap<String, String> fields = StringByteIterator.getStringMap(values);
			
			boolean cacheKeyExist = false;
			boolean updateDBSuccess = false;
			boolean firstTimeDirty = false;

			try {
				lease.acquireTillSuccess(id, TardisClientConfig.leaseKey(key));
				
				cacheKeyExist = jedis.exists(id, TardisClientConfig.normalKey(key));
				
				if (cacheKeyExist) {
					jedis.hmset(id, TardisClientConfig.normalKey(key), fields);
				}
				
				// insert into mongodb
				if (!isDBFailed.get()) {
					
					if (!RecoveryResult.FAIL.equals(recovery.recover(RecoveryCaller.WRITE, key))) {
					  if (!TardisClientConfig.SKIP_UPDATE_MONGO) {
					    updateDBSuccess = mongo.update(TardisClientConfig.normalKey(key), fields).isOk();
					  } else {
					    updateDBSuccess = true;
					  }
					}
				} else {
					updateDBSuccess = false;
				}

				if (!updateDBSuccess) {
					firstTimeDirty = !jedis.exists(id, TardisClientConfig.bufferedWriteKey(key));
					if (cacheKeyExist) {
						// put key as dirty
						jedis.hset(id, TardisClientConfig.bufferedWriteKey(key), "d", "d");
					} else {
						jedis.hmset(id, TardisClientConfig.bufferedWriteKey(key), fields);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			} finally {
				lease.releaseLease(id, TardisClientConfig.leaseKey(key), luaKeys);
				if (firstTimeDirty && !updateDBSuccess) {
					lease.acquireLease(id, TardisClientConfig.ewLeaseKey(key));
					jedis.sadd(id, TardisClientConfig.ewKey(key), key);
					lease.releaseLease(id, TardisClientConfig.ewLeaseKey(key), luaKeys);
				}
				
				if (TardisClientConfig.measureSuccessWrites) {
					TardisClientConfig.numberOfSuccessfulWrites.incrementAndGet();
				}
			}
			return Status.OK;
		} catch (Exception e) {
			e.printStackTrace();
			if (TardisClientConfig.measureSuccessWrites) {
				System.out.println("success write " + TardisClientConfig.numberOfSuccessfulWrites.get());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				System.exit(0);
			}
			return Status.ERROR;
		}
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		return Status.NOT_IMPLEMENTED;
	}

}
