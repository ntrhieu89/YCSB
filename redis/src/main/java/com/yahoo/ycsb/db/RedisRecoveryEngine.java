package com.yahoo.ycsb.db;

import static com.yahoo.ycsb.db.TardisClientConfig.*;

import java.util.Map;

import com.yahoo.ycsb.Status;

public class RedisRecoveryEngine {
	private final HashShardedJedis redis;
	private final MongoDbClient mongo;

	public RedisRecoveryEngine(HashShardedJedis redis, MongoDbClient mongo) {
		super();
		this.redis = redis;
		this.mongo = mongo;
	}

	public RecoveryResult recover(RecoveryCaller caller, String key) {
		int id = redis.getKeyServerIndex(key);

		String bufferedWriteKey = bufferedWriteKey(key);
		String normalKey = normalKey(key);

		if (!redis.exists(id, bufferedWriteKey)) {
			return RecoveryResult.CLEAN;
		}

		Map<String, String> bufferedWrites = redis.hgetAll(id, normalKey);

		if (bufferedWrites.isEmpty()) {
			bufferedWrites = redis.hgetAll(id, bufferedWriteKey);
		}

		if (!Status.OK.equals(mongo.update(normalKey(key), bufferedWrites))) {
			return RecoveryResult.FAIL;
		}
		redis.del(id, bufferedWriteKey);
		return RecoveryResult.SUCCESS;
	}

}
