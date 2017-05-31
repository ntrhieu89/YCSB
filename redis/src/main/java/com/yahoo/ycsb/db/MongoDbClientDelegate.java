package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;

public class MongoDbClientDelegate extends MongoDbClient {

	private final AtomicBoolean isDBFailed;

	public MongoDbClientDelegate(String ip, AtomicBoolean isDBFailed) {
		super(ip);
		this.isDBFailed = isDBFailed;
	}

	@Override
	public Status delete(String table, String key) {
		if (isDBFailed.get()) {
			return Status.ERROR;
		}
		return super.delete(table, key);
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		if (isDBFailed.get()) {
			return Status.ERROR;
		}
		return super.insert(table, key, values);
	}

	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		if (isDBFailed.get()) {
			return Status.ERROR;
		}
		return super.read(table, key, fields, result);
	}

	@Override
	public Status read(String key, Map<String, String> result) {
		if (isDBFailed.get()) {
			return Status.ERROR;
		}
		return super.read(key, result);
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> value) {
		if (isDBFailed.get()) {
			return Status.ERROR;
		}
		return super.update(table, key, value);
	}

	@Override
	public Status update(String key, Map<String, String> values) {
		if (isDBFailed.get()) {
			return Status.ERROR;
		}
		return super.update(key, values);
	}

}
