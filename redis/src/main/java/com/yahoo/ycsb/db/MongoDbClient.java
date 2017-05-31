/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
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

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package com.yahoo.ycsb.db;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.bson.Document;
import org.bson.types.Binary;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc.
 * <a href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends DB {

	/** Used to include a field in a response. */
	private static final Integer INCLUDE = Integer.valueOf(1);

	private MongoClient mongoClient;

	private static final List<Document> bulkInserts = new ArrayList<Document>();

	private static final String MONGO_DB_NAME = "ycsb";
	private static final String MONGO_TABLE = "users";
	private final String ipAddress;

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		try {
			mongoClient.close();
		} catch (Exception e1) {
			System.err.println("Could not close MongoDB connection pool: " + e1.toString());
			e1.printStackTrace();
			return;
		} finally {
			mongoClient = null;
		}
	}

	public void dropDatabase() {
		System.out.println("drop database and create collection");
		mongoClient.dropDatabase(MONGO_DB_NAME);
		MongoDatabase db = mongoClient.getDatabase(MONGO_DB_NAME);
		db.createCollection(MONGO_TABLE);
	}

	/**
	 * Delete a record from the database.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error. See the
	 *         {@link DB} class's description for a discussion of error codes.
	 */
	@Override
	public Status delete(String table, String key) {
		try {

			MongoCollection<Document> collection = this.mongoClient.getDatabase(MONGO_DB_NAME)
					.getCollection(MONGO_TABLE);

			DeleteResult result = collection.deleteOne(eq("_id", String.valueOf(key)));
			if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
				System.err.println("Nothing deleted for key " + key);
				return Status.NOT_FOUND;
			}
			return Status.OK;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	@Override
	public void init() throws DBException {
		this.mongoClient = new MongoClient(this.ipAddress, new MongoClientOptions.Builder().serverSelectionTimeout(1000)
				.connectionsPerHost(500).writeConcern(WriteConcern.ACKNOWLEDGED).build());
	}

	public MongoDbClient(String ip) {
		this.ipAddress = ip;
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error. See the
	 *         {@link DB} class's description for a discussion of error codes.
	 */
	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		try {
			Document toInsert = new Document("_id", key);
			Map<String, String> fields = StringByteIterator.getStringMap(values);

			for (Map.Entry<String, String> entry : fields.entrySet()) {
				toInsert.put(entry.getKey(), entry.getValue());
			}

			synchronized (bulkInserts) {
				bulkInserts.add(toInsert);
			}

			return Status.OK;
		} catch (Exception e) {
			System.err.println("Exception while trying bulk insert with " + bulkInserts.size());
			e.printStackTrace();
			return Status.ERROR;
		}
	}
	
	public Status insert(String key, HashMap<String, String> values) {
		try {
			Document toInsert = new Document("_id", key);

			for (Map.Entry<String, String> entry : values.entrySet()) {
				toInsert.put(entry.getKey(), entry.getValue());
			}

			synchronized (bulkInserts) {
				bulkInserts.add(toInsert);
			}

			return Status.OK;
		} catch (Exception e) {
			System.err.println("Exception while trying bulk insert with " + bulkInserts.size());
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	public void insertMany() {
		synchronized (bulkInserts) {
			System.out.println("bulk inserts " + bulkInserts.size());
			MongoCollection<Document> collection = this.mongoClient.getDatabase(MONGO_DB_NAME).getCollection(MONGO_TABLE);
			collection.insertMany(bulkInserts);
			bulkInserts.clear();
		}
	}

	/**
	 * Read a record from the database. Each field/value pair from the result
	 * will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	@Override
	public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		try {
			MongoCollection<Document> collection = this.mongoClient.getDatabase(MONGO_DB_NAME)
					.getCollection(MONGO_TABLE);

			FindIterable<Document> findIterable = collection.find(eq("_id", String.valueOf(key)));

			if (fields != null) {
				Document projection = new Document();
				for (String field : fields) {
					projection.put(field, INCLUDE);
				}
				findIterable.projection(projection);
			}

			Document queryResult = findIterable.first();

			if (queryResult != null) {
				fillMap(result, queryResult);
			}
			return queryResult != null ? Status.OK : Status.NOT_FOUND;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}

	public Status read(String key, Map<String, String> result) {
		try {
			MongoCollection<Document> collection = this.mongoClient.getDatabase(MONGO_DB_NAME)
					.getCollection(MONGO_TABLE);

			FindIterable<Document> findIterable = collection.find(eq("_id", String.valueOf(key)));
			Document queryResult = findIterable.first();
			
			if (queryResult != null) {
				queryResult.forEach((k, v) -> {
					result.put(k, String.valueOf(v));
				});
			}
			return queryResult != null ? Status.OK : Status.NOT_FOUND;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}

	/**
	 * Perform a range scan for a set of records in the database. Each
	 * field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param startkey
	 *            The record key of the first record to read.
	 * @param recordcount
	 *            The number of records to read
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A Vector of HashMaps, where each HashMap is a set field/value
	 *            pairs for one record
	 * @return Zero on success, a non-zero error code on error. See the
	 *         {@link DB} class's description for a discussion of error codes.
	 */
	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		return Status.NOT_IMPLEMENTED;
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key, overwriting any existing values with the same field name.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to write.
	 * @param values
	 *            A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> value) {
		try {
			MongoCollection<Document> collection = this.mongoClient.getDatabase(MONGO_DB_NAME)
					.getCollection(MONGO_TABLE);

			Document fieldsToSet = new Document();

			Map<String, String> values = StringByteIterator.getStringMap(value);

			for (Map.Entry<String, String> entry : values.entrySet()) {
				fieldsToSet.put(entry.getKey(), entry.getValue());
			}
			Document update = new Document("$set", fieldsToSet);

			UpdateResult result = collection.updateOne(eq("_id", String.valueOf(key)), update);
			if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
				System.err.println("Nothing updated for key " + key);
				return Status.NOT_FOUND;
			}
			return Status.OK;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}

	public Status update(String key, Map<String, String> values) {
		try {
			MongoCollection<Document> collection = this.mongoClient.getDatabase(MONGO_DB_NAME)
					.getCollection(MONGO_TABLE);

			Document fieldsToSet = new Document();

			for (Map.Entry<String, String> entry : values.entrySet()) {
				fieldsToSet.put(entry.getKey(), entry.getValue());
			}
			Document update = new Document("$set", fieldsToSet);

			UpdateResult result = collection.updateOne(eq("_id", String.valueOf(key)), update);
			if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
				System.err.println("Nothing updated for key " + key);
				return Status.NOT_FOUND;
			}
			return Status.OK;
		} catch (Exception e) {
			System.err.println(e.toString());
			return Status.ERROR;
		}
	}
	
	public static void main(String[] args) throws Exception {
		MongoDbClient client = new MongoDbClient("127.0.0.1:27017");
		client.update("Nuser2166894489591224238", new HashMap<>());
//		client.cleanup();
	}

	/**
	 * Fills the map with the values from the DBObject.
	 * 
	 * @param resultMap
	 *            The map to fill/
	 * @param obj
	 *            The object to copy values from.
	 */
	protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
		for (Map.Entry<String, Object> entry : obj.entrySet()) {
			if (entry.getValue() instanceof Binary) {
				resultMap.put(entry.getKey(), new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
			}
		}
	}
}
