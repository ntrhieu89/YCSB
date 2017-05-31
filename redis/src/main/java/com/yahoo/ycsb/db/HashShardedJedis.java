package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;

/**
 * 
 * sharded jedis based on user id
 * 
 * @author haoyuh
 *
 */
public class HashShardedJedis {

	List<Jedis> clients = new ArrayList<>();
	private static final Map<String, Map<Integer, String>> scriptNameServerIndexSHAMap = new HashMap<>();

	public HashShardedJedis(String... urls) {
		for (String url : urls) {
			String[] ip = url.split(":");
			Jedis client = new Jedis(ip[0], Integer.parseInt(ip[1]));
			System.out.println(url);
			client.connect();
			clients.add(client);
		}
	}

	public void loadScript() {
		RedisLuaScripts.luaScripts.forEach(i -> {
			System.out.println("load script " + i);
			scriptLoad(i);
		});
	}

	public void scriptLoad(String script) {
		Map<Integer, String> map = new HashMap<>();
		for (int i = 0; i < clients.size(); i++) {
			String sha = clients.get(i).scriptLoad(script);
			map.put(i, sha);
		}
		scriptNameServerIndexSHAMap.put(script, map);
	}

	public int getKeyServerIndex(String key) {
		if (clients.size() == 1) {
			return 0;
		}
		return (int) (Long.parseLong(key.substring(4)) % clients.size());
	}

	public long setnx(int id, String key, String value) {
		return clients.get(id).setnx(key, value);
	}

	public Map<String, String> hgetAll(int id, String key) {
		return clients.get(id).hgetAll(key);
	}

	public List<String> hmget(int id, String key, String[] fields) {
		return clients.get(id).hmget(key, fields);
	}

	public boolean exists(int id, String key) {
		return clients.get(id).exists(key);
	}

	public Object hmset(int id, String key, HashMap<String, String> stringMap) {
		return clients.get(id).hmset(key, stringMap);
	}

	public Long hset(int id, String key, String field, String value) {
		return clients.get(id).hset(key, field, value);
	}

	public Set<String> smembers(int id, String key) {
		return clients.get(id).smembers(key);
	}

	public long del(int id, String key) {
		return clients.get(id).del(key);
	}

	@SuppressWarnings("unchecked")
	public Map<String, Boolean> mexists(List<String> keys, int ewIndex, List<String> args, String prefix,
			Set<String> resultList) {
		Map<Integer, List<String>> serverKeysMap = new HashMap<>();
		Map<String, Boolean> result = new HashMap<>();

		args.stream().forEach(i -> {
			int serverId = getKeyServerIndex(i);
			List<String> list = serverKeysMap.get(serverId);
			if (list == null) {
				list = new ArrayList<>();
				serverKeysMap.put(serverId, list);
			}
			list.add(prefix + i);
		});

		serverKeysMap.forEach((serverId, keyss) -> {
			Jedis client = clients.get(serverId);

			if (serverId == getServerIndex(ewIndex)) {
				List<List<?>> response = (List<List<?>>) client.evalsha(
						scriptNameServerIndexSHAMap.get(RedisLuaScripts.MULTI_EXISTS_2).get(serverId), keys, keyss);

				resultList.addAll((List<String>) response.get(0));

				for (int i = 0; i < keys.size(); i++) {
					result.put(keys.get(i), (Long) response.get(1).get(i) == 0 ? false : true);
				}
			} else {
				List<?> response = (List<?>) client.evalsha(
						scriptNameServerIndexSHAMap.get(RedisLuaScripts.MULTI_EXISTS).get(serverId), keyss, Collections.emptyList());

				for (int i = 0; i < keys.size(); i++) {
					result.put(keys.get(i), (Long) response.get(i) == 0 ? false : true);
				}
			}

		});
		return result;
	}

	public void srem(int id, String key, String[] values) {
		clients.get(id).srem(key, values);
	}

	public Object evalsha(int id, String script, List<String> keys, List<String> args) {
		return clients.get(id).evalsha(scriptNameServerIndexSHAMap.get(script).get(id), keys, args);
	}

	public void sadd(int id, String key, String value) {
		clients.get(id).sadd(key, value);
	}

	@SuppressWarnings("unchecked")
	public Map<String, Long> mscard(List<Integer> ids, String prefix) {
		Map<Integer, List<String>> serverKeysMap = new HashMap<>();
		Map<String, Long> result = new HashMap<>();

		ids.stream().forEach(i -> {
			int serverId = getServerIndex(i);

			List<String> list = serverKeysMap.get(serverId);
			if (list == null) {
				list = new ArrayList<>();
				serverKeysMap.put(serverId, list);
			}
			list.add(prefix + i);
		});

		serverKeysMap.forEach((serverId, keys) -> {
			Jedis client = clients.get(serverId);
			List<Long> response = (List<Long>) client.evalsha(
					scriptNameServerIndexSHAMap.get(RedisLuaScripts.MULTI_SET_SCARD).get(serverId), keys,
					Collections.<String> emptyList());
			for (int i = 0; i < keys.size(); i++) {
				result.put(keys.get(i), response.get(i));
			}
		});
		return result;
	}

	public void close() {
		clients.forEach(i -> {
			i.close();
		});
	}

	public int getServerIndex(Integer i) {
		if (clients.size() == 1) {
			return 0;
		}
		return i % clients.size();
	}

}
