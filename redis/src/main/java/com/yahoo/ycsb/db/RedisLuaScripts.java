package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;

public class RedisLuaScripts {
		
	public static String RELEASE_LEASE = "if redis.call(\"GET\", KEYS[1]) == KEYS[2] then return redis.call(\"DEL\", KEYS[1]) end return 0";
	
	public static String MULTI_SET_SCARD = "local l2 = {}; for key,value in ipairs({unpack(KEYS)}) do l2[key]=redis.call(\"SCARD\", value) end; return l2";
	
	public static String MULTI_EXISTS = "local l2 = {}; for key,value in ipairs({unpack(KEYS)}) do l2[key]=redis.call(\"EXIST\", value) end; return l2";
	
	public static String MULTI_EXISTS_2 = "local l2 = {}; local l1 = redis.call(\"SMEMBERS\", KEYS[1]); for key,value in ipairs({unpack(ARGV)}) do l2[key]=redis.call(\"EXISTS\", value) end; return {l1, l2}";
	
	public static List<String> luaScripts = new ArrayList<>();

	static {
		luaScripts.add(RELEASE_LEASE);
		luaScripts.add(MULTI_SET_SCARD);
		luaScripts.add(MULTI_EXISTS_2);
		luaScripts.add(MULTI_EXISTS);
	}

	public static void main(String[] ab) {
//		viewProfile();
//		acceptFriendInvitee();
//		removeIfExist();
//		twoSetGet();
		multiSetScard();
//		multiSetScard2();
//		SADD_IF_EXIST();
	}
	
	private static void multiSetScard() {
		System.out.println("multiSetScard");
		Jedis client = new Jedis();

		client.scriptFlush();
		client.flushAll();

		String hash = client.scriptLoad(MULTI_SET_SCARD);
		System.out.println(hash);
//		client.sadd("a", "t1");

		List<String> keys = new ArrayList<>();
		keys.add("a");
		keys.add("t1");

		System.out.println((List<Integer>)client.evalsha(hash, keys, keys));

		client.close();
	}
}
