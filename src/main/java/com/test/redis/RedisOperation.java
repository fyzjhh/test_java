package com.test.redis;

import redis.clients.jedis.Jedis;

public class RedisOperation {
	public int count = 0;
	/** ����Redis server��Client��ʼ�� */
	Jedis jredis = null;

	public RedisOperation() {
		jredis = new Jedis("172.17.2.163", 6379);
	}

	/** ��������Ϊinsert */
	public void executeInsertPrepare(long loopCount, long sno, String name) {
		try {

			for (long i = 1; i <= loopCount; i++) {
				jredis.set(String.valueOf(i + sno), name);
				count++;
			}
		} catch (Exception e) {
			System.out.format("insert error :%s\n", e.getMessage());
		}

	}

	/** ��������Ϊselect */
	public void queryPrepare(long loopCount, long sno) {
		try {

			for (long i = 1; i <= loopCount; i++) {
				jredis.get(String.valueOf(i + sno));
				count++;
			}
		} catch (Exception e) {
			System.out.format("query error :%s\n", e.getMessage());
		}

	}

	/** ��������Ϊupdate */
	public void executeUpdatePrepare(long loopCount, long sno, String name) {
		try {

			for (long i = 1; i <= loopCount; i++) {
				jredis.set(String.valueOf(i + sno), name);
				count++;
			}
		} catch (Exception e) {
			System.out.format("update error :%s\n", e.getMessage());
		}

	}
}
