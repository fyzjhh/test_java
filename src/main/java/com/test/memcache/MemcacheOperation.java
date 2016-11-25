package com.test.memcache;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;

public class MemcacheOperation {
	public int count = 0;
	/** 访问Memcache server的Client初始化 */
	MemCachedClient memCachedClient = null;

	static {
		String[] servers = { "172.17.2.163:11231" };
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(servers);
		pool.setFailover(true);
		pool.setInitConn(50);
		pool.setMinConn(20);
		pool.setMaxConn(500);
		pool.setMaintSleep(30);
		pool.setNagle(false);
		pool.setSocketTO(60000);
		pool.setAliveCheck(true);
		pool.initialize();
	}

	public MemcacheOperation() {

		memCachedClient = new MemCachedClient();
		// try {
		// InetSocketAddress isa = new InetSocketAddress("172.17.2.163", 11231);
		// memCachedClient = new MemcachedClient(isa);
		// } catch (IOException e) {
		// System.out.println("can not connect to 172.17.2.163:11231 ");
		// e.printStackTrace();
		// }
	}

	/** 操作类型为insert */
	public void executeInsertPrepare(long loopCount, long sno, String name) {
		try {

			for (long i = 1; i <= loopCount; i++) {
				memCachedClient.set(String.valueOf(i + sno), name);
				count++;
			}
		} catch (Exception e) {
			System.out.format("insert error :%s\n", e.getMessage());
		}
	}

	/** 操作类型为select */
	public void queryPrepare(long loopCount, long sno) {
		try {

			for (long i = 1; i <= loopCount; i++) {
				memCachedClient.get(String.valueOf(i + sno));
				count++;
			}
		} catch (Exception e) {
			System.out.format("query error :%s\n", e.getMessage());
		}

	}

	/** 操作类型为update */
	public void executeUpdatePrepare(long loopCount, long sno, String name) {
		try {

			for (long i = 1; i <= loopCount; i++) {
				memCachedClient.set(String.valueOf(i + sno), name);
				count++;

			}
		} catch (Exception e) {
			System.out.format("update error :%s\n", e.getMessage());
		}

	}
}
