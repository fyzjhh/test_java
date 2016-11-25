package com.test.memcache;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;



public class TestJavaMemcache {
	/** 开启的并发线程数 */
	static int threads = 5;
	/** 每个线程执行的操作次数 */
	static long runs = 50000;
	/** 操作类型，0-insert,1-select,2-update */
	static int type = 0;
	static Integer myLock = threads;
	/** 同个各个线程执行情况的周期 */
	static int interval = 1;

	static long insertTimes = 0;

	static long selectTimes = 0;

	static long upadteTimes = 0;

	static MemcacheManagerThread manager;
	static String SO = "0123456789";
	static String SP = SO + SO + SO + SO + SO + SO + SO + SO + SO + SO;
	static String SQ = SP + SP + SP + SP + SP + SP + SP + SP + SP + SP;

	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws Exception {

		testmultithreadmemcache(args);
	}

	private static void testmultithreadmemcache(String[] args) throws Exception {
		if (args.length == 4) {
			threads = Integer.parseInt(args[0]);
			runs = Integer.parseInt(args[1]);
			type = Integer.parseInt(args[2]);
			interval = Integer.parseInt(args[3]);
		}
		myLock = new Integer(threads);
		System.out.println("Memcache start: " + format.format(new Date())
				+ ", interval second: " + interval);

		manager = new MemcacheManagerThread(threads, interval);

		for (int i = 0; i < threads; i++) {
			MemcacheWorkerThread thread = new MemcacheWorkerThread();
			thread.start();
			manager.threads[i] = thread;
			Thread.sleep(500);
		}

		manager.start();

	}

	public static class MemcacheWorkerThread extends Thread {
		MemcacheOperation Memcacheop = new MemcacheOperation();

		public void run() {
			/** 操作类型为insert */
			if (type == 0) {
				long startTime = System.currentTimeMillis();
				Memcacheop.executeInsertPrepare(runs, 10000000L, SP);
				long time = System.currentTimeMillis() - startTime;

				synchronized (myLock) {
					insertTimes += time;

					myLock--;
					if (myLock.equals(0)) {
						System.out
								.println("***********************test  result**********************");
						System.out.println("over! threads: " + threads
								+ ", runs/threads: " + runs);

						System.out.println("insert total time: " + insertTimes
								/ 1000 + "s, the last thread  time: " + time
								/ 1000 + "s, avg: " + (long) runs * threads
								* 1000 / time + " insert/s");
						manager.interrupt();
						manager.finished = true;
					}
				}
			}
			/** 操作类型为select */
			else if (type == 1) {
				long startTime = System.currentTimeMillis();
				Memcacheop.queryPrepare(runs, 10000000L);
				long time = System.currentTimeMillis() - startTime;

				synchronized (myLock) {
					selectTimes += time;

					myLock--;
					if (myLock.equals(0)) {
						System.out
								.println("***********************test  result**********************");
						System.out.println("over! threads: " + threads
								+ ", runs/threads: " + runs);

						System.out.println("select total time: " + selectTimes
								/ 1000 + "s, the last thread time: " + time
								/ 1000 + "s, avg: " + (long) runs * threads
								* 1000 / time + " select/s");
						manager.interrupt();
						manager.finished = true;
					}
				}
			}/** 操作类型为update */
			else {
				long startTime = System.currentTimeMillis();

				Memcacheop.executeUpdatePrepare(runs, 10000000L, SP);

				long time = System.currentTimeMillis() - startTime;

				synchronized (myLock) {
					upadteTimes += time;

					myLock--;
					if (myLock.equals(0)) {
						System.out
								.println("***********************test result**********************");
						System.out.println("over! threads: " + threads
								+ ", runs/threads: " + runs);

						System.out.println("update total time: " + upadteTimes
								/ 1000 + "s, the last thread time: " + time
								/ 1000 + "s, avg: " + (long) runs * threads
								* 1000 / time + " update/s");
						manager.interrupt();
						manager.finished = true;
					}
				}
			}

		}
	}

	public static void testselect() throws Exception {

		String[] servers = { "172.17.2.163:11231" };
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(servers);
		pool.setFailover(true);
		pool.setInitConn(10);
		pool.setMinConn(5);
		pool.setMaxConn(250);
		pool.setMaintSleep(30);
		pool.setNagle(false);
		pool.setSocketTO(3000);
		pool.setAliveCheck(true);
		pool.initialize();

		MemCachedClient memCachedClient = new MemCachedClient();
		Date startdate = new Date();
		System.out.println("start:" + format.format(startdate));
		for (int i = 0; i < 1000000; i++) {
			memCachedClient.get("" + i);
			if (i % 10000 == 9999) {
				System.out.println(format.format(new Date())
						+ " keys number were got !!! : " + i);
			}
		}
		Date enddate = new Date();
		System.out.println("end:" + format.format(enddate));
		System.out.println(" memcache time costed in seconds :"
				+ (enddate.getTime() - startdate.getTime()) / 1000);
		pool.shutDown();
	}

	public static void testinsert() throws Exception {

		String[] servers = { "172.17.2.163:11231" };
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(servers);
		pool.setFailover(true);
		pool.setInitConn(10);
		pool.setMinConn(5);
		pool.setMaxConn(250);
		pool.setMaintSleep(30);
		pool.setNagle(false);
		pool.setSocketTO(3000);
		pool.setAliveCheck(true);
		pool.initialize();

		MemCachedClient memCachedClient = new MemCachedClient();
		Date startdate = new Date();
		System.out.println("start:" + format.format(startdate));
		for (int i = 0; i < 1000000; i++) {
			memCachedClient.set("" + i, SP);
			if (i % 10000 == 9999) {
				System.out.println(format.format(new Date())
						+ " keys number were stored !!! : " + i);
			}
		}
		Date enddate = new Date();
		System.out.println("end:" + format.format(enddate));
		System.out.println(" memcache time costed in seconds :"
				+ (enddate.getTime() - startdate.getTime()) / 1000);
		pool.shutDown();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void teststat(String resultfile) throws Exception {

		BufferedWriter output = new BufferedWriter(new FileWriter(resultfile));

		String[] servers = { "172.17.2.163:11231" };
		SockIOPool pool = SockIOPool.getInstance();
		pool.setServers(servers);
		pool.setFailover(true);
		pool.setInitConn(10);
		pool.setMinConn(5);
		pool.setMaxConn(250);
		pool.setMaintSleep(30);
		pool.setNagle(false);
		pool.setSocketTO(3000);
		pool.setAliveCheck(true);
		pool.initialize();

		Set itemset = new HashSet();
		MemCachedClient memCachedClient = new MemCachedClient();
		Map<String, Map<String, String>> mainmap = memCachedClient.statsItems();

		Iterator mainiter = mainmap.entrySet().iterator();
		while (mainiter.hasNext()) {
			Map.Entry mainentry = (Map.Entry) mainiter.next();
			// String maimkey = (String) mainentry.getKey();
			Map<String, String> itemmap = (Map<String, String>) mainentry
					.getValue();
			// System.out.println(maimkey + "         ");

			Iterator itemiter = itemmap.entrySet().iterator();
			while (itemiter.hasNext()) {
				Map.Entry itementry = (Map.Entry) itemiter.next();
				String itemkey = (String) itementry.getKey();

				String itemname = itemkey.split(":")[1];
				itemset.add(new Integer(itemname));
				// String itemval = (String) itementry.getValue();
				// System.out.println(itemkey + "    ::::     " + itemval);
			}

		}
		List keyList = new ArrayList();

		List itemlists = Arrays.asList(itemset.toArray());
		Collections.sort(itemlists);
		for (Iterator iterator = itemlists.iterator(); iterator.hasNext();) {
			Integer itemnum = (Integer) iterator.next();
			// System.out.println("=====" + object);
			Map<String, Map<String, String>> allmap = memCachedClient
					.statsCacheDump(itemnum, 0);
			Iterator xxxiter = allmap.entrySet().iterator();
			while (xxxiter.hasNext()) {
				Map.Entry mainentry = (Map.Entry) xxxiter.next();
				// String xxxkey = (String) mainentry.getKey();
				Map<String, String> yyymap = (Map<String, String>) mainentry
						.getValue();
				// System.out.println(xxxkey + "         ");

				Iterator yyyiter = yyymap.entrySet().iterator();
				while (yyyiter.hasNext()) {
					Map.Entry yyyentry = (Map.Entry) yyyiter.next();
					String yyykey = (String) yyyentry.getKey();
					keyList.add(new Integer(yyykey));
					// String yyyval = (String) yyyentry.getValue();
					// System.out.println(yyykey + "    ::::     " + yyyval);
				}

			}
			// System.out.println(object);
		}

		Collections.sort(keyList);
		output.write("key count:" + keyList.size() + "\r\n");
		output.write("key list:" + keyList);
		output.close();
	}
}
