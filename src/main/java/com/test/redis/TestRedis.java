package com.test.redis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;

public class TestRedis {
	/** �����Ĳ����߳��� */
	static int threads = 5;
	/** ÿ���߳�ִ�еĲ������� */
	static long runs = 50000;
	/** �������ͣ�0-insert,1-select,2-update */
	static int type = 0;
	static Integer myLock = threads;
	/** ͬ�������߳�ִ����������� */
	static int interval = 1;

	static long insertTimes = 0;

	static long selectTimes = 0;

	static long upadteTimes = 0;

	static RedisManagerThread manager;

	static String SO = "0123456789";
	static String SP = SO + SO + SO + SO + SO + SO + SO + SO + SO + SO;
	static String SQ = SP + SP + SP + SP + SP + SP + SP + SP + SP + SP;
	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws Exception {

		selectall();

	}

	// userAttr:ip:003b1fa7-cbca-462f-aa5e-14c3ba99889d set
	// userAttr:userAgent:00315e6b-9e65-409d-923e-360cd3983049 set
	// userTrack:0023c390-67c9-4fca-86c4-d0907ca3cf1d:1 list
	// visitTime:2015-08-23 set

	@SuppressWarnings({ "rawtypes" })
	private static void selectall() throws Exception {
		// Jedis jedis = new Jedis("192.168.0.223", 6380);
		// jedis.auth("CFZFnlvmybr7h1w7Go8GKw6fiDI7hD5L");
		Jedis jedis = new Jedis("101.71.39.62", 6380);
		jedis.auth("ZlfARlo4Gw0EynkwF7c+0PdNlqHdlf");
		jedis.connect();
		jedis.select(4);

		String[] key_arr = new String[] { "userAttr:ip:",
				"userAttr:userAgent:", "userTrack:", "visitTime:" };
		String[] type_arr = new String[] { "set", "set", "list", "set" };
		String[] part_arr = new String[] { "uuid", "uuid", "uuid", "date" };

		for (int i = 0; i < key_arr.length; i++) {
			String part = part_arr[i];
			String type = type_arr[i];
			if (part.equalsIgnoreCase("uuid")) {

				for (int j = 0; j < 10; j++) {

					String skey = key_arr[i] + j + "00*";
					Set keys = jedis.keys(skey);
					System.out.println("========keys count : " + keys.size());
					Iterator it = keys.iterator();
					int cnt = 0;
					while (it.hasNext()) {
						String key = (String) it.next();
						if (type.equalsIgnoreCase("set")) {
							Set<String> s = jedis.smembers(key);
							for (Iterator iterator = s.iterator(); iterator
									.hasNext();) {
								Object object = (Object) iterator.next();
								System.out.println(cnt + "\t" + key + "\t"
										+ object);
							}
						}
						if (type.equalsIgnoreCase("list")) {
							List s = jedis.lrange(key, 0, -1);
							for (Iterator iterator = s.iterator(); iterator
									.hasNext();) {
								Object object = (Object) iterator.next();
								System.out.println(cnt + "\t" + key + "\t"
										+ object);
							}
						}

					}
				}
			}

			if (part.equalsIgnoreCase("date")) {
				String start_date = "2015-08-01";
				String date;
				for (int j = 0; j < 180; j++) {
					date = addDay(start_date, j);
					String skey = key_arr[i] + date + "*";
					Set keys = jedis.keys(skey);
					System.out.println("========keys count : " + keys.size());
					Iterator it = keys.iterator();
					int cnt = 0;
					while (it.hasNext()) {
						String key = (String) it.next();
						if (type.equalsIgnoreCase("set")) {
							Set<String> s = jedis.smembers(key);
							for (Iterator iterator = s.iterator(); iterator
									.hasNext();) {
								Object object = (Object) iterator.next();
								System.out.println(cnt + "\t" + key + "\t"
										+ object);
							}
						}
						if (type.equalsIgnoreCase("list")) {
							List s = jedis.lrange(key, 0, -1);
							for (Iterator iterator = s.iterator(); iterator
									.hasNext();) {
								Object object = (Object) iterator.next();
								System.out.println(cnt + "\t" + key + "\t"
										+ object);
							}
						}

					}
				}

			}

		}

		jedis.close();

	}

	static DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");

	public static String addDay(String action_day, int i) throws ParseException {
		Calendar cal = Calendar.getInstance();
		Date tmpdate = dateformat.parse(action_day);
		cal.setTime(tmpdate);
		cal.add(Calendar.DAY_OF_MONTH, i);
		tmpdate = cal.getTime();
		return dateformat.format(tmpdate);
	}

	private static void testmultithreadredis(String[] args) throws Exception {
		if (args.length == 4) {
			threads = Integer.parseInt(args[0]);
			runs = Integer.parseInt(args[1]);
			type = Integer.parseInt(args[2]);
			interval = Integer.parseInt(args[3]);
		}

		myLock = new Integer(threads);
		System.out.println("Redis start: " + format.format(new Date())
				+ ", interval second: " + interval);

		manager = new RedisManagerThread(threads, interval);

		for (int i = 0; i < threads; i++) {
			RedisWorkerThread thread = new RedisWorkerThread();
			thread.start();
			manager.threads[i] = thread;
			Thread.sleep(500);
		}

		manager.start();

	}

	public static class RedisWorkerThread extends Thread {
		RedisOperation redisop = new RedisOperation();

		public void run() {
			/** ��������Ϊinsert */
			if (type == 0) {
				long startTime = System.currentTimeMillis();
				redisop.executeInsertPrepare(runs, 10000000L, SP);
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
			/** ��������Ϊselect */
			else if (type == 1) {
				long startTime = System.currentTimeMillis();
				redisop.queryPrepare(runs, 10000000L);
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
			}/** ��������Ϊupdate */
			else {
				long startTime = System.currentTimeMillis();

				redisop.executeUpdatePrepare(runs, 10000000L, SP);

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

	@SuppressWarnings(value = { "rawtypes", "unchecked" })
	public static void teststats(String resultfile) throws Exception {

		BufferedWriter output;

		output = new BufferedWriter(new FileWriter(resultfile));

		Jedis jedis = new Jedis("172.17.2.163", 6379);

		jedis.connect();

		Set keys = jedis.keys("*");

		List keyList = new ArrayList(keys);
		Collections.sort(keyList, new MyCom());
		output.write("key count:" + keyList.size() + "\r\n");
		output.write("key list:" + keyList);

		output.close();

	}

	@SuppressWarnings("rawtypes")
	private static void testselect() {
		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();
		Date startdate = new Date();
		System.out.println("start:" + format.format(startdate));
		for (int i = 0; i < 1000000; i++) {
			jedis.get("" + i);
			if (i % 10000 == 9999) {
				System.out.println(format.format(new Date())
						+ " keys number were got !!! : " + i);
			}
		}
		Date enddate = new Date();
		System.out.println("end:" + format.format(enddate));
		System.out.println(" redis time costed in seconds : "
				+ (enddate.getTime() - startdate.getTime()) / 1000);
		jedis.disconnect();
	}

	@SuppressWarnings("rawtypes")
	private static void testinsert() {
		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();
		Date startdate = new Date();
		System.out.println("start:" + format.format(startdate));
		for (int i = 0; i < 1000000; i++) {
			jedis.set("" + i, SP);
			if (i % 10000 == 9999) {
				System.out.println(format.format(new Date())
						+ " keys number were stored !!! : " + i);
			}
		}
		Date enddate = new Date();
		System.out.println("end:" + format.format(enddate));
		System.out.println(" redis time costed in seconds : "
				+ (enddate.getTime() - startdate.getTime()) / 1000);
		jedis.disconnect();
	}

	@SuppressWarnings("rawtypes")
	private static void testlist() throws Exception {
		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();

		Set keys = jedis.keys("*");
		System.out.println("keys count : " + keys.size());
		List<Integer> numList = new ArrayList<Integer>(300);
		for (int i = 0; i < 300; i++) {
			numList.add(0);
		}
		List<Object> dataList = Arrays.asList(keys.toArray());
		for (int j = 0; j < dataList.size(); j++) {

			int num = new Integer(dataList.get(j).toString());
			int lasnum = numList.get(num / 100);
			numList.set(num / 100, lasnum + 1);

		}

		for (int k = 0; k < numList.size(); k++) {
			Integer numdata = numList.get(k);
			System.out.println("count(num >= " + k * 100 + "0 && num < "
					+ (k + 1) * 100 + ") :" + numdata);
			// output.write("count(num >= " + k * 100 + "0 && num < " + (k + 1)
			// * 100 + ") :" + numdata + "\r\n");
		}
		//
		// for (int m = 0; m < 10; m++) {
		// jedis.lpush("list", String.valueOf(m));
		// }
		//
		// keys = jedis.keys("*");
		// System.out.println("keys count : " + keys.size());

		jedis.disconnect();

	}

	private static void teststat(String filename, String resultfile)
			throws Exception {

		BufferedWriter output = new BufferedWriter(new FileWriter(resultfile));

		File file = new File(filename);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));

			for (int m = 0; m < 100; m++) {

				String line1 = reader.readLine();
				if (line1 == null
						|| !line1.matches(".*time.*timeoffset.*key count.*")) {
					continue;
				}

				String line2 = reader.readLine();
				String line3 = reader.readLine();
				String line4 = reader.readLine();
				String line5 = reader.readLine();
				String line6 = reader.readLine();
				String line7 = reader.readLine();

				if (line7 != null && line7.matches("\\[.*\\]")) {

					List<Integer> numList = new ArrayList<Integer>(100);
					for (int i = 0; i < 100; i++) {
						numList.add(0);
					}

					String datas = line7.substring(1, line7.length() - 1);
					List<String> dataList = Arrays.asList(datas.split(" *, *"));
					for (int j = 0; j < dataList.size(); j++) {

						int num = new Integer(dataList.get(j).toString());
						int lasnum = numList.get(num / 100);
						numList.set(num / 100, lasnum + 1);

					}

					output.write(line1 + "\r\n" + line2 + "\r\n" + line3
							+ "\r\n" + line4 + "\r\n" + line5 + "\r\n" + line6
							+ "\r\n");
					// System.out.println(line1 + "\r\n" + line2 + "\r\n" +
					// line3
					// + "\r\n" + line4 + "\r\n" + line5 + "\r\n" + line6);

					for (int k = 0; k < numList.size(); k++) {
						Integer numdata = numList.get(k);
						// System.out.println("count(num >= " + k * 100
						// + "0 && num < " + (k + 1) * 100 + ") :"
						// + numdata);
						output.write("count(num >= " + k * 100 + "0 && num < "
								+ (k + 1) * 100 + ") :" + numdata + "\r\n");
					}

				}
			}

			reader.close();
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}

	}

	@SuppressWarnings("rawtypes")
	private static void testredis2() throws InterruptedException {
		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();

		Set keys = jedis.keys("*");
		System.out.println("keys count : " + keys.size());

		for (int i = 0; i < 3000; i++) {

			String status = jedis.setex(String.valueOf(i), 95 - (i / 100), SQ
					+ i);
			if ("OK".equals(status) == false) {
				System.out.println("==== var" + i + " is not setted !!");
			}
		}
		System.out.println(format.format(new Date())
				+ "    0-2999 insert finished");
		for (int i = 3000; i < 6000; i++) {
			String status = jedis.set(String.valueOf(i), SQ + i);
			if ("OK".equals(status) == false) {
				System.out.println("==== var" + i + " is not setted !!");
			}
		}
		System.out.println(format.format(new Date())
				+ "    3000-5999 insert finished");
		for (int i = 0; i < 500; i++) {
			int time = 5 + (i % 6);
			for (int j = 0; j < time; j++) {
				jedis.get(String.valueOf(i));
			}
		}
		for (int i = 3000; i < 3500; i++) {
			int time = 1 + (i % 5);
			for (int j = 0; j < time; j++) {
				jedis.get(String.valueOf(i));
			}
		}
		System.out.println(format.format(new Date()) + "    read finished");
		for (int i = 6000; i < 10000; i++) {
			String status = jedis.set(String.valueOf(i), SQ + i);
			if ("OK".equals(status) == false) {
				System.out.println("==== var" + i + " is not setted !!");
			}
		}
		System.out.println(format.format(new Date())
				+ "    6000-9999 insert finished");
		keys = jedis.keys("*");
		System.out.println("keys count : " + keys.size());
	}

	@SuppressWarnings("rawtypes")
	private static void testredis1() throws InterruptedException {
		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();

		Set keys = jedis.keys("*");
		System.out.println("keys count : " + keys.size());

		for (int i = 0; i < 300; i++) {

			String status = jedis
					.setex(String.valueOf(i), (i / 5) + 20, SQ + i);
			if ("OK".equals(status) == false) {
				System.out.println("==== var" + i + " is not setted !!");
			}
		}

		for (int i = 300; i < 600; i++) {
			String status = jedis.set(String.valueOf(i), SQ + i);
			if ("OK".equals(status) == false) {
				System.out.println("==== var" + i + " is not setted !!");
			}
		}
		Thread.sleep(1000);
		for (int i = 0; i < 300; i++) {

			for (int j = 0; j < (i % 10); i++) {
				jedis.get(String.valueOf(i));
			}
		}
		for (int i = 300; i < 600; i++) {
			for (int j = 0; j < (i % 5); i++) {
				jedis.get(String.valueOf(i));
			}
		}

		Thread.sleep(1000);
		for (int i = 600; i < 1000; i++) {
			String status = jedis.set(String.valueOf(i), SQ + i);
			if ("OK".equals(status) == false) {
				System.out.println("==== var" + i + " is not setted !!");
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private static void testinsertfull() {
		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();
		Set keys = jedis.keys("*");
		System.out.println("key count : " + keys.size());
		for (int i = 0; i < 3000; i++) {

			String status = jedis.setex(String.valueOf(i), (i / 40) + 60, SQ
					+ i);
			if ("OK".equals(status) == false) {
				System.out.println("==== var" + i + " is not setted !!");
			}
		}
		for (int i = 3000; i < 10000; i++) {
			String status = jedis.set(String.valueOf(i), SQ + i);
			if ("OK".equals(status) == false) {
				System.out.println("==== var" + i + " is not setted !!");
			}
		}
		keys = jedis.keys("*");
		System.out.println("key count : " + keys.size());

	}

	@SuppressWarnings("static-access")
	private static void testdel() throws Exception {
		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();
		Set keys = jedis.keys("*");
		System.out.println(keys.size());

		for (int i = 0; i < 100; i++) {
			Long status = jedis.del(String.valueOf(i));

		}
	}

	@SuppressWarnings("static-access")
	private static void testtl() throws Exception {
		Jedis jedis = new Jedis("172.17.2.163", 6379, 500);
		jedis.connect();
		jedis.setex("ttl1", 100, "valuesttl1");
		System.out.println(jedis.ttl("ttl1"));
		Thread.currentThread().sleep(10000);
		System.out.println(jedis.ttl("ttl1"));
		Thread.currentThread().sleep(10000);
		System.out.println(jedis.ttl("ttl1"));
	}

}
