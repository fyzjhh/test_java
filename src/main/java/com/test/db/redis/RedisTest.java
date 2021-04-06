package com.test.redis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class RedisTest {
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

	static RedisManagerThread manager;

	static String SO = "0123456789";
	static String SP = SO + SO + SO + SO + SO + SO + SO + SO + SO + SO;
	static String SQ = SP + SP + SP + SP + SP + SP + SP + SP + SP + SP;
	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws Exception {

		testinsert();
	}

	private static void testpub(String[] args) {

		try {
			Socket socket = new Socket("172.17.2.163", 6379);
			OutputStream out = socket.getOutputStream();

			out.write("publish news.test1 \"jhh1\" \r\n".getBytes());
			out.write("publish news.test2 \"jhh2\" \r\n".getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void testsub(String[] args) {
		String cmd = args + "\r\n";
		try {
			Socket socket = new Socket("172.17.2.163", 6379);
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();

			out.write(cmd.getBytes()); // 发送订阅命令
			byte[] buffer = new byte[1024];
			while (true) {
				int readCount = in.read(buffer);
				System.out.write(buffer, 0, readCount);
				System.out.println("--------------------------------------");
			}
		} catch (Exception e) {
		}
	}

	public static void testpubsub() throws Exception {

		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();
		jedis.publish("news.test1", "jhhjhh1");
		JhhPubSub jedisPubSub = new JhhPubSub();
		jedis.subscribe(jedisPubSub, "news.test2");
		jedis.disconnect();

	}

	@SuppressWarnings(value = { "rawtypes", "unchecked" })
	public static void teststats(String resultfile) throws Exception {

		BufferedWriter output;

		output = new BufferedWriter(new FileWriter(resultfile));

		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();
//		jedis.del("*");
		Set keys = jedis.keys("*");
		jedis.publish("news.test1", "jhhjhh1");
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
		for (int i = 22000; i < 25000; i++) {
			jedis.set("" + i, SP);
			if (i % 1000 == 999) {
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
