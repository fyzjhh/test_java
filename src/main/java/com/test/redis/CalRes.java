package com.test.redis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import redis.clients.jedis.Jedis;

import com.netease.backend.db.result.Comparator;

public class CalRes extends Thread {
	class SampleComparator extends Comparator {

		public int compare(Object o1, Object o2) {
			if (o1 instanceof String) {
				int i1 = Integer.parseInt((String) o1);
				int i2 = Integer.parseInt((String) o2);
				return (i1 >= i2) ? 1 : 0;
			}
			return 0;
		}
	}

	public static void main(String[] args) {
		Jedis jedis = new Jedis("172.17.2.163", 6379);
		jedis.connect();
		List<String> configSets = jedis.configGet("maxmemory*");
		System.out.println(configSets);
		String infoStr = jedis.info();

		String[] res = infoStr.split("\r\n");
		for (int i = 0; i < res.length; i++) {
			if (res[i].contains("key")) {
				System.out.println(res[i]);
			}
		}

		jedis.disconnect();
		// CalRes cr = new CalRes("E:\\temp\\redis.log");
		// cr.run();
	}

	private String fn;

	public CalRes(String filename) {
		fn = filename;
	}

	@SuppressWarnings(value = { "rawtypes", "unchecked" })
	public void run() {

		BufferedWriter output;

		try {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			output = new BufferedWriter(new FileWriter(fn));
			Date startDate = new Date();
			output.write("================start:" + format.format(startDate)
					+ "\r\n");

			Jedis jedisconfig = new Jedis("172.17.2.163", 6379);
			jedisconfig.connect();
			List<String> configSets = jedisconfig.configGet("maxmemory*");
			output.write("====config:" + configSets + "\r\n");
			jedisconfig.disconnect();

			int tmp_cnt = -1;
			for (int i = 0; i < 100; i++) {
				Jedis jedis = new Jedis("172.17.2.163", 6379);
				try {
					jedis.connect();

					Set keys = jedis.keys("*");
					if (tmp_cnt != keys.size() && keys.size() >= 3000) {
						Date tempDate = new Date();
						long timeoffset = (tempDate.getTime() - startDate
								.getTime()) / 1000;
						String datetime = format.format(tempDate);
						String[] infoStrs = jedis.info().split("\r\n");
						output.write(i + " ======== time:" + datetime
								+ ",timeoffset:" + timeoffset + ",key count:"
								+ keys.size() + ",info:" + "\r\n");
						for (int m = 0; m < infoStrs.length; m++) {
							if (infoStrs[m].contains("key")) {
								output.write(infoStrs[m] + "\r\n");
							}
						}
						List keyList = new ArrayList(keys);
						Collections.sort(keyList, new MyCom());
						output.write(keyList + "\r\n");

					}

					tmp_cnt = keys.size();
					jedis.disconnect();
				} catch (Exception e) {
					e.printStackTrace();
				}
				Thread.sleep(800);
			}
			output.write("================end:" + format.format(new Date())
					+ "\r\n");
			output.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

// Iterator t1 = keyList.iterator();
// while (t1.hasNext()) {
// Object obj1 = t1.next();
// output.write(obj1 + "\r\n");
// // + jedis.get(obj1.toString())
// }