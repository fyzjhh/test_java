package com.test.memcache;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.spy.memcached.MemcachedClient;

public class TestSpyMemcache {
	static String SP = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
	// static String PR = SP + SP + SP + SP + SP + SP + SP + SP + SP + SP ;
	static String PR = SP + SP + SP + SP + SP;
	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws Exception {

		testinsert();

	}
	

	public static void testselect() throws Exception {
		InetSocketAddress isa = new InetSocketAddress("172.17.2.163", 11231);
		MemcachedClient mc = new MemcachedClient(isa);

//		System.out.println(mc.getState());
//		Map<SocketAddress, Map<String, String>> stats = mc.getStats();
//		Set sets = stats.keySet();
//		for (Iterator iterator = sets.iterator(); iterator.hasNext();) {
//			System.out.println(stats.get(iterator.next()));
//		}
//
		Map<SocketAddress, Map<String, String>> stats = mc.getStats("items");

		Set sets = stats.keySet();
		for (Iterator iterator = sets.iterator(); iterator.hasNext();) {
			System.out.println(stats.get(iterator.next()));
		}

//		System.out.println("start:" + format.format(new Date()));
//		for (int i = 30000; i <= 50000; i++) {
//			mc.set(String.valueOf(i), 0, PR).get();
//			if (i % 1000 == 999) {
//				System.out.println(format.format(new Date())
//						+ " keys number were stored !!! : " + i);
//			}
//		}

//		System.out.println("end:" + format.format(new Date()));
		
		mc.shutdown();
	}
//
	@SuppressWarnings("rawtypes")
	public static void testinsert() throws Exception {
		InetSocketAddress isa = new InetSocketAddress("172.17.2.163", 11211);
		MemcachedClient mc = new MemcachedClient(isa);

		System.out.println(mc.getState());
		Map<SocketAddress, Map<String, String>> stats = mc.getStats();
		Set sets = stats.keySet();
		for (Iterator iterator = sets.iterator(); iterator.hasNext();) {
			System.out.println(stats.get(iterator.next()));
		}

		Map<SocketAddress, Map<String, String>> stats_items = mc.getStats("items");

		Set sets_items = stats_items.keySet();
		for (Iterator iterator = sets_items.iterator(); iterator.hasNext();) {
			System.out.println(stats_items.get(iterator.next()));
		}

		System.out.println("start:" + format.format(new Date()));
		for (int i = 30000; i <= 40000; i++) {
			mc.replace(String.valueOf(i), 0, PR);
			if (i % 1000 == 999) {
				System.out.println(format.format(new Date())
						+ " keys number were stored !!! : " + i+" "+mc.get(String.valueOf(i)));
			}
		}

		System.out.println("end:" + format.format(new Date()));
		mc.shutdown();
	}

}
