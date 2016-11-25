package com.test.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestMysqlPerf {
	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String url = "jdbc:mysql://172.17.2.163:4533/test";
	static String user = "root";
	static String password = "";
	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";
	static String[] suffs = { "@vip", "@163", "@188", "@csdn", "@sina", "@corp" };
	static Object[] enums = { "A", "B", "C", "D", "E" };

	public static void main(String[] args) throws Exception {
		testmultithreads();

		System.out.println("====success====");
	}

	/*
	 * 开启N个线程,每个线程做M次,执行同一个SQL
	 * 
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void testmultithreads() throws Exception {

		int client_count = 5;
		int exec_count = 20;
		String url = "jdbc:mysql://10.199.134.41:3306/xqy-portal?user=root&password=Servy0u123";
		String sql = "select count(1) from sys_businesslog where id=<id>";
		ExecutorService exec = Executors.newFixedThreadPool(client_count);
		HashMap taskMap = new HashMap<String, Future>(client_count);
		for (int i = 0; i < client_count; i++) {
			DBThread taski = new DBThread(i, url, sql, exec_count);
			Future futurei = exec.submit(taski);
			taskMap.put(i, futurei);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Object key = entry.getKey();
			Future val = (Future) entry.getValue();
			Object ret = (Object) val.get();
			System.out.println(key + "result is " + ret);
		}

		exec.shutdown();

	}
	//
	// public static long getrandom(long small, long large) throws Exception {
	//
	// return small + (long) ((large - small) * Math.random());
	//
	// }

	public static int getrandom(int small, int large) throws Exception {

		return small + (int) ((large - small) * Math.random());

	}

	private static String getRndStr(int len) {

		Random rndstr = new Random();
		StringBuffer sb = new StringBuffer();
		for (int j = 0; j < len; j++) {
			int number = rndstr.nextInt(base.length());
			sb.append(base.charAt(number));
		}
		return sb.toString();
	}

	private static Object getRndEnum() throws Exception {

		int len = enums.length;
		int rndInd = getrandom(0, len - 1);
		return enums[rndInd];
	}

}

class DBThread implements Callable {
	private int index;
	private String url;
	private String sql;
	private int exec_count;

	public DBThread(int index, String url, String sql, int exec_count) {
		this.index = index;
		this.url = url;
		this.sql = sql;
		this.exec_count = exec_count;
	}

	@Override
	public Object call() throws Exception {
		try {

			Long startTime = System.currentTimeMillis();
			Connection conn = DriverManager.getConnection(url);

			// PreparedStatement pst = (PreparedStatement)
			// conn.prepareStatement(sql);

			for (int i = 0; i < exec_count; i++) {

				// ResultSet rs = pst.executeQuery();

				Statement st = conn.createStatement();
				int rand = TestMysqlPerf.getrandom(35747, 46988);
				String exec_sql = sql.replaceAll("<id>", String.valueOf(rand));
				ResultSet rs = st.executeQuery(exec_sql);

				if (rs.next()) {
					String s = index + " " + i + " " + rs.getString(1);
					System.out.println(s + " " + exec_sql);
				}
				rs.close();
				st.close();
			}

			// if (pst != null) {
			// pst.close();
			// pst = null;
			// }

			if (conn != null) {
				conn.close();
				conn = null;
			}
			Long endTime = System.currentTimeMillis();
			System.out.println(index + " spent time " + (endTime - startTime));
			return 0;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 1;
		}
	}
}