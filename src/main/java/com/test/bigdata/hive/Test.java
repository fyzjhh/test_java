package com.test.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;

public class Test {
	private static Configuration conf = null;

	static {
//		conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum", "192.168.12.210");
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
//		conf.set("hbase.master.port", "60000");
//		conf = HBaseConfiguration.create(conf);

	}

	public static void testjdbc() throws Exception {

		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");

		Connection con = DriverManager.getConnection(
				"jdbc:hive://192.168.12.21:10000/default", "", "");
		Statement stmt = con.createStatement();
		String tableName = "testHiveDriverTable";
		stmt.executeQuery("drop table " + tableName);
		ResultSet res = stmt.executeQuery("create table " + tableName
				+ " (key int, value string)");
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}
		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
		String filepath = "/tmp/a.txt";
		sql = "load data local inpath '" + filepath + "' into table "
				+ tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);

		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t"
					+ res.getString(2));
		}

		// regular hive query
		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}

	}

	public static void main(String[] args) throws Exception {
		// testjdbc();

		testthread2();

	}

	static DateFormat spacedatetimeformat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void testthread() throws Exception {
		ExecutorService exec = null;
		HashMap taskMap = null;

		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");

		exec = Executors.newFixedThreadPool(20);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < 20; l++) {
			final String sql = "select count(iuin) from tab_login where vclientip like '"
					+ l + "%' and par_datetime='201312'";
			Callable call = new Callable() {
				public String call() throws Exception {
					Connection con = DriverManager.getConnection(
							"jdbc:hive://192.168.12.210:10000/default", "", "");
					Statement stmt = con.createStatement();
					String r1 = spacedatetimeformat.format(new Date())
							+ Thread.currentThread() + "\t" + sql;
					System.out.println(r1);
					ResultSet rs = stmt.executeQuery(sql);

					while (rs.next()) {
						// int v1 = rs.getInt(1);
						// String v2 = rs.getString(2);
						// String v3 = rs.getString(3);
						// int v4 = rs.getInt(4);
						// String r = spacedatetimeformat.format(new Date())
						// + Thread.currentThread() + "\t" + v1 + "\t"
						// + v2 + "\t" + v3 + "\t" + v4;

						int v1 = rs.getInt(1);

						String r = spacedatetimeformat.format(new Date())
								+ Thread.currentThread() + "\t" + v1;
						System.out.println(r);
					}
					return "OK";
				}
			};
			Future task = exec.submit(call);
			taskMap.put("task-" + l, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			System.out.println(key + "\t" + ret);
		}

		exec.shutdown();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void testthread1() throws Exception {
		ExecutorService exec = null;
		HashMap taskMap = null;

		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");

		exec = Executors.newFixedThreadPool(20);
		taskMap = new HashMap<String, Future>();
		for (int l = 1; l < 11; l++) {
			final String sql = "select count(distinct iuin) from db1.tab_accountlog_addr where iuin%20="
					+ l;
			Callable call = new Callable() {
				public String call() throws Exception {
					Connection con = DriverManager.getConnection(
							"jdbc:hive://192.168.12.210:10000/db1", "", "");
					Statement stmt = con.createStatement();
					String r1 = spacedatetimeformat.format(new Date())
							+ Thread.currentThread() + "\t" + sql;
					System.out.println(r1);
					ResultSet rs = stmt.executeQuery(sql);

					while (rs.next()) {
						int v1 = rs.getInt(1);

						String r = spacedatetimeformat.format(new Date())
								+ Thread.currentThread() + "\t" + v1;
						System.out.println(r);
					}
					return "OK";
				}
			};
			Future task = exec.submit(call);
			taskMap.put("task-" + l, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			System.out.println(key + "\t" + ret);
		}

		exec.shutdown();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void testthread2() throws Exception {
		ExecutorService exec = null;
		HashMap taskMap = null;

		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");

		exec = Executors.newFixedThreadPool(1);
		taskMap = new HashMap<String, Future>();
		for (int l = 1; l < 2; l++) {
			final String sql = "select count(distinct iuin) from caochuan_2.tab_accountlog_addr where iuin%20="
					+ l;
			Callable call = new Callable() {
				public String call() throws Exception {
					Connection con = DriverManager.getConnection(
							"jdbc:hive://183.136.237.178:10000/caochuan_2", "",
							"");
					Statement stmt = con.createStatement();
					String r1 = spacedatetimeformat.format(new Date())
							+ Thread.currentThread() + "\t" + sql;
					System.out.println(r1);
					ResultSet rs = stmt.executeQuery(sql);

					while (rs.next()) {
						int v1 = rs.getInt(1);

						String r = spacedatetimeformat.format(new Date())
								+ Thread.currentThread() + "\t" + v1;
						System.out.println(r);
					}
					return "OK";
				}
			};
			Future task = exec.submit(call);
			taskMap.put("task-" + l, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			System.out.println(key + "\t" + ret);
		}

		exec.shutdown();
	}
}