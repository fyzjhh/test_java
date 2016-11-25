package com.test.drill;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class Test {
	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String url = "jdbc:mysql://172.17.2.163:4533/test";
	static String user = "root";
	static String password = "";
	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";
	static String[] suffs = { "@vip", "@163", "@188", "@csdn", "@sina", "@corp" };
	static Object[] enums = { "A", "B", "C", "D", "E" };

	public static void main(String[] args) throws Exception {
		testselect();

		System.out.println("====success====");
	}

	private static void testselect() throws Exception {

		Class.forName("org.apache.drill.jdbc.Driver");
		String url = "jdbc:drill:zk=192.168.18.221:2281,192.168.18.221:2282,192.168.18.221:2283/drill/jhhdrill";

		Connection conn = DriverManager.getConnection(url);
		Statement stmt = conn.createStatement();
		String sql = "select *  from jhhmongodb.yun.`user` limit 10";
		ResultSet rs = stmt.executeQuery(sql);
		// ResultSetMetaData rsmd = rs.getMetaData(); // 获取字段名
		// if (rsmd != null) {
		// int count = rsmd.getColumnCount();
		// for (int i = 1; i <= count; i++) {
		// System.out.println("hyqTest======" + rsmd.getColumnName(i));
		// rsmd.getColumnType(i);
		// }
		// }

		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getString(2));
		}
	}

	class DBThread extends Thread {

		private int a, b;

		DBThread() {
		}

		@Override
		public void run() {
		}
	}
}
