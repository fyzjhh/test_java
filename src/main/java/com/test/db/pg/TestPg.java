package com.test.pg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.derby.impl.store.raw.log.LogToFile;

public class TestPg {

	static String url = "jdbc:mysql://172.17.2.163:4533/test";
	static String user = "root";
	static String password = "";
	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";
	static String[] suffs = { "@vip", "@163", "@188", "@csdn", "@sina", "@corp" };

	public static void main(String[] args) throws Exception {
		test();
		// testselect();

		System.out.println("====success====");
	}

	private static void test() throws Exception {
		String driver = "org.postgresql.Driver";
		String url = "jdbc:postgresql://172.17.3.32:18001/postgres";
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url, "u1", "123");

		conn.setAutoCommit(false);
		Statement stmt = conn.createStatement();
		// String sql1 =
		// "CREATE TABLE t1(id int, name varchar(20),age int ,primary key(id)) ";
		// stmt.execute(sql1);
		String sqli = null;

		for (int i = 3; i < 101; i++) {
			sqli = "insert into t1 values(" + i + ",'anv'," + (i % 20 + 5)
					+ ") ";
			stmt.execute(sqli);
			if (i % 100 == 99)
				conn.commit();
		}

		stmt.close();

		conn.close();
	}
}
