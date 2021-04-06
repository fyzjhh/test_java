package com.test.derby;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.derby.impl.store.raw.log.LogToFile;

public class TestDerby {

	static String url = "jdbc:mysql://172.17.2.163:4533/test";
	static String user = "root";
	static String password = "";
	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";
	static String[] suffs = { "@vip", "@163", "@188", "@csdn", "@sina", "@corp" };

	public static void main(String[] args) throws Exception {
		testselect();

		System.out.println("====success====");
	}

	private static void testudi() throws Exception {
		String driver = "org.apache.derby.jdbc.ClientDriver";
		String url = "jdbc:derby://localhost:1527/db1;user=sa;password=jhh";
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url);

		Statement stmt = conn.createStatement();
		String sqli = "insert into t values(11,'aa',10) ";
		String sqlu = "update t set name='jiang' where id=11 ";
		String sqld = "delete from t where id =11 ";

		stmt.execute(sqli);
		stmt.execute(sqlu);
		stmt.execute(sqld);

	}

	private static void testconn() throws Exception {
		String driver = "org.apache.derby.jdbc.ClientDriver";
		String url = "jdbc:derby://localhost:1527/db1;user=sa;password=jhh";

		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url);

		conn.close();
	}

	private static void testselect() throws Exception {
		String driver = "org.apache.derby.jdbc.ClientDriver";
		String url = "jdbc:derby://localhost:1527/db1;user=sa;password=jhh";

		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url);

//		Statement stmt = conn.createStatement();
//
//		String sql = "select * from t ";
//
//		ResultSet rs = stmt.executeQuery(sql);
//		while (rs.next()) {
//			System.out.println(rs.getInt(1) + "\t" + rs.getString(2));
//		}
//		rs.close();
//		stmt.close();

		PreparedStatement ps = conn
				.prepareStatement("select * from t where age=10");

		ResultSet prs = ps.executeQuery();
		while (prs.next()) {
			System.out.println(prs.getInt(1) + "\t" + prs.getString(2));
		}
		prs.close();
		ps.close();

		conn.close();
	}
}
