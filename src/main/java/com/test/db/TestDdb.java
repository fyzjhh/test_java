package com.test.db;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.derby.iapi.services.io.NewByteArrayInputStream;

import com.netease.backend.db.DBConnection;

public class TestDdb {

	public static void main(String[] args) throws Exception {

		testLoad();
	}

	private static void testInsert() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		DBConnection conn = (DBConnection) DriverManager.getConnection(
				"172.31.132.214:8886?key=src/secret.key", "jhhtest", "jhhtest");
		long id = conn.allocateRecordId("JHH_User");
		conn.setAutoCommit(false);
		Statement stmt = conn.createStatement();

		String name = null;
		int enabled = 0;
		for (int i = 1222; i < 1300; i++) {
			name = "nm_" + String.valueOf(i);
			enabled = i % 10;
			stmt.executeUpdate("replace into JHH_User values (" + (i + id)
					+ ",'" + name + "'," + enabled + "); ");
			if (i % 100 == 99) {
				System.out.println("the " + i / 100 + " reconds====");
			}
		}
		conn.close();
	}

	private static void testNosProcedure() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(
				"inspur1.photo.163.org:8881?key=src/secret.key", "nos_test",
				"nos_test");

		Statement cs = conn.createStatement();
		cs.execute("call DDB_test(2532555,'abc','6',12,'jjj')");
		ResultSet rs = cs.getResultSet();
		System.out.println(rs);
		//
		// try{
		// String sql = "{ call sp_select_role_priv ( ? ) } " ;
		//
		// dbm = DBConnectionPool.getInstance();
		// conn = dbm.getConnection(Constants.DB_earn) ;
		// cstmt = (SybCallableStatement) conn.prepareCall(sql) ;
		// java.sql.ResultSet rset = null ; //得到的结果集
		// java.sql.ResultSet rs = null ; //小结果集
		//
		// do{
		// rs = cstmt.getResultSet() ;
		// System.out.println("## resultSet:"+k);
		// while (rs.next()) {
		// if(k==1){
		// if(rs.getInt(1)<1){
		// continue;
		// }
		// }
		// if(k==2){
		// if(rs.getInt(1)<0){ //1:具有权限
		// continue;
		// }
		// }
		// }
		// rs.close() ;
		// k++;
		// }while (cstmt.getMoreResults());
		//
		// }catch (SQLException ex) {
		// ex.printStackTrace() ;
		// }
		// finally {
		// ........
		// }
	}

	private static void testSelect() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(
				"10.100.82.19:8888?key=src/secret.key", "fs_online",
				"fs_online");

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("select  org ,count(distinct(account))   from FS_User where ( id >=10 and id<20000 ) or ( id >=483100885653 and id<483100888653) group by org  having count(distinct(account))>100  limit 100");

		while (rs.next()) {
			System.out.println(rs.getString(1) + "\t" + rs.getInt(2));
		}
	}

	private static void testInsert12() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(
				"172.31.132.214:8888?key=src/secret.key", "test", "test");

		Statement stmt = conn.createStatement();

		String name = null;
		int enabled = 0;
		for (int i = 0; i < 1000000; i++) {
			name = "this " + String.valueOf(i) + " file";
			enabled = i % 10;
			stmt.executeUpdate("replace into JHH_USER values (" + i + ",'"
					+ name + "'," + enabled + "); ");
			if (i % 100 == 99) {
				System.out.println("the " + i / 100 + " reconds====");
			}
		}
		conn.close();
	}

	private static void testSelect12() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(
				"172.19.0.109:18888?key=src/secret.key", "light_blog_test",
				"light_blog_test");

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("select BlogID,count(distinct(PostID)) from RecommendTagPost group by BlogID having count(distinct(PostID)) >= 3");

		while (rs.next()) {
			System.out.println(rs.getInt(1) + "\t" + rs.getInt(2));
		}
	}

	private static void testLoad() throws Exception {
		String file = "D:/temp/aa.txt";
		String charset = "latin1";
		int columnCount = 3;
		byte attrQuoter = '\0';
		String attrDelimiter = "99999ZZZZZZZZZZ99999";
		String lineSeparator = "AAAAA0000000000AAAAA\n";
		JhhCsvByteParser input = new JhhCsvByteParser(file, charset,
				columnCount, attrQuoter, attrDelimiter.getBytes(), lineSeparator.getBytes());
		input.setSmart(false);
		int rows = 0, i = 0;
		int bucketCount = 4;
		OutputStream[] tempFileArray = new FileOutputStream[bucketCount];
		for (i = 0; i < bucketCount; i++)
			tempFileArray[i] = new FileOutputStream("D:/temp/aa" + i + ".txt");

		while (true) {
			try {
				byte[][] row = input.getNext();
				if (row == null)
					break;
				int bucketNo = 0;
				bucketNo = (rows % bucketCount);
				OutputStream tempFile = tempFileArray[bucketNo];
				for (i = 0; i < row.length; i++) {
					if (i > 0)
						tempFile.write(attrDelimiter.getBytes(charset));
					tempFile.write(row[i]);
				}
				tempFile.write(lineSeparator.getBytes(charset));
				rows++;

				if ((rows % 10) == 0)
					System.out.println("Splitted " + rows + " rows.");

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void test() throws Exception {
		// Class.forName("com.mysql.jdbc.Driver");
		// Connection myconn =
		// DriverManager.getConnection("jdbc:mysql://192.168.21.89:3306/ecp10?useUnicode=true&characterEncoding=utf8&connectTimeout=5000&logger=com.mysql.jdbc.log.NullLogger","yiliao","yiliao");
		// System.out.println(myconn);

		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager
				.getConnection("192.168.21.89?user=yiliao&password=yiliao&key=src/secret.key");

		PreparedStatement pst = null;

		pst = (PreparedStatement) conn
				.prepareStatement("select uid from uinfo limit 10");
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
		if (conn != null) {
			conn.close();
			conn = null;
		}

	}

	private static void testlofterTimelineUpgrade() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(
				"172.21.0.4:8881?key=src/secret.key", "blog_trace_test",
				"blog_trace_test");

		PreparedStatement pst = null;

		pst = (PreparedStatement) conn
				.prepareStatement("INSERT INTO GT_TimelineType0CreateTime_bak(UserId, PartId, Part, PartSize, Version) VALUES(?,?,?,?,?),(?,?,?,?,?)");

		byte[] by1 = new byte[] { 0x10, 0x20, 0x30 };
		byte[] by2 = new byte[] { 0x00, 0x7f, (byte) 0x80, (byte) 0xff };
		ByteArrayInputStream ba1 = new ByteArrayInputStream(by1);
		ByteArrayInputStream ba2 = new ByteArrayInputStream(by2);
		int id = 11111125;
		pst.setInt(1, id);
		pst.setInt(2, 104);
		// pst.setBytes(3, by1);
		// pst.setBinaryStream(3, ba1);
		pst.setAsciiStream(3, ba1);
		pst.setInt(4, 1000);
		pst.setInt(5, 1000);

		pst.setInt(6, id);
		pst.setInt(7, 105);
		pst.setAsciiStream(8, ba2);
		pst.setInt(9, 1001);
		pst.setInt(10, 1001);

		pst.executeUpdate();
		if (pst != null) {
			pst.close();
			pst = null;
		}
		if (conn != null) {
			conn.close();
			conn = null;
		}

	}

	private static void testInsert11() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		// Connection conn = DriverManager.getConnection(
		// " 123.58.180.85:8892?key=ddbpath/conf/secret.key", "notemiror",
		// "notemiror");
		Connection conn = DriverManager.getConnection(
				"172.17.2.163:8881?key=src/secret.key", "test", "test");

		Statement stmt = conn.createStatement();

		for (int i = 1; i <= 10000; i++) {

			stmt.executeUpdate("replace into Test_TBL1 values (" + i + "," + i
					* i + "); ");
			if (i % 100 == 0) {
				System.out.println("the " + i / 100 + " reconds====");
			}
		}
		conn.close();
	}

	private static void testSelect11() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(
				"172.17.2.163:8881?key=src/secret.key", "test", "test");

		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery("select * from Test_TBL1 limit 100");

		while (rs.next()) {
			System.out.println(rs.getLong(1) + "\t" + rs.getLong(2));
		}
	}
}
