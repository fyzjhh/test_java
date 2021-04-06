package com.test.mysql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestMysql {
	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String url = "jdbc:mysql://172.17.2.163:4533/test";
	static String user = "root";
	static String password = "";
	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";
	static String[] suffs = { "@vip", "@163", "@188", "@csdn", "@sina", "@corp" };
	static Object[] enums = { "A", "B", "C", "D", "E" };

	public static void main(String[] args) throws Exception {
		test_online_schema_change();
		// for (int i = 0; i < 10; i++) {
		//
		// }
		// testdata("db1", "tab1_3");
		// testdata("db1", "tab1_4");
		System.out.println("====success====");
	}

	private static void testbatch() throws Exception {

		Class.forName("com.mysql.jdbc.Driver"); // ����mysq��

		String url = "jdbc:mysql://172.17.2.163:3311/test";
		String user = "test";
		String password = "test";
		Connection conn = DriverManager.getConnection(url, user, password);

		Long startTime = System.currentTimeMillis();

		for (int i = 40; i < 90; i++) {
			PreparedStatement pst = (PreparedStatement) conn
					.prepareStatement("drop table tab_" + i + " ;");
			pst.execute();
			// ResultSet rs = pst.executeQuery();
			// if (rs.next()) {
			// System.out.println("tab_" + i + "  count " + rs.getInt(1));
			// }
		}

		// for (int i = 2; i < 100; i++) {
		// PreparedStatement pst = (PreparedStatement) conn
		// .prepareStatement("create table tab_" + i + " like tab_1 ;");
		// pst.execute();
		//
		// }

		Long endTime = System.currentTimeMillis();
		System.out.println("spent time :"
				+ dtft.format(new Date(endTime - startTime)));

		if (conn != null) {
			conn.close();
			conn = null;
		}

	}

	private static void testdata(String database, String table) {
		Connection conn;
		Statement pstmt;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager
					.getConnection("jdbc:mysql://192.168.12.142:3306?user=root&password=ttxsdb@10");

			Long startTime = System.currentTimeMillis();
			String crtdb = "create database IF NOT EXISTS " + database;
			pstmt = conn.createStatement();
			pstmt.execute(crtdb);
			pstmt.close();

			String usedb = "use " + database;
			pstmt = conn.createStatement();
			pstmt.execute(usedb);
			pstmt.close();

			String crttbl = "  create table IF NOT EXISTS " + table
					+ "(id int primary key, name varchar(20)) engine = innodb ";
			pstmt = conn.createStatement();
			pstmt.execute(crttbl);

			pstmt.close();

			PreparedStatement ps = conn.prepareStatement("replace into "
					+ table + " values(?,?);");

			for (int i = 0; i < 10000; i++) {
				if (i % 500 == 499) {
					System.out.println(i + "====");
				}
				ps.setInt(1, i);
				ps.setString(2, "xxx" + i);
				ps.execute();
			}
			ps.close();

			Long endTime = System.currentTimeMillis();
			Date spenttime = new Date(endTime - startTime);
			System.out.println("spent time :" + format.format(spenttime));
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void tt() {
		Connection conn;
		PreparedStatement pstmt;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager
					.getConnection("jdbc:mysql://172.17.3.32:6331/test?useCursorFetch=true&user=root");
			pstmt = conn
					.prepareStatement(
							"update tt set core_count='4',position='xxx',uuid='22' where id=8;",
							ResultSet.TYPE_FORWARD_ONLY,
							ResultSet.CONCUR_READ_ONLY);
			pstmt.execute();
			pstmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void testInsert() throws Exception {

		char charA = '\u0041';
		System.out.println(charA);
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager
				.getConnection(
						"jdbc:mysql://localhost:4311/test?useUnicode=true&characterEncoding=utf8",
						"root", "");

		Statement stmt = conn.createStatement();
		byte b1 = (byte) 0xF1;
		byte b2 = (byte) 0x9F;
		byte b3 = (byte) 0x90;
		byte b4 = (byte) 0x84;
		byte[] bs = new byte[] { b1, b2, b3, b4 };
		String st1 = new String(bs, "UTF-8");
		stmt.executeUpdate("insert into t values (" + 11 + ",'" + st1 + "'); ");

		conn.close();
	}

	private static void test_online_schema_change() throws Exception {

		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager
				.getConnection(
						"jdbc:mysql://192.168.12.210:3306/test?useUnicode=true&characterEncoding=utf8",
						"root", "123");

		Long startTime = System.currentTimeMillis();

		Statement stmt = null;
		PreparedStatement ps = null;

		String usedb = "use test";
		stmt = conn.createStatement();
		stmt.execute(usedb);

		String table = "t1";

		// String crttbl = "  create table IF NOT EXISTS "
		// + table
		// +
		// " (id int primary key auto_increment, name varchar(64)) engine = innodb ";
		// stmt = conn.createStatement();
		// stmt.execute(crttbl);
		//
		// ps = conn.prepareStatement("replace into " + table +
		// " values(?,?);");
		//
		// for (int i = 0; i < 1000; i++) {
		// if (i % 1000 == 999) {
		// System.out.println(i + "====");
		// }
		// ps.setInt(1, i);
		// ps.setString(2, "xxx_" + i);
		// ps.execute();
		// }
		// ps.close();

		int insert_offset = 11000000;
		int update_offset = 1210;
		int delete_offset = 1310;
		String t = null;
		for (int i = 0; i < 10; i++) {
			t = "replace into " + table + " values(" + (i + insert_offset)
					+ "," + "'xxx_" + (i + insert_offset) + "');";
			System.out.println(t);
			ps = conn.prepareStatement("replace into " + table
					+ " values(?,?);");
			ps.setInt(1, i + insert_offset);
			ps.setString(2, "xxx_" + (i + insert_offset));
			ps.execute();

			t = " update " + table + " set name ='yy' where id="
					+ (i + update_offset);
			System.out.println(t);
			stmt.execute(t);

			t = " delete from  " + table + " where id=" + (i + delete_offset);
			System.out.println(t);
			stmt.execute(t);

			Thread.currentThread().sleep(1000);
		}

		stmt.close();
		ps.close();
		Long endTime = System.currentTimeMillis();
		Date spenttime = new Date(endTime - startTime);
		System.out.println("spent time :" + format.format(spenttime));

		conn.close();
	}

	public static List<String> readfile(String sn) throws Exception {
		StringBuilder ressb = new StringBuilder();
		List<String> proclist = new ArrayList<String>();

		File file = new File(sn);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;

			while (true) {

				tempString = reader.readLine();
				if (tempString == null)
					break;
				if (tempString.trim().matches(" *END *")) {
					ressb.append(tempString);
					proclist.add(ressb.toString());

					ressb.delete(0, ressb.length());

				} else {
					ressb.append(tempString).append("\r\n");
				}

			}
			reader.close();

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
		return proclist;
	}

	private static void testcrtprocedure() throws Exception {
		List<String> proclist = readfile("E:\\sftpdir\\procedure.txt");

		Class.forName("com.mysql.jdbc.Driver"); // ����mysq��
		Connection conn = DriverManager.getConnection(url, user, password);

		Long startTime = System.currentTimeMillis();

		PreparedStatement pst = null;

		for (String myps : proclist) {
			try {
				pst = (PreparedStatement) conn.prepareStatement(myps);
				pst.execute();
			} catch (SQLException e) {
				System.out.println("��ݲ�������");
				e.printStackTrace();
			}
		}

		Long endTime = System.currentTimeMillis();
		System.out.println("spent time :"
				+ dtft.format(new Date(endTime - startTime)));

		if (pst != null) {
			pst.close();
			pst = null;
		}
		if (conn != null) {
			conn.close();
			conn = null;
		}

	}

	private static void test() throws Exception {

		Class.forName("com.mysql.jdbc.Driver"); // ����mysq��

		String url = "jdbc:mysql://192.168.12.210/db1";
		String user = "tungsten";
		String password = "tungsten";
		Connection conn = DriverManager.getConnection(url, user, password);

		Long startTime = System.currentTimeMillis();

		for (int i = 0; i < 50000; i++) {
			PreparedStatement pst = (PreparedStatement) conn
					.prepareStatement("insert into t1 (name) values(?);");

			pst.setString(1, "name_" + i);
			pst.execute();

			// System.out.println("sleep 5 second ");
			// Thread.currentThread().sleep(5000);
		}

		Long endTime = System.currentTimeMillis();
		System.out.println("spent time :"
				+ dtft.format(new Date(endTime - startTime)));

		if (conn != null) {
			conn.close();
			conn = null;
		}

	}

	private static void testmultithreads() throws Exception {

		ExecutorService es = Executors.newFixedThreadPool(20);
		// for (int i = 0; i < 80; i++) {
		// DBThread taski = new DBThread(i);
		// Future futurei = es.submit(taski);
		// System.out.println("task  " + i + ":" + futurei.get().toString());
		// }
		es.shutdownNow();

	}

	private static void testlocktimeout() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String url = null;
		String user = null;
		String password = null;
		Class.forName("com.mysql.jdbc.Driver");

		url = "jdbc:mysql://172.17.2.163:4533/test";
		user = "test";
		password = "test";
		conn = DriverManager.getConnection(url, user, password);

		Long startTime = System.currentTimeMillis();
		conn.setAutoCommit(false);

		stmt = conn.createStatement();
		for (int j = 100; j < 110; j++) {
			rs = stmt.executeQuery("select * from tt where id =" + j
					+ " limit 1 for update ;");
			if (rs.next()) {
				int id = rs.getInt("id");
				String name = rs.getString("name") + "_extra";
				stmt.executeUpdate(" update tt set name ='" + name
						+ "' where id=" + id);
			}
		}

		conn.commit();
		Long endTime = System.currentTimeMillis();
		System.out.println("spent time :"
				+ dtft.format(new Date(endTime - startTime)));

		if (conn != null) {
			conn.close();
			conn = null;
		}

	}

	private static void testbatchinsert() throws Exception {
		Connection conn = null;
		String url = null;
		String user = null;
		String password = null;
		Class.forName("com.mysql.jdbc.Driver");

		url = "jdbc:mysql://172.17.2.163:3311/test";
		user = "test";
		password = "test";
		conn = DriverManager.getConnection(url, user, password);

		conn.setAutoCommit(false);

		PreparedStatement jhhhdomain = (PreparedStatement) conn
				.prepareStatement("replace into JHH_Domain(domainid,domainname,descstr,createtime) values (?,?,?,?);");

		PreparedStatement jhhuser = (PreparedStatement) conn
				.prepareStatement("replace into JHH_User(username,usersuffix,usermd5,domainid,descstr,createtime) values (?,?,?,?,?,?);");

		PreparedStatement jhhfile = (PreparedStatement) conn
				.prepareStatement("replace into JHH_File(fileid,filemd5,username,usersuffix,descstr,uploadtime,expirytime) values (?,?,?,?,?,?,?);");

		Long startTime = System.currentTimeMillis();

		Random random = new Random();

		for (int i = 26; i <= 30; i++) {
			int rnddomains = Math.abs(random.nextInt());

			String d_domainname = getRndStr(rnddomains % 4 + 2);
			String d_descstr = getRndStr(rnddomains % 10 + 6);
			long d_createtime = getrandom(946656000000L, 1420041600000L);

			jhhhdomain.setInt(1, 0);
			jhhhdomain.setString(2, d_domainname);
			jhhhdomain.setString(3, d_descstr);
			jhhhdomain.setString(4, dtft.format(new Date(d_createtime)));

			jhhhdomain.executeUpdate();

			int domainusers = (rnddomains % 2000) + 1000;
			for (int j = 1; j <= domainusers; j++) {
				int rndusers = Math.abs(random.nextInt());

				String u_username = getRndStr(rndusers % 5 + 4);
				String u_usersuffix = suffs[rndusers % 6];
				long u_usermd5 = Math.abs(random.nextLong());
				int u_domainid = i;
				String u_descstr = getRndStr(rndusers % 10 + 6);
				long u_createtime = d_createtime + rndusers % 4 + 2;

				jhhuser.setString(1, u_username);
				jhhuser.setString(2, u_usersuffix);
				jhhuser.setLong(3, u_usermd5);
				jhhuser.setInt(4, u_domainid);
				jhhuser.setString(5, u_descstr);
				jhhuser.setString(6, dtft.format(new Date(u_createtime)));

				jhhuser.executeUpdate();

				int userfils = (rndusers % 100) + 50;
				for (int k = 1; k <= userfils; k++) {

					int rndfiles = Math.abs(random.nextInt());

					long f_filemd5 = Math.abs(random.nextLong());
					String f_username = u_username;
					String f_usersuffix = u_usersuffix;
					String f_descstr = getRndStr(rndfiles % 10 + 6);
					long f_uploadtime = u_createtime + rndfiles % 3600 + 3600;
					long f_expirytime = f_uploadtime + rndfiles % 3600;

					jhhfile.setLong(1, 0L);
					jhhfile.setLong(2, f_filemd5);
					jhhfile.setString(3, f_username);
					jhhfile.setString(4, f_usersuffix);
					jhhfile.setString(5, f_descstr);
					jhhfile.setString(6, dtft.format(new Date(f_uploadtime)));
					jhhfile.setString(7, dtft.format(new Date(f_expirytime)));

					jhhfile.executeUpdate();
					if (k % 500 == 0) {
						System.out.println(dtft.format(new Date()) + " file : "
								+ k);
					}
				}

				if (j % 100 == 0) {
					System.out
							.println(dtft.format(new Date()) + " user : " + j);
				}
				conn.commit();
			}

			System.out.println(dtft.format(new Date()) + " domain : " + i);

		}

		// pst.setInt(1,512);
		// pst.setLong(2, 5000000L);
		// pst.setLong(3, 5000000L);
		// pst.setString(4, "jianghehui");
		// pst.setString(5, "this is a good boy");
		// pst.setString(6, "2011-12-01");
		// pst.setString(7, "2011-12-11");
		// pst.addBatch();
		// pst.executeBatch();
		// conn.commit();
		Long endTime = System.currentTimeMillis();
		System.out.println("spent time :"
				+ dtft.format(new Date(endTime - startTime)));

		if (jhhhdomain != null) {
			jhhhdomain.close();
			jhhhdomain = null;
		}
		if (jhhuser != null) {
			jhhuser.close();
			jhhuser = null;
		}
		if (jhhfile != null) {
			jhhfile.close();
			jhhfile = null;
		}
		if (conn != null) {
			conn.close();
			conn = null;
		}

	}

	private static void testInsertFile() throws Exception {
		Connection conn = null;
		String url = null;
		String user = null;
		String password = null;
		Class.forName("com.mysql.jdbc.Driver");

		url = "jdbc:mysql://172.17.2.163:3311/test";
		user = "test";
		password = "test";
		conn = DriverManager.getConnection(url, user, password);

		conn.setAutoCommit(false);

		PreparedStatement jhhfile = (PreparedStatement) conn
				.prepareStatement("replace into JHH_File(fileid,filemd5,username,usersuffix,descstr,uploadtime,expirytime) values (?,?,?,?,?,?,?);");

		Long startTime = System.currentTimeMillis();

		Random random = new Random();

		int rndusers = 500;
		for (int k = 1; k <= rndusers; k++) {

			int rndfiles = Math.abs(random.nextInt());

			long f_filemd5 = Math.abs(random.nextLong());
			String f_username = getRndStr(rndusers % 5 + 4);
			String f_usersuffix = suffs[rndusers % 6];
			String f_descstr = getRndStr(rndfiles % 10 + 6);
			long f_uploadtime = getrandom(946656000000L, 1420041600000L)
					+ rndfiles % 3600 + 3600;
			long f_expirytime = getrandom(946656000000L, 1420041600000L)
					+ rndfiles % 3600;

			jhhfile.setLong(1, 0L);
			jhhfile.setLong(2, f_filemd5);
			jhhfile.setString(3, f_username);
			jhhfile.setString(4, f_usersuffix);
			jhhfile.setString(5, f_descstr);
			jhhfile.setString(6, dtft.format(new Date(f_uploadtime)));
			jhhfile.setString(7, dtft.format(new Date(f_expirytime)));

			jhhfile.executeUpdate();
			if (k % 500 == 0) {
				System.out.println(dtft.format(new Date()) + " file : " + k);
			}
		}

		conn.commit();

		Long endTime = System.currentTimeMillis();
		System.out.println("spent time :"
				+ dtft.format(new Date(endTime - startTime)));

		if (jhhfile != null) {
			jhhfile.close();
			jhhfile = null;
		}
		if (conn != null) {
			conn.close();
			conn = null;
		}

	}

	private static long getrandom(long small, long large) throws Exception {

		return small + (long) ((large - small) * Math.random());

	}

	private static int getrandom(int small, int large) throws Exception {

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

	@SuppressWarnings("unchecked")
	private static void testconn() throws Exception {

		List<Connection> connlist = new ArrayList<Connection>();

		String url = null;
		String user = null;
		String password = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("����ش���");
			e.printStackTrace();
		}

		try {
			url = "jdbc:mysql://172.17.2.163:4533/test";
			user = "test";
			password = "test";
			Long startTime = System.currentTimeMillis();
			for (int i = 0; i < 220; i++) {

				Connection conn = DriverManager.getConnection(url, user,
						password);
				if (conn != null) {
					connlist.add(conn);
				}
				if (i % 50 == 49) {
					Long endTime = System.currentTimeMillis();
					System.out.println(dtft
							.format(new Date(endTime - startTime))
							+ " add  : "
							+ i);
				}
			}

		} catch (SQLException e) {
			System.out.println("��ݿ����Ӵ���");
			e.printStackTrace();
		}

		try {
			for (Object object : connlist) {
				Connection conn = (Connection) object;
				if (conn != null)
					conn.close();
			}
		} catch (Exception e) {
			System.out.println("��ݿ�رմ���");
			e.printStackTrace();
		}

	}

	private static void testselect() throws Exception {

		Class.forName("com.mysql.jdbc.Driver");
		String url = "jdbc:mysql://172.17.2.201:4332/test";

		Connection conn = DriverManager.getConnection(url, "nos_test",
				"nos_test");
		Statement stmt = conn.createStatement();
		String sql = "select * from BlogSettings ";
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData  rsmd = rs.getMetaData(); // 获取字段名  
		if (rsmd != null) {  
		    int count = rsmd.getColumnCount();  
		    for (int i = 1; i <= count; i++) {  
		        System.out.println("hyqTest======" + rsmd.getColumnName(i));  
		        rsmd.getColumnType(i);  
		    }
		}  
		    
		while (rs.next()) {
			System.out.println(rs.getInt(2));

		}
	}

	// -------ʹ�ÿɹ����Ľ��ɾ��һ�У�
	// pstmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,
	// ResultSet.CONCUR_UPDATABLE);
	// // ��ѯ
	// rs = pstmt.executeQuery();
	// // �ƶ��α굽Ҫɾ�����
	// rs.absolute(20);
	//
	// // ִ��ɾ��
	// rs.deleteRow();

	// ------ȡ����£�
	// // �ƶ��α굽������
	// rs.absolute(5);
	// // ���ø���ֵ
	// rs.updateString("name", "k187");
	// rs.updateInt(3, 2);
	// rs.updateString(4, "Ů");
	// rs.updateDate(5, new java.sql.Date(new java.util.Date().getTime())); //
	// ���ڱ�����sql֧�ֵ�
	// // ȡ�����
	// rs.cancelRowUpdates(); // ֻ���ڸ���ǰ�汻����ʱ�ſ���ȡ��
	// // ִ�и���
	// rs.updateRow();
	//
	//
	// print(rs); // ��ӡ���µ���

	// ---------ʹ�ÿɹ���������һ�У�
	// rs = pstmt.executeQuery();
	// // �ƶ��α굽������
	// rs.absolute(5);
	// // ���ø���ֵ
	// rs.updateString("name", "k187");
	// rs.updateInt(3, 2);
	// rs.updateString(4, "Ů");
	// rs.updateDate(5, new java.sql.Date(new java.util.Date().getTime())); //
	// ���ڱ�����sql֧�ֵ�
	// // ִ�и���
	// rs.updateRow();
	//
	// print(rs); // ��ӡ���µ���

	// ----------ʹ�ÿɹ����Ľ�����һ�У�
	// rs = pstmt.executeQuery();
	// // �ƶ��α굽������
	// rs.moveToInsertRow();
	// // ���ò���ֵ
	// rs.updateString(2, "k187");
	// rs.updateInt(3, 2);
	// rs.updateString(4, "Ů");
	// rs.updateDate(5, new java.sql.Date(new java.util.Date().getTime())); //
	// ���ڱ�����sql֧�ֵ�
	// // ִ�в���
	// rs.insertRow();
	private static void testcursor() throws Exception {

		Class.forName("com.mysql.jdbc.Driver");
		String url = "jdbc:mysql://172.17.2.201:4332/test";
		String sql = "select * from BlogSettings where BlogID<10";
		String sql1 = "select * from BlogSettings where BlogID>20000 ";
		Connection conn = DriverManager.getConnection(url, "nos_test",
				"nos_test");

		PreparedStatement ps = conn.prepareStatement(sql,
				ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

		ResultSet rs = ps.executeQuery();
		// rs.setFetchSize(100);
		rs.next();
		Blob blob = rs.getBlob(3);
		InputStream imgIs = blob.getBinaryStream();
		InputStreamReader imgIsr = new InputStreamReader(imgIs);
		BufferedReader buff_imgIsr = new BufferedReader(imgIsr);
		String line = null;
		while (null != (line = buff_imgIsr.readLine())) {
			System.out.println(line); // �����������Ļ��ʵ������԰�����Ҫ����
		}

		PreparedStatement ps1 = conn.prepareStatement(sql1,
				ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

		ResultSet rs1 = ps1.executeQuery();
		// rs1.setFetchSize(100);
		rs1.next();
		Blob blob1 = rs.getBlob(3);
		InputStream imgIs1 = blob1.getBinaryStream();
		InputStreamReader imgIsr1 = new InputStreamReader(imgIs1);
		BufferedReader buff_imgIsr1 = new BufferedReader(imgIsr1);
		String line1 = null;
		while (null != (line1 = buff_imgIsr1.readLine())) {
			System.out.println(line1); // �����������Ļ��ʵ������԰�����Ҫ����
		}
		// rs.close();
		//
		// while (rs.next()) {
		// System.out.println(rs.getInt(2));
		// }
		// ps.close();
		// conn.close();
	}

	private static void testnormal() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		String sql = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("����ش���");
			e.printStackTrace();
		}
		try {
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			System.out.println("��ݿ����Ӵ���");
			e.printStackTrace();
		}

		List<String> proclist = new ArrayList<String>();
		try {
			stmt = conn.createStatement();
			sql = "show procedure status ";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println(rs.getString(1) + "\t" + rs.getString(2)
						+ "\t" + rs.getString(3) + "\t" + rs.getString(4));
				proclist.add(rs.getString(2));
			}

		} catch (SQLException e) {
			System.out.println("��ݲ�������");
			e.printStackTrace();
		}

		Collections.sort(proclist);
		for (String string : proclist) {
			try {
				sql = "show create procedure  " + string;
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					System.out
							.println(rs.getString(1) + "\t" + rs.getString(3));
				}

			} catch (SQLException e) {
				System.out.println("��ݲ�������");
				e.printStackTrace();
			}
		}

		// List<String> tablelist = new ArrayList<String>();
		// try {
		// stmt = conn.createStatement();
		// sql = "show tables";
		// rs = stmt.executeQuery(sql);
		// while (rs.next()) {
		// tablelist.add(rs.getString(1));
		// }
		//
		// } catch (SQLException e) {
		// System.out.println("��ݲ�������");
		// e.printStackTrace();
		// }

		// Collections.sort(tablelist);
		// for (String string : tablelist) {
		// System.out.println(string);
		// }

		try {
			if (rs != null) {
				rs.close();
				rs = null;
			}
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
			if (conn != null) {
				conn.close();
				conn = null;
			}
		} catch (Exception e) {
			System.out.println("��ݿ�رմ���");
			e.printStackTrace();
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
