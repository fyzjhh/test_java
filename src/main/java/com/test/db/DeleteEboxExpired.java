package com.test.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DeleteEboxExpired {

	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String url = "10.100.82.77:8892?key=src/secret.key";
//	static String url = "10.100.82.19:8888?key=E:/ddb/ddb4.3/conf/secret.key";
	static String user = "ebox_test";
	static String pass = "ebox_test";

	public static void main(String[] args) throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(url, user, pass);
		conn.close();
//		cntErrorDocid();
	}

	private static void delexp(String[] args) throws Exception {
		if (args.length == 4) {
			url = args[0];
			user = args[1];
			pass = args[2];
			System.out.println(df.format(new Date()) + "; start clean");
			doDelete(args[3]);
			System.out.println(df.format(new Date()) + "; finished clean");
		} else {
			System.out
					.println("usage : DeleteEboxExpired url user pass md5file(photoid ,md5high, count , expiredtime)");
		}
	}

	private static void cntErrorDocid() throws Exception {
		Long startTime = System.currentTimeMillis();

		getErrorDocid();

		Long endTime = System.currentTimeMillis();
		System.out.println("ÓÃÊ±£º" + df.format(new Date(endTime - startTime)));

	}

	public static void getErrorDocid() throws Exception {
		BufferedWriter output = new BufferedWriter(new FileWriter(
				"E:/temp/errordocid.txt"));

		int allcnt = 0;// counter
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(url, user, pass);

		PreparedStatement fpps = conn
				.prepareStatement("select count(id) from FS_FilePersistent where docId=? and md5High=? ;");

		Statement md5ps = conn.createStatement();
		ResultSet md5rs = md5ps
				.executeQuery("select photoid,md5high from TMPA_PhotoMD5 limit 10000;");

		while (md5rs.next()) {

			long vphotoid = md5rs.getLong(1);
			long vmd5high = md5rs.getLong(2);

			fpps.setLong(1, vphotoid);
			fpps.setLong(2, vmd5high);
			ResultSet fprs = fpps.executeQuery();

			if (fprs.next()) {
				int res = fprs.getInt(1);
				if (res > 0) {
					allcnt++;
					System.out.println(df.format(new Date()) + "; id :"
							+ vphotoid + ", md5high:" + vmd5high + ",cnt :"
							+ res + " is not null in filepersistent  .");
					output.write(df.format(new Date()) + "; id :" + vphotoid
							+ ", md5high:" + vmd5high + ",cnt :" + res
							+ " is not null in filepersistent  ." + "\r\n");
				}
			}

		}
		md5rs.close();
		output.close();
		System.out.println(df.format(new Date()) + "; all count:" + allcnt);

		conn.close();
	}

	public static void doDelete(String md5file) throws Exception {

		int allcnt = 0;// counter
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(url, user, pass);

		conn.setAutoCommit(false);
		PreparedStatement md5ps = conn
				.prepareStatement("delete from PhotoMD5 where photoid=? and md5High=? and count=0 ;");
		PreparedStatement recyclebinps = conn
				.prepareStatement("insert ignore into DFS_RecycleBin(DocID,DeleteTime) values (?,?);");

		File file = null;
		BufferedReader reader = null;
		try {
			file = new File(md5file);
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				String[] arrs = tempString.split("\t");
				if (arrs.length == 4) { // photoid ,md5high, count , expiredtime

					long vphotoid = Long.valueOf(arrs[0]);
					long vmd5high = Long.valueOf(arrs[1]);
					System.out.println(df.format(new Date()) + "; id :"
							+ vphotoid + ", md5high:" + vmd5high);

					md5ps.setLong(1, vphotoid);
					md5ps.setLong(2, vmd5high);
					int delcnt = md5ps.executeUpdate();
					if (delcnt == 1) {

						recyclebinps.setLong(1, vphotoid);
						recyclebinps.setLong(2, 1330531200000L); // 2012-03-01
						recyclebinps.executeUpdate();
					}
					conn.commit();
					allcnt++;
				}
			}
			reader.close();
			System.out.println(df.format(new Date()) + "; all count:" + allcnt);
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
		conn.close();
	}
}
