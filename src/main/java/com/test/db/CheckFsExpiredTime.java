package com.test.db;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CheckFsExpiredTime {

	static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String url = "172.17.3.85:8890?key=src/secret.key";
	static String user = "fs_mirror";
	static String pass = "fs_mirror";

	public static void main(String[] args) throws Exception {
		geterrorrecord();
	}

	private static void geterrorrecord() throws Exception {
		Long startTime = System.currentTimeMillis();

		checkerrorrecord();

		Long endTime = System.currentTimeMillis();
		System.out.println("”√ ±£∫" + df.format(new Date(endTime - startTime)));

	}

	public static void checkerrorrecord() throws Exception {
		BufferedWriter output = new BufferedWriter(new FileWriter(
				"E:/temp/fserrorrecord.txt"));

		int allerrcnt = 0;// counter
		int allcnt = 0;// counter
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(url, user, pass);

		for (int m = 0; m < 10; m++) {
			System.out.println(df.format(new Date()) + "; round :" + m);
			output.write(df.format(new Date()) + "; round :" + m + "\r\n");

			PreparedStatement fileps = conn
					.prepareStatement("select expiredtime from FS_File where docid=? ;");

			Statement md5ps = conn.createStatement();
			ResultSet md5rs = md5ps
					.executeQuery("select photoid, expirytime from PhotoMD5 where expirytime >= 1327852800000 limit 100 offset "
							+ 100 * m + " ;");

			while (md5rs.next()) {

				long vphotoid = md5rs.getLong(1);
				long vexpirytime = md5rs.getLong(2);
				fileps.setLong(1, vphotoid);
				ResultSet filers = fileps.executeQuery();

				while (filers.next()) {
					long res_expirytime = filers.getLong(1);

					if (vexpirytime != -1 && res_expirytime != vexpirytime) {
						allerrcnt++;
						System.out.println(df.format(new Date()) + "; id :"
								+ vphotoid + ",expirytime in photomd5 :"
								+ vexpirytime + ",expirytime in fs_file :"
								+ res_expirytime + " is not same .");
						output.write(df.format(new Date()) + "; id :"
								+ vphotoid + ",expirytime in photomd5 :"
								+ vexpirytime + ",expirytime in fs_file :"
								+ res_expirytime + " is not same ." + "\r\n");
						break;
					}
					allcnt++;
					if (allcnt % 200 == 0) {
						System.out.println(df.format(new Date()) + "; allcnt :"
								+ allcnt + ",allerrcnt :" + allerrcnt);
						output.write(df.format(new Date()) + "; allcnt :"
								+ allcnt + ",allerrcnt :" + allerrcnt + "\r\n");
					}
				}

			}
			fileps.close();
			md5rs.close();
		}
		output.close();
		System.out.println(df.format(new Date()) + "; all count:" + allerrcnt);

		conn.close();
	}

}
