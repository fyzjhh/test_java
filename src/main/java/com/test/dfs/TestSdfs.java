package com.test.dfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.netease.backend.dfs.FileStream;
import com.netease.backend.mfs.MFSManager;
import com.netease.backend.mfs.MFSStream;

public class TestSdfs {

	public static void main(String[] args) throws Exception {
		uploadfile();
		// getalldocids("D:/installfiles/allpaths.txt",
		// "D:/installfiles/allpathdocids.txt");
		// getalldocids(
		// "C:/Documents and Settings/User/My Documents/allpaths.txt",
		// "C:/Documents and Settings/User/My Documents/alldocidsxxx.txt");
	}

	private static void uploadfile() throws Exception {

		MFSManager mfs = new MFSManager();

		mfs.launch("localhost", 5555);
		String dirpath = "E:\\datas\\iometerres\\ceshi";
		File file = new File(dirpath);
		if (file.isDirectory()) {
			File[] files = file.listFiles();

			for (int i = 0; i < files.length; ++i) {

				FileInputStream fis = new FileInputStream(files[i]);
				FileStream fs = new FileStream(fis, file.length());
				fs.setStorageType(1);
				fs.setUserType(0);
				MFSStream stream = new MFSStream(fs);

				long docid = mfs.insertFile(stream);

				fis.close();
				stream.close();
				System.out.println(files[i].getAbsolutePath() + " is uploaded "
						+ docid);
			}
		}

		mfs.shutdown();
		System.out.println("==============success=================");

	}

	private static void testddbconn() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(
				"172.17.2.47:8888?key=src/secret.key", "photomd5", "photomd5");

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("select photoid from PhotoMD5 limit 10");

		while (rs.next()) {
			System.out.println(rs.getInt(1));
		}
	}

	private static int testselect() throws Exception {
		Class.forName("com.netease.backend.db.DBDriver");
		Connection conn = DriverManager.getConnection(
				"172.17.2.47:8888?key=src/secret.key", "photomd5", "photomd5");

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("select photoid from PhotoMD5 limit 10");

		while (rs.next()) {
			System.out.println(rs.getInt(1));
		}
		return 0;
	}

	private static void getallnondocids(String in, String out) throws Exception {

		BufferedWriter output = new BufferedWriter(new FileWriter(out));

		File file = new File(in);
		BufferedReader reader = null;
		Long docidstr = null;
		try {
			Class.forName("com.netease.backend.db.DBDriver");
			Connection conn = DriverManager.getConnection(
					"172.17.3.85:8890?key=src/secret.key", "fs_m_photomd5",
					"fs_m_photomd5");
			reader = new BufferedReader(new FileReader(file));

			while ((docidstr = new Long(reader.readLine())) != null) {

				PreparedStatement stmt = conn
						.prepareStatement("select photoid from PhotoMD5 where photoid=? limit 1;");
				stmt.setLong(1, docidstr);
				ResultSet rs = stmt.executeQuery();
				if (rs.next()) {
					Long photoid = rs.getLong(1);
					output.write(docidstr + " : " + photoid + "\r\n");
				} else {
					output.write(docidstr + " : not exists" + "\r\n");
				}

			}

			reader.close();
			output.close();
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

	}

	private static void getalldocids(String in, String out) throws Exception {

		BufferedWriter output = new BufferedWriter(new FileWriter(out));

		File file = new File(in);
		BufferedReader reader = null;
		String str = null;
		try {
			reader = new BufferedReader(new FileReader(file));

			while ((str = reader.readLine()) != null) {
				// long docid = getId(str);
				if (!str.contains("trash"))
					output.write(getId(str) + "\r\n");
			}

			reader.close();
			output.close();
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

	}

	//
	// public static long getId(String path) {
	// String[] s = path.split("/");
	//
	// if (s.length == 0 || s.length > 5) {
	// System.err.println("Invalid Path " + path);
	// System.exit(-1);
	// }
	// int bucketId = Integer.valueOf(s[0]);
	// long docid = (long) bucketId << 48;
	//
	// for (int i = 1; i < s.length; ++i) {
	// short v = Short.valueOf(s[i]).shortValue();
	// if (v >= (1 << 12)) {
	// System.err.println("Invalid Path " + path);
	// System.exit(-1);
	// }
	// docid |= (long) v << (4 - i) * 12;
	// }
	// return docid;
	// }

	public static long getId(String path) {
		String[] s = path.split("/");

		if (s.length == 0 || s.length > 5) {
			System.err.println("Invalid Path " + path);
			System.exit(-1);
		}
		int bucketId = Integer.valueOf(s[0]);
		long docid = (long) bucketId << 40;

		for (int i = 1; i < s.length; ++i) {
			short v = Short.valueOf(s[i]).shortValue();
			if (v >= (1 << 10)) {
				System.err.println("Invalid Path " + path);
				System.exit(-1);
			}
			docid |= (long) v << (4 - i) * 10;
		}
		return docid;
	}

	public static String getPath(long id) {
		long mask = ((1 << 10) - 1);
		return String.valueOf(id >> 40) + File.separatorChar
				+ ((id >> 30) & mask) + File.separatorChar
				+ ((id >> 20) & mask) + File.separatorChar
				+ ((id >> 10) & mask) + File.separatorChar + (id & mask);
	}
	//
	// public static void getallpaths( String[] args ) {
	//
	// if( args.length != 1) {
	// System.err.println("Usage: Id2Path docid");
	// System.exit(-1);
	// }
	//
	// System.out.println(Id2Path.getPath(Long.valueOf(args[0]).longValue()));
	// }
	//
	// private static void uploadfile() throws Exception {
	// MFSManager mfs = new MFSManager();
	// try {
	// mfs.launch("172.17.2.201", 5559);
	// } catch (DFSException e) {
	// e.printStackTrace();
	// }
	// File file = new File("C:\\RHDSetup.log");
	// FileInputStream fis = new FileInputStream(file);
	//
	// FileStream fs = new FileStream(fis, file.length());
	// UFSStream stream = new UFSStream(fs);
	//
	// long[] docids = mfs
	// .allocMultiIDsWithSN(1, 0, "172.17.2.201", 5504, 150);
	// long docid = mfs.insertFile(docids[0], stream);
	// mfs.shutdown();
	// System.out.println("out==============================" + docid);
	// }
	//
	// private static void testInsert() throws Exception {
	// MFSManager mfs = new MFSManager();
	// try {
	// mfs.launch("172.17.2.201", 58163);
	// } catch (DFSException e) {
	// e.printStackTrace();
	// }
	// File file = new File("C:\\Symbol_of_ZJUT.png");
	// FileInputStream fis = new FileInputStream(file);
	//
	// FileStream fs = new FileStream(fis, file.length());
	// MFSStream stream = new MFSStream(fs);
	// String ip = "10.100.83.";
	// for (int i = 0; i < 1; i++) {
	// int snip = 66 + i;
	// long[] docid = mfs.allocMultiIDsWithSN(1, 0, ip + snip, 5500, 1);
	// mfs.appendFile(docid[0], fs);
	// mfs.insertFileEnd(docid[0], stream, true);
	// }
	//
	// System.out.println("out==============================");
	// }
}
