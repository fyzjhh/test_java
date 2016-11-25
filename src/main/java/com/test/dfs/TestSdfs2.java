package com.test.dfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.netease.backend.dfs.DFSException;
import com.netease.backend.dfs.FileStream;
import com.netease.backend.mfs.DocID;
import com.netease.backend.mfs.MFSManager;
import com.netease.backend.mfs.MFSStream;

public class TestSdfs2 {
	final static String appStr = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
	final static String appStrs = appStr + appStr + appStr + appStr + appStr
			+ appStr + appStr + appStr + appStr + appStr;
	final static String appStrss = appStrs + appStrs + appStrs + appStrs
			+ appStrs + appStrs + appStrs + appStrs + appStrs + appStrs;
	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws Exception {
		Long startTime = System.currentTimeMillis();

		uploadfile();

		Long endTime = System.currentTimeMillis();
		System.out.println(dtft.format(new Date(endTime - startTime)));
		// batchUploadfile();
		// getalldocids(
		// "C:\\Documents and Settings\\User\\My Documents\\paths.txt",
		// "C:\\Documents and Settings\\User\\My Documents\\docids.txt");
		// long docid = uploadfile();
		// readfile(docid);
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
				output.write(getsdfsId(str) + "\r\n");
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

	public static long getsdfsId(String path) {
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

	public static long getdfsId(String path) {
		String[] s = path.split("/");

		if (s.length == 0 || s.length > 5) {
			System.err.println("Invalid Path " + path);
			System.exit(-1);
		}
		int bucketId = Integer.valueOf(s[0]);
		long docid = (long) bucketId << 48;

		for (int i = 1; i < s.length; ++i) {
			short v = Short.valueOf(s[i]).shortValue();
			if (v >= (1 << 12)) {
				System.err.println("Invalid Path " + path);
				System.exit(-1);
			}
			docid |= (long) v << (4 - i) * 12;
		}
		return docid;
	}

	private static void readfile(long docid) throws Exception {

		MFSManager mfs = new MFSManager();
		try {
			mfs.launch("172.17.4.115", 5555);
		} catch (DFSException e) {
			e.printStackTrace();
		}

		InputStream ofis = mfs.getFile(docid).getInputStream();
		FileOutputStream fos = new FileOutputStream(new File(
				"E:\\temp\\QSServerConf_bak.xml"));
		byte[] buffer = new byte[4096];

		while (ofis.read(buffer) != -1) {
			fos.write(buffer);
		}
		fos.flush();

		ofis.close();
		fos.close();

		mfs.shutdown();
		System.out.println("out=================" + docid);

	}

	private static void batchUploadfile() throws Exception {
		// MFSManager mfs = new MFSManager();
		// try {
		// mfs.launch("172.17.2.201", 5557);
		// } catch (DFSException e) {
		// e.printStackTrace();
		// }
		// String baseStr = "zzzzzzzzzzzzzzzzzzzz ";
		// StringBuilder strBld = new StringBuilder(baseStr);
		// for (int i = 1; i <= 100; i++) {
		//
		// String us = strBld.toString();
		// MessageDigest md = MessageDigest.getInstance("MD5");
		// byte[] md5byte = md.digest(us.getBytes());
		//
		// ByteArrayInputStream bains = new ByteArrayInputStream(strBld
		// .toString().getBytes());
		// FileStream fs = new FileStream(bains, strBld.toString().length());
		//
		// MFSStream mfsStream = new MFSStream(fs);
		// // System.out.println(new String(md5byte));
		// mfsStream.setMd5(md5byte);
		// // mfsStream.setStorageType(1);
		// // mfsStream.setUserType(0);
		//
		// DocID doc_h = mfs.insertFileHead(mfsStream);
		// DocID docid = mfs.appendFile(doc_h.getDocId(), mfsStream, false);
		// // DocID doc_e = mfs.insertFileEnd(docid.getDocId(), mfsStream);
		//
		// fs.close();
		// mfsStream.close();
		//
		// // System.out.println(doc_e.getDocId() + " uploaded !!");
		// // Thread.sleep(2000);
		// // add string
		// for (int m = 1; m < i; m++) {
		// strBld.append(appStrs);
		// }
		// }
		// mfs.shutdown();
		// System.out.println("===============batch upload sucess");
	}

	private static void uploadfile() throws Exception {

		MFSManager mfs = new MFSManager();

		mfs.launch("fs-51.space.163.org", 5558);

		// FileStream dfs = mfs
		// .readFileQuick(1126999418470401279L, "172.17.4.126");
		//
		// InputStream dfis = dfs.getInputStream();
		//
		// FileOutputStream fos = new FileOutputStream("C:\\aaaa2");
		// int bytesRead;
		// byte[] buf = new byte[4096]; // 1K buffer
		// while ((bytesRead = dfis.read(buf)) != -1) {
		// fos.write(buf, 0, bytesRead);
		// }
		// fos.flush();
		// fos.close();
		// dfis.close();

		File file = new File(
				"C:\\Documents and Settings\\User\\×ÀÃæ\\temp\\google_logo.png");

		FileInputStream fis = new FileInputStream(file);
		FileStream fs = new FileStream(fis, file.length());
		MFSStream stream = new MFSStream(fs);

		long[] docids = mfs.allocMultiIDsWithSN(1, 0, "10.100.83.93", 5500, 50);
		// DocID doc_h = mfs.insertFileHead(stream);

		// long docid =
		mfs.appendFile(docids[0], stream);
		// DocID doc_e = mfs.insertFileEnd(docid, stream);
		fis.close();
		stream.close();
		mfs.shutdown();
		System.out.println("out=================" + docids[0]);

	}

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
