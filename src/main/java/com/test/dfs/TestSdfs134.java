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

public class TestSdfs134 {
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

	}

	private static void uploadfile() throws Exception {

		MFSManager mfs = new MFSManager();

		// mfs.launch("220.181.72.233", 5555);
		mfs.launch("220.181.72.233:5555,220.181.72.234:5555");
		File file = new File("D:/temp/commons-dbcp-1.4.jar");

		FileInputStream fis = new FileInputStream(file);
		FileStream fs = new FileStream(fis, file.length());
		MFSStream stream = new MFSStream(fs);
		stream.setLastWrite(true);
		long[] docids = mfs.allocMultiIDsWithSN(1, 0, "192.168.224.62", 5500, 50);
		mfs.appendFile(docids[0], stream);

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
