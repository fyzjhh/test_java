package com.test.mysql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class TestGenData {
	static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String url = "jdbc:mysql://172.17.2.163:4533/test";
	static String user = "root";
	static String password = "";
	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static DateFormat dft = new SimpleDateFormat("yyyy-MM-dd");
	static String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";
	static String alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static String[] suffs = { "@vip", "@163", "@188", "@csdn", "@sina", "@corp" };
	static Object[] enums = { "A", "B", "C", "D", "E" };
	static Object[] bank_enums = { "zhongguo", "nongye", "jianshe", "gongshang", "zhaoshang", "jiaotong" };

	public static void main(String[] args) throws Exception {
		test_gen_uinfo();
		test_gen_ulogin();
		test_gen_ubank();
		System.out.println("====success====");
	}

	// q_create_table_1====
	// CREATE /*+ hdp=hdp2 hdc=uid */ TABLE mytesthdb2.uinfo (
	// uid int NOT NULL,
	// uname varchar(32) NOT NULL DEFAULT '',
	// ubirthday date NOT NULL DEFAULT '1970-01-01',
	// PRIMARY KEY (uid),
	// UNIQUE KEY uname (uname),
	// KEY ubirthday (ubirthday)
	// ) ENGINE=InnoDB DEFAULT CHARSET=utf8
	// ;;;;
	//
	// q_create_table_2====
	// CREATE /*+ hdp=hdp2 hdc=uid */ TABLE mytesthdb2.ulogin (
	// uid int NOT NULL ,
	// uloginip varchar(32) NOT NULL DEFAULT '',
	// ulogindatetime datetime NOT NULL DEFAULT '1970-01-01',
	// PRIMARY KEY (uid,ulogindatetime),
	// KEY uloginip (uloginip),
	// KEY ulogindatetime (ulogindatetime)
	// ) ENGINE=InnoDB DEFAULT CHARSET=utf8
	// ;;;;
	//
	// q_create_table_3====
	// CREATE /*+ hdp=hdp2 hdc=uid */ TABLE mytesthdb2.ubank (
	// uid int NOT NULL ,
	// ucardbank varchar(32) NOT NULL DEFAULT '',
	// ucardno varchar(32) NOT NULL DEFAULT '',
	// PRIMARY KEY (uid,ucardno),
	// UNIQUE KEY ucardno (ucardno)
	// ) ENGINE=InnoDB DEFAULT CHARSET=utf8
	// ;;;;

	private static void test_gen_uinfo() throws Exception {

		Long startTime = System.currentTimeMillis();

		String tablename = "uinfo";
		int min = 1000000;
		int max = 2000000;
		int count = 500000;

		File file = new File("D:/tmp/" + tablename + ".txt");
		if (file.exists() == false) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file, false);
		BufferedWriter bw = new BufferedWriter(fw);

		for (int i = 1; i <= count; i++) {
			int uid = getrandom(min, max);
			String uname = getRndStr(3) + "_" + getRndStr(3);
			long tmptime = getrandom(1475251200000L, 1477929600000L);
			String ubirthday = dft.format(new Date(tmptime));
			String l = uid + "," + uname + "," + ubirthday + "\n";
			bw.write(l);
			if (i % 10000 == 0) {
				bw.flush();
				System.out.println("write count " + i);
			}
		}
		bw.close();

		Long endTime = System.currentTimeMillis();
		Date spenttime = new Date(endTime - startTime);
		System.out.println("spent time :" + format.format(spenttime));

	}

	private static void test_gen_ulogin() throws Exception {

		Long startTime = System.currentTimeMillis();

		String tablename = "ulogin";
		int min = 1000000;
		int max = 2000000;
		int count = 200000;

		File file = new File("D:/tmp/" + tablename + ".txt");
		if (file.exists() == false) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file, false);
		BufferedWriter bw = new BufferedWriter(fw);

		for (int i = 1; i <= count; i++) {
			int uid = getrandom(min, max);

			int logincount = getrandom(0, 10);
			for (int j = 1; j <= logincount; j++) {
				String uloginip = getrandom(1, 4) + "." + getrandom(4, 8);
				long tmptime = getrandom(1475251200000L, 1477929600000L);
				String ulogindatetime = dtft.format(new Date(tmptime));
				String l = uid + "," + uloginip + "," + ulogindatetime + "\n";
				bw.write(l);

			}
			if (i % 10000 == 0) {
				bw.flush();
				System.out.println("write count " + i);
			}
		}
		bw.close();

		Long endTime = System.currentTimeMillis();
		Date spenttime = new Date(endTime - startTime);
		System.out.println("spent time :" + format.format(spenttime));

	}

	private static void test_gen_ubank() throws Exception {

		Long startTime = System.currentTimeMillis();

		String tablename = "ubank";
		int min = 1000000;
		int max = 2000000;
		int count = 100000;

		File file = new File("D:/tmp/" + tablename + ".txt");
		if (file.exists() == false) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file, false);
		BufferedWriter bw = new BufferedWriter(fw);

		for (int i = 1; i <= count; i++) {
			int uid = getrandom(min, max);
			int logincount = getrandom(0, 5);
			for (int j = 1; j <= logincount; j++) {

				String ucardbank = (String) getRndBank();
				String ucardno = getrandom(1000, 9000) + "-" + getrandom(1000, 9000);
				String l = uid + "," + ucardbank + "," + ucardno + "\n";
				bw.write(l);

			}

			if (i % 10000 == 0) {
				bw.flush();
				System.out.println("write count " + i);
			}
		}
		bw.close();

		Long endTime = System.currentTimeMillis();
		Date spenttime = new Date(endTime - startTime);
		System.out.println("spent time :" + format.format(spenttime));

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
			int number = rndstr.nextInt(alpha.length());
			sb.append(alpha.charAt(number));
		}
		return sb.toString();
	}

	private static Object getRndEnum() throws Exception {

		int len = enums.length;
		int rndInd = getrandom(0, len - 1);
		return enums[rndInd];
	}

	private static Object getRndBank() throws Exception {

		int len = bank_enums.length;
		int rndInd = getrandom(0, len - 1);
		return bank_enums[rndInd];
	}
}
