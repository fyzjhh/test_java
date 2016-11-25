package com.test.cp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CalFsAccCnt {
	static Log logger = LogFactory.getLog(CalFsAccCnt.class);
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	static long macrosecsinday = 24 * 3600 * 1000;

	static String[] tos = {};
	static Hashtable<String, String> toMap = new Hashtable<String, String>();

	public static void main(String[] args) throws Exception {
		// if (args.length != 1) {
		// System.out.println("usage: exec filename");
		// System.exit(1);
		// }
		int allcnt = 0;
		int zerocnt = 0;
		int onecnt = 0;
		int twocnt = 0;
		int threecnt = 0;
		// String ufile = args[0];
		String ufile = "D:/temp/urs_del.201211.txt";
		File file = new File(ufile);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tmpStr = null;

			while ((tmpStr = reader.readLine()) != null) {
				allcnt++;
				int hc = Math.abs(tmpStr.hashCode());
				if (hc % 4 == 0) {
					zerocnt++;
				}
				if (hc % 4 == 1) {
					onecnt++;
				}
				if (hc % 4 == 2) {
					twocnt++;
				}
				if (hc % 4 == 3) {
					threecnt++;
				}
				if (allcnt % 100000 == 99999) {
					float zerof = (float) zerocnt / (float) allcnt;
					float onef = (float) onecnt / (float) allcnt;
					float twof = (float) twocnt / (float) allcnt;
					float threef = (float) threecnt / (float) allcnt;
					String s = "stats:" + allcnt + "  ,  " + zerocnt + ","
							+ onecnt + "," + twocnt + "," + threecnt + "  ,  "
							+ zerof + "," + onef + "," + twof + "," + threef;
					logger.info(s);
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

	}

}