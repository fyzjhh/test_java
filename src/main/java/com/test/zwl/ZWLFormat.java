package com.test.zwl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;

public class ZWLFormat {
	private static final String FG = "--";
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	static long macrosecsinday = 24 * 3600 * 1000;
	static String basefd = "D:/temp/jpm/";
	static String ufile = "D:/jhhWorks/jhh/temp.txt";
	static Hashtable<String, String> toMap = new Hashtable<String, String>();
	static String fromdt = df.format(new Date());
	static Hashtable<String, String> userMap = new Hashtable<String, String>(
			5000);

	public static void main(String[] args) throws Exception {
		xuhao();
		System.out.println("=========success==============");
	}

	private static void xuhao() {
		String questionfile = "D:/temp/temp2.txt";

		BufferedReader qreader = null;
		try {
			qreader = new BufferedReader(new FileReader(questionfile));
			String tmpStr = null;

			int m = 0;
			while ((tmpStr = qreader.readLine()) != null) {
				if (tmpStr.matches("^[0-9]+\\. .*")) {
					m++;
					System.out.println(m + ". " + tmpStr.substring(3).trim());
				} else {
					System.out.println(tmpStr);
				}

			}
			qreader.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void del() {
		String questionfile = "D:/temp/zwl/q.txt";
		String answerfile = "D:/temp/zwl/a.txt";

		BufferedReader areader = null;
		BufferedReader qreader = null;
		try {
			areader = new BufferedReader(new FileReader(answerfile));
			qreader = new BufferedReader(new FileReader(answerfile));
			String tmpStr = null;

			String str = "";
			while ((tmpStr = areader.readLine()) != null) {
				if ((tmpStr.matches(".*答案：.*对.*") || tmpStr
						.matches(".*答案：.*错.*")) && tmpStr.length() < 12) {
					str = tmpStr.substring(0, 3).trim();
					System.out.print(str + " ");
				}
			}
			areader.close();
			qreader.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void format() {
		String srcfile = "D:/jhhWorks/jhh/temp.txt";
		String questionfile = "D:/temp/zwl/q.txt";
		String answerfile = "D:/temp/zwl/a.txt";
		String qafile = "D:/temp/zwl/qa.txt";
		File file = new File(srcfile);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tmpStr = null;
			FileOutputStream qfos = new FileOutputStream(questionfile, true);
			OutputStreamWriter qout = new OutputStreamWriter(qfos);
			FileOutputStream afos = new FileOutputStream(answerfile, true);
			OutputStreamWriter aout = new OutputStreamWriter(afos);
			FileOutputStream qafos = new FileOutputStream(qafile, true);
			OutputStreamWriter qaout = new OutputStreamWriter(qafos);
			boolean isQuestion = true;
			String str = "";
			int n = 0;
			while ((tmpStr = reader.readLine()) != null) {
				if (tmpStr.startsWith("xxx ")) {
					isQuestion = true;
					if (n > 0) {
						String o = n + "  " + str;
						aout.write(o + "\r\n");
						qaout.write(o + "\r\n");
					}
					str = tmpStr.substring(5);
					n++;
				} else if (tmpStr.startsWith("答案：")) {
					isQuestion = false;
					String o = n + " " + str;
					qout.write(o + "\r\n");
					qaout.write(o + "\r\n");
					str = tmpStr;
				} else if (tmpStr.matches("[ \t]*")) {
					continue;
				} else {
					str = str.concat(tmpStr);
				}
			}
			String o = n + "  " + str;
			aout.write(o + "\r\n");
			qaout.write(o + "\r\n");
			reader.close();
			qout.close();
			aout.close();
			qaout.close();
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
