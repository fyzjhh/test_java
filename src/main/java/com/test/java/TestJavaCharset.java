package com.test.java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TestJavaCharset implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3014716914084285225L;
	private static final int MIN_INDEX = 19968;
	private static final int MAX_INDEX = 40869;
	private static final String CR = "\r\n";
	private static final String TAB = "\t";

	
	
	public static void main(String[] args) throws Exception {

		convertChar();

		System.out.println();
		System.out.println("====success====");
	}

	private static void convertChar1() throws UnsupportedEncodingException {
		System.out.println(Charset.defaultCharset().name());
		String[] srccharset = new String[] { "latin1", "CP936", "GB18030",
				"utf8" };
		String[] descharset = new String[] { "latin1", "CP936", "GB18030",
				"utf8" };
		String srcstr = "中文";
		for (int i = 0; i < srccharset.length; i++) {
			byte[] bytes = srcstr.getBytes(srccharset[i]);
			for (int j = 0; j < bytes.length; j++) {
				System.out.print(bytes[j] + " ");
			}
			System.out.println();

			for (int k = 0; k < srccharset.length; k++) {
				String desstr = new String(bytes, descharset[k]);
				System.out.println(descharset[k] + "\t" + desstr);
			}
		}

	}
	
	public static String test_charset(byte[] bs, String charset) {

		String ret = "unknown";
		try {
			ret = new String(bs, charset);
		} catch (Exception e) {

			String msg = "unencode err: byte array is " + Arrays.toString(bs)
					+ ",err msg is " + e.getMessage();
			System.out.println(msg);
			ret = "unknown";
		}
		return ret;
	}
	
	private static void printGB18030() throws Exception {
		// λ������ñ��ֽڱ�ʾ(1 ASCII��2��4�ֽ�)���ɱ�ʾ27484�����֡�
		// ��Χ��1�ֽڴ�00��7F;
		// 2�ֽڸ��ֽڴ�81��FE�����ֽڴ�40��7E��80��FE��4�ֽڵ�һ���ֽڴ�81��FE���ڶ����ֽڴ�30��39��
		BufferedWriter output = new BufferedWriter(new FileWriter(
				"D:/installfiles/gb18030code.txt"));

		String charname = "gb18030";
		for (byte bytes = 0; bytes <= 127 && bytes >= 0; bytes++) {

			String str = new String(new byte[] { bytes }, charname);

			output.write(str + "=[" + (bytes) + "]\t");
		}
		output.write("\r\n");

		for (int highbyte = 128; highbyte <= 255; highbyte++) {
			System.out.println("writing highbyte ..." + highbyte);
			for (int lowbyte = 0; lowbyte <= 255; lowbyte++) {

				byte[] bytes = new byte[] { (byte) highbyte, (byte) lowbyte };
				String str = new String(bytes, charname);
				output.write(str + "=" + Arrays.toString(bytes) + "\t");
			}
			output.write("\r\n");
		}
		output.write("\r\n");
		for (int firstbyte = 129; firstbyte <= 254; firstbyte++) {
			System.out.println("writing highbyte ..." + firstbyte);
			for (int secondbyte = 30; secondbyte <= 39; secondbyte++) {

				for (int thirdbyte = 129; thirdbyte <= 254; thirdbyte++) {
					for (int forthbyte = 30; forthbyte <= 39; forthbyte++) {

						byte[] bytes = new byte[] { (byte) firstbyte,
								(byte) secondbyte, (byte) thirdbyte,
								(byte) forthbyte };
						String str = new String(bytes, charname);
						output.write(str + "=" + Arrays.toString(bytes) + "\t");
					}
				}

			}
			output.write("\r\n");
		}
		output.write("\r\n");

		output.close();
	}

	private static void printGBK() throws Exception {

		// ���ֽڴ�81��FE�����ֽڴ�40��FE��
		BufferedWriter output = new BufferedWriter(new FileWriter(
				"D:/installfiles/gbkcode.txt"));

		String charname = "gbk";
		for (byte bytes = 0; bytes <= 127 && bytes >= 0; bytes++) {

			String str = new String(new byte[] { bytes }, charname);

			output.write(str + "=[" + (bytes) + "]\t");
		}
		output.write("\r\n");
		for (int highbyte = 128; highbyte <= 255; highbyte++) {
			System.out.println("writing highbyte ..." + highbyte);
			for (int lowbyte = 0; lowbyte <= 255; lowbyte++) {

				byte[] bytes = new byte[] { (byte) highbyte, (byte) lowbyte };
				String str = new String(bytes, charname);
				output.write(str + "=" + Arrays.toString(bytes) + "\t");
			}
			output.write("\r\n");
		}
		output.close();
	}

	public static void printDirectory(File f) {
		if (!f.isDirectory()) {// �����Ŀ¼�����ӡ���
			System.out.println("FFFF    " + f.getAbsolutePath());
		} else {
			File[] fs = f.listFiles();
			System.out.println("DDDD    " + f.getAbsolutePath());

			for (int i = 0; i < fs.length; ++i) {
				File file = fs[i];
				printDirectory(file);
			}
		}
	}

	private static void convertChar() throws UnsupportedEncodingException {
		String[] srccharset = new String[] { "latin1", "CP936", "GB18030",
				"utf8" };
		String[] descharset = new String[] { "latin1", "CP936", "GB18030",
				"utf8" };
		String srcstr = "����";

		byte[] bytes = srcstr.getBytes("utf-16");
		for (int j = 0; j < bytes.length; j++) {
			System.out.print(bytes[j] + " ");
		}
		System.out.println();

//		String desstr = new String(bytes, "utf8");
//		System.out.println("utf8" + "\t" + desstr);

	}

	@SuppressWarnings("rawtypes")
	private static void printAllCharSets() {
		// ��ǰϵͳ�����ַ�SortedMap<String,Charset>
		Map charSets = Charset.availableCharsets();
		Iterator it = charSets.keySet().iterator();
		while (it.hasNext()) {
			// �ַ���
			String csName = (String) it.next();

			System.out.print(csName);
			// charset.aliases()�����ַ����� Set<String>
			Iterator aliases = ((Charset) charSets.get(csName)).aliases()
					.iterator();

			// ��ʾ����
			if (aliases.hasNext()) {
				System.out.print(": ");
			}
			while (aliases.hasNext()) {
				System.out.print(aliases.next());
				if (aliases.hasNext()) {
					System.out.print(", ");
				}
			}
			System.out.println();
		}
	}

	private static void test() {

		String s = "����";
		System.out.println(s.getBytes());
		// System.out.println(System.getProperties());
		// getrand(10);
		// try {
		// String ss = new
		// String("�ķ���ķ���ķ���ķ�����������˿��־������˼��෢Ŷ�Ƿ������İ���������".getBytes(""),"utf8");
		// System.out.println(ss);
		// } catch (UnsupportedEncodingException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");



	private static long getrandom(long small, long large) throws Exception {

		return small + (long) ((large - small) * Math.random());

	}

	private static void printenv() throws Exception {
		// \u4e00-\u9fa5
		int GBKCode;
		for (int i = MIN_INDEX; i <= MAX_INDEX; i++) {
			GBKCode = getGBKCode(i);

			System.out.print((char) i + TAB + i + TAB + Integer.toHexString(i)
					+ TAB + GBKCode + TAB + Integer.toHexString(GBKCode) + CR);

		}
		System.out.println("======System AvailableLocales:======== ");
		Locale list[] = DateFormat.getAvailableLocales();

		for (int i = 0; i < list.length; i++) {
			System.out.println(list[i].toString() + "\t"
					+ list[i].getDisplayName());
		}

		System.out.println("======System availableCharsets:======== ");
		Map m = Charset.availableCharsets();
		Set names = m.keySet();
		Iterator it = names.iterator();
		while (it.hasNext()) {
			System.out.println(it.next());
		}
		System.out.println("======System env:======== ");
		System.out.println(System.getenv());

		System.out.println("======System Properties:======== ");
		System.getProperties().list(System.out);
	}

	private static int getGBKCode(int unicodeCode)
			throws UnsupportedEncodingException {
		char c = (char) unicodeCode;
		byte[] bytes = (c + "").getBytes("GBK");
		return ((bytes[0] & 255) << 8) + (bytes[1] & 255);
	}

}
