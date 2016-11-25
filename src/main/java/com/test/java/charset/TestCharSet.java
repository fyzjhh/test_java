package com.test.java.charset;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class TestCharSet {
	final static String[] CHARNAMES = { "GBK", "GB2312", "GB18030", "UTF-8",
			"UTF-16" };

	public static void main(String[] args) throws Exception {

		// printAllCharSets();
		printGBK();
		// printGB18030();
		// printGB2312();
		// analyze();
		// printutf16();
		test();
	}

	public static byte[] con_utf82gbk(byte[] srcBytes) throws Exception {

		byte[] bytes = new byte[2];
		String bitStr = Conversion.byteToBit(srcBytes[0]).substring(4)
				+ Conversion.byteToBit(srcBytes[1]).substring(2)
				+ Conversion.byteToBit(srcBytes[2]).substring(2);
		bytes[0] = Integer.valueOf(bitStr.substring(0, 8), 2).byteValue();
		bytes[1] = Integer.valueOf(bitStr.substring(8), 2).byteValue();
		return bytes;
	}

	public static byte[] con_gbk2utf8(byte[] srcBytes) throws Exception {
		byte[] bytes = new byte[3];
		String bitStr = "1110"
				+ Conversion.byteToBit(srcBytes[0]).substring(0, 4) + "10"
				+ Conversion.byteToBit(srcBytes[0]).substring(4, 8)
				+ Conversion.byteToBit(srcBytes[1]).substring(0, 2) + "10"
				+ Conversion.byteToBit(srcBytes[1]).substring(2, 8);
		bytes[0] = Integer.valueOf(bitStr.substring(0, 8), 2).byteValue();
		bytes[1] = Integer.valueOf(bitStr.substring(8, 16), 2).byteValue();
		bytes[2] = Integer.valueOf(bitStr.substring(16), 2).byteValue();
		return bytes;
	}

	public static byte[] conallgbk2utf8(byte[] gbkBytes) throws Exception {

		System.out.println(Conversion.byteToBit((byte) 129));
		System.out.println(Integer.valueOf("10000001", 2).byteValue());

		String gbkStr = new String(gbkBytes, "GBK");
		char c[] = gbkStr.toCharArray();
		byte[] utf8Bytes = new byte[3 * c.length];
		for (int i = 0; i < c.length; i++) {
			int m = (int) c[i];
			String word = Integer.toBinaryString(m);

			StringBuffer sb = new StringBuffer();
			int len = 16 - word.length();
			for (int j = 0; j < len; j++) {
				sb.append("0");
			}
			sb.append(word);
			sb.insert(0, "1110");
			sb.insert(8, "10");
			sb.insert(16, "10");

			String s1 = sb.substring(0, 8);
			String s2 = sb.substring(8, 16);
			String s3 = sb.substring(16);

			byte b0 = Integer.valueOf(s1, 2).byteValue();
			byte b1 = Integer.valueOf(s2, 2).byteValue();
			byte b2 = Integer.valueOf(s3, 2).byteValue();
			byte[] bf = new byte[3];
			bf[0] = b0;
			utf8Bytes[i * 3] = bf[0];
			bf[1] = b1;
			utf8Bytes[i * 3 + 1] = bf[1];
			bf[2] = b2;
			utf8Bytes[i * 3 + 2] = bf[2];

		}
		return utf8Bytes;
	}

	public static void correctEncode() throws UnsupportedEncodingException {
		String gbk = "�@";
		String iso = new String(gbk.getBytes("UTF-8"), "ISO-8859-1");
		for (byte b : iso.getBytes("ISO-8859-1")) {
			System.out.print(b + " ");
		}
		System.out.println(iso);

		// ģ��UTF-8�������վ��ʾ
		System.out.println(new String(iso.getBytes("ISO-8859-1"), "UTF-8"));
	}

	public static void encodeError() throws UnsupportedEncodingException {
		String gbk = "������";
		String utf8 = new String(gbk.getBytes("UTF-8"));

		// ģ��UTF-8�������վ��ʾ
		System.out.println(new String(utf8.getBytes(), "UTF-8"));
	}

	public static void encodeError2() throws UnsupportedEncodingException {
		String gbk = "����2011";
		String utf8 = new String(gbk.getBytes("UTF-8"));

		// ģ��UTF-8�������վ��ʾ
		System.out.println(new String(utf8.getBytes(), "UTF-8"));
	}

	public static void analyze() throws UnsupportedEncodingException {
		String gbk = "������";
		String utf8 = new String(gbk.getBytes("UTF-8"));
		for (byte b : gbk.getBytes("UTF-8")) {
			System.out.print(b + " ");
		}
		System.out.println();
		for (byte b : utf8.getBytes()) {
			System.out.print(b + " ");
		}
	}

	public static void gbk2Utf() throws UnsupportedEncodingException {
		String gbk = "������";
		char[] c = gbk.toCharArray();
		byte[] fullByte = new byte[3 * c.length];
		for (int i = 0; i < c.length; i++) {
			String binary = Integer.toBinaryString(c[i]);
			StringBuffer sb = new StringBuffer();
			int len = 16 - binary.length();
			// ǰ�油��
			for (int j = 0; j < len; j++) {
				sb.append("0");
			}
			sb.append(binary);
			// ����λ���ﵽ��24λ3���ֽ�
			sb.insert(0, "1110");
			sb.insert(8, "10");
			sb.insert(16, "10");
			fullByte[i * 3] = Integer.valueOf(sb.substring(0, 8), 2)
					.byteValue();// �������ַ�������
			fullByte[i * 3 + 1] = Integer.valueOf(sb.substring(8, 16), 2)
					.byteValue();
			fullByte[i * 3 + 2] = Integer.valueOf(sb.substring(16, 24), 2)
					.byteValue();
		}
		// ģ��UTF-8�������վ��ʾ
		System.out.println(new String(fullByte, "UTF-8"));
	}

	private static String bytestostring(byte[] bytes) throws Exception {
		String s = " ";
		for (int i = 0; i < bytes.length; i++) {
			// s = s + Conversion.byteToBit(bytes[i]) + " ";
			s = s + (bytes[i]) + " ";
		}
		return s;
	}

	private static void test() throws Exception {
		// String s = "01";
		// byte[] bytesutf8 = s.getBytes("UTF-8");
		// System.out.println(bytesutf8);
		byte[] a = new byte[] { (byte)0xd7, (byte)0xfc };
		String ds = new String(a, "gbk");
		System.out.println(ds);
	}

	/**
	 * �ж��ַ�ı���
	 * 
	 * @param str
	 * @return
	 */
	public static String getStrEncoding(String str) {
		String encode;
		encode = "GB2312";
		try {
			if (str.equals(new String(str.getBytes(encode), encode))) {
				String s = encode;
				return s;
			}
		} catch (Exception exception) {
		}
		encode = "ISO-8859-1";
		try {
			if (str.equals(new String(str.getBytes(encode), encode))) {
				String s1 = encode;
				return s1;
			}
		} catch (Exception exception1) {
		}
		encode = "UTF-8";
		try {
			if (str.equals(new String(str.getBytes(encode), encode))) {
				String s2 = encode;
				return s2;
			}
		} catch (Exception exception2) {
		}
		encode = "GBK";
		try {
			if (str.equals(new String(str.getBytes(encode), encode))) {
				String s3 = encode;
				return s3;
			}
		} catch (Exception exception3) {
		}
		encode = "BIG5";
		try {
			if (str.equals(new String(str.getBytes(encode), encode))) {
				String s4 = encode;
				return s4;
			}
		} catch (Exception exception3) {
		}
		return "";
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

	private static void printutf16() throws Exception {
		// UTF-16������2�ֽڣ�Unicode�в�ͬ���ֵ��ַ�ͬ��������еı�׼������Ϊ�˱���ת����
		// ��
		// 0��0000��0��007F��ASCII�ַ��0��0080��0��00FF��ISO-8859-1��ASCII����չ��
		// ϣ����ĸ��ʹ�ô�0��0370�� 0��03FF
		// �Ĵ��룬˹������ʹ�ô�0��0400��0��04FF�Ĵ��룬����ʹ�ô�0��0530��0��058F�Ĵ��룬ϣ������ʹ�ô�0��0590��0��05FF�Ĵ�
		// �롣
		// �й��ձ��ͺ�����������֣��ܳ�ΪCJK��ռ���˴�0��3000��0��9FFF�Ĵ��룻
		// ����0��00��c���Լ�����ϵͳ�ļ���������������壬�ʺ�
		// ���������ҪUTF-8���뱣���ı���ȥ�����0��00��

		String charname = "utf16";
		String file = "D:/jhhWorks/jhh/ziliao/jhh_utf16code.txt";
		FileOutputStream fos = new FileOutputStream(file);
		OutputStreamWriter output = new OutputStreamWriter(fos, charname);

		output.write("\r\n");
		for (int highbyte = 0; highbyte <= 255; highbyte++) {
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

	private static void printutf8() throws Exception {
		// ���ֻ��һ���ֽ�������߶�����λΪ0��
		// ����Ƕ��ֽڣ����һ���ֽڴ����λ��ʼ������Ķ�����λֵΪ1�ĸ���������������ֽ���������ֽھ���10��ͷ��
		// UTF-8�����õ�6���ֽڡ�

		/*
		 * 
		 * UTF8-octets = *( UTF8-char ) UTF8-char = UTF8-1 / UTF8-2 / UTF8-3 /
		 * UTF8-4 UTF8-1 = %x00-7F UTF8-2 = %xC2-DF UTF8-tail UTF8-3 = %xE0
		 * %xA0-BF UTF8-tail / %xE1-EC 2( UTF8-tail ) / %xED %x80-9F UTF8-tail /
		 * %xEE-EF 2( UTF8-tail ) UTF8-4 = %xF0 %x90-BF 2( UTF8-tail ) / %xF1-F3
		 * 3( UTF8-tail ) / %xF4 %x80-8F 2( UTF8-tail ) UTF8-tail = %x80-BF
		 */

		String charname = "utf8";
		String file = "D:/temp/jhh_utf8code.txt";
		FileOutputStream fos = new FileOutputStream(file);
		OutputStreamWriter output = new OutputStreamWriter(fos, charname);

		for (byte bytes = 0; bytes <= 127 && bytes >= 0; bytes++) {

			String str = new String(new byte[] { bytes }, charname);

			output.write(str + "=[" + (bytes) + "]\t");
		}
		output.write("\r\n");

		for (int highbyte = 194; highbyte < 224; highbyte++) {
			System.out.println("writing highbyte ..." + highbyte);
			for (int lowbyte = 128; lowbyte < 192; lowbyte++) {

				byte[] bytes = new byte[] { (byte) highbyte, (byte) lowbyte };
				String str = new String(bytes, charname);
				output.write(str + "=" + Arrays.toString(bytes) + "\t");
			}
			output.write("\r\n");
		}

		output.write("\r\n");
		for (int firstbyte = 224; firstbyte < 240; firstbyte++) {
			System.out.println("writing highbyte ..." + firstbyte);
			for (int secondbyte = 128; secondbyte < 192; secondbyte++) {

				for (int thirdbyte = 128; thirdbyte < 192; thirdbyte++) {

					byte[] bytes = new byte[] { (byte) firstbyte,
							(byte) secondbyte, (byte) thirdbyte };
					String str = new String(bytes, charname);
					output.write(str + "=" + Arrays.toString(bytes) + "\t");
				}

			}
			output.write("\r\n");
		}
		output.write("\r\n");
		for (int firstbyte = 240; firstbyte < 244; firstbyte++) {
			System.out.println("writing highbyte ..." + firstbyte);
			for (int secondbyte = 128; secondbyte < 192; secondbyte++) {

				for (int thirdbyte = 128; thirdbyte < 192; thirdbyte++) {
					for (int forthbyte = 128; forthbyte < 192; forthbyte++) {

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

	private static void printGB18030() throws Exception {
		// λ������ñ��ֽڱ�ʾ(1 ASCII��2��4�ֽ�)���ɱ�ʾ27484�����֡�
		// ��Χ��1�ֽڴ�00��7F;
		// 2�ֽڸ��ֽڴ�81��FE�����ֽڴ�40��7E��80��FE��4�ֽڵ�һ���ֽڴ�81��FE���ڶ����ֽڴ�30��39��
		BufferedWriter output = new BufferedWriter(new FileWriter(
				"D:/installfiles/jhh_gb18030code.txt"));

		String charname = "gb18030";
		for (byte bytes = 0; bytes <= 127 && bytes >= 0; bytes++) {

			String str = new String(new byte[] { bytes }, charname);

			output.write(str + "=[" + (bytes) + "]\t");
		}
		output.write("\r\n");

		for (int highbyte = 129; highbyte <= 255; highbyte++) {
			System.out.println("writing highbyte ..." + highbyte);
			for (int lowbyte = 64; lowbyte <= 126; lowbyte++) {

				byte[] bytes = new byte[] { (byte) highbyte, (byte) lowbyte };
				String str = new String(bytes, charname);
				output.write(str + "=" + Arrays.toString(bytes) + "\t");
			}
			for (int lowbyte = 129; lowbyte <= 255; lowbyte++) {

				byte[] bytes = new byte[] { (byte) highbyte, (byte) lowbyte };
				String str = new String(bytes, charname);
				output.write(str + "=" + Arrays.toString(bytes) + "\t");
			}
			output.write("\r\n");
		}
		output.write("\r\n");
		for (int firstbyte = 129; firstbyte <= 255; firstbyte++) {
			System.out.println("writing highbyte ..." + firstbyte);
			for (int secondbyte = 48; secondbyte <= 64; secondbyte++) {

				for (int thirdbyte = 129; thirdbyte <= 255; thirdbyte++) {
					for (int forthbyte = 48; forthbyte <= 64; forthbyte++) {

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

	private static void printGB2312() throws Exception {

		// ��Χ�����ֽڴ�A1��F7, ���ֽڴ�A1��FE�������ֽں͵��ֽڷֱ����0XA0���ɵõ����롣

		String charname = "gb2312";
		String file = "D:/jhhWorks/jhh/ziliao/jhh_gb2312code.txt";
		FileOutputStream fos = new FileOutputStream(file);
		OutputStreamWriter output = new OutputStreamWriter(fos, charname);

		for (byte bytes = 0; bytes <= 127 && bytes >= 0; bytes++) {

			String str = new String(new byte[] { bytes }, charname);

			output.write(str + "=[" + (bytes) + "]\t");
		}
		output.write("\r\n");
		for (int highbyte = 161; highbyte <= 247; highbyte++) {
			System.out.println("writing highbyte ..." + highbyte);
			for (int lowbyte = 161; lowbyte <= 254; lowbyte++) {

				byte[] bytes = new byte[] { (byte) highbyte, (byte) lowbyte };
				String str = new String(bytes, charname);
				output.write(str + "=" + Arrays.toString(bytes) + "\t");
			}
			output.write("\r\n");
		}
		output.close();
	}

	private static void printGBK() throws Exception {
		// ���ֽڴ�81��FE�����ֽڴ�40��FE��

		String charname = "gbk";
		String file = "D:/temp/jhh_gbkcode.txt";
		FileOutputStream fos = new FileOutputStream(file);
		OutputStreamWriter output = new OutputStreamWriter(fos, charname);

		for (byte bytes = 0; bytes <= 127 && bytes >= 0; bytes++) {

			String str = new String(new byte[] { bytes }, charname);

			output.write(str + "=[" + (bytes) + "]\t");
		}
		output.write("\r\n");
		for (int highbyte = 129; highbyte <= 254; highbyte++) {
			System.out.println("writing highbyte ..." + highbyte);
			for (int lowbyte = 64; lowbyte <= 254; lowbyte++) {

				if (lowbyte != 127) {
					byte[] bytes = new byte[] { (byte) highbyte, (byte) lowbyte };
					String str = new String(bytes, charname);
					output.write(str + "=" + Arrays.toString(bytes) + "\t");
				}

			}
			output.write("\r\n");
		}
		output.close();
	}

}
