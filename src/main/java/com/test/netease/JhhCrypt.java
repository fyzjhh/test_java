package com.test.netease.other;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

/**
 * 
 * @author jhh
 * 
 */
public class JhhCrypt {
	private static String jhhcode = "UTF-8";
	private static byte[] jhhcrypt = "xxx".getBytes();
	private static int jhhcryptlen = jhhcrypt.length;

	private static char[] base64EncodeChars = new char[] { 'A', 'B', 'C', 'D',
			'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q',
			'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
			'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
			'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3',
			'4', '5', '6', '7', '8', '9', '+', '/' };

	private static byte[] base64DecodeChars = new byte[] { -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
			-1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59,
			60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
			10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1,
			-1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
			38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1,
			-1, -1 };

	public static void main(String[] args) throws Exception {
		testencodefile("D:/temp/3449757314566464918.jpg", "D:/temp/jia.txt");
		testdecodefile("D:/temp/jia.txt", "D:/temp/jie.jpg");
	}

	/**
	 * ���Լ���һ���ļ�
	 * 
	 * @param sr
	 * @param df
	 * @return
	 * @throws Exception
	 */
	public static boolean testencodefile(String sr, String df) throws Exception {
		BufferedReader rf = null;
		String tls = null;
		rf = new BufferedReader(new FileReader(sr));

		FileOutputStream fos = new FileOutputStream(df);
		OutputStreamWriter output = new OutputStreamWriter(fos);

		while ((tls = rf.readLine()) != null) {
			String mi = JhhCrypt.jhhencode(tls);
			output.write(mi + "\n");
		}

		rf.close();
		output.close();
		return true;
	}

	/**
	 * ���Խ���һ���ļ�
	 * 
	 * @param sr
	 * @param df
	 * @return
	 * @throws Exception
	 */
	public static boolean testdecodefile(String sr, String df) throws Exception {
		BufferedReader rf = null;
		String tls = null;
		rf = new BufferedReader(new FileReader(sr));

		FileOutputStream fos = new FileOutputStream(df);
		OutputStreamWriter output = new OutputStreamWriter(fos);

		while ((tls = rf.readLine()) != null) {
			String mi = JhhCrypt.jhhdecode(tls);
			output.write(mi + "\n");
		}

		rf.close();
		output.close();
		return true;
	}

	public static String getJhhcode() {
		return jhhcode;
	}

	public static void setJhhcode(String jhhcode) {
		JhhCrypt.jhhcode = jhhcode;
	}

	public static byte[] getJhhcrypt() {
		return jhhcrypt;
	}

	public static void setJhhcrypt(byte[] jhhcrypt) {
		JhhCrypt.jhhcrypt = jhhcrypt;
		JhhCrypt.jhhcryptlen = jhhcrypt.length;
	}

	/*
	 * ����base64�Ľ�һ������
	 */
	public static String jhhencode(String str) {

		String srcstr = str;

		byte[] srcbyte = null;
		try {
			srcbyte = srcstr.getBytes(jhhcode);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < srcbyte.length; i++) {
			srcbyte[i] = (byte) (srcbyte[i] + jhhcrypt[i % jhhcryptlen]);
		}

		String enstr = JhhCrypt.encode(srcbyte);

		return enstr;
	}

	/*
	 * ����base64�Ľ�һ������
	 */
	public static String jhhdecode(String str) {

		String enstr = str;

		byte[] desbyte = JhhCrypt.decode(enstr);

		for (int i = 0; i < desbyte.length; i++) {
			desbyte[i] = (byte) (desbyte[i] - jhhcrypt[i % jhhcryptlen]);
		}
		String desstr = null;
		try {
			desstr = new String(desbyte, jhhcode);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return desstr;
	}

	/*
	 * base64����
	 */
	public static String encode(byte[] data) {
		StringBuffer sb = new StringBuffer();
		int len = data.length;
		int i = 0;
		int b1, b2, b3;

		while (i < len) {
			b1 = data[i++] & 0xff;
			if (i == len) {
				sb.append(base64EncodeChars[b1 >>> 2]);
				sb.append(base64EncodeChars[(b1 & 0x3) << 4]);
				sb.append("==");
				break;
			}
			b2 = data[i++] & 0xff;
			if (i == len) {
				sb.append(base64EncodeChars[b1 >>> 2]);
				sb.append(base64EncodeChars[((b1 & 0x03) << 4)
						| ((b2 & 0xf0) >>> 4)]);
				sb.append(base64EncodeChars[(b2 & 0x0f) << 2]);
				sb.append("=");
				break;
			}
			b3 = data[i++] & 0xff;
			sb.append(base64EncodeChars[b1 >>> 2]);
			sb.append(base64EncodeChars[((b1 & 0x03) << 4)
					| ((b2 & 0xf0) >>> 4)]);
			sb.append(base64EncodeChars[((b2 & 0x0f) << 2)
					| ((b3 & 0xc0) >>> 6)]);
			sb.append(base64EncodeChars[b3 & 0x3f]);
		}
		return sb.toString();
	}

	/*
	 * base64����
	 */
	public static byte[] decode(String str) {
		byte[] data = str.getBytes();
		int len = data.length;
		ByteArrayOutputStream buf = new ByteArrayOutputStream(len);
		int i = 0;
		int b1, b2, b3, b4;

		while (i < len) {

			do {
				b1 = base64DecodeChars[data[i++]];
			} while (i < len && b1 == -1);
			if (b1 == -1) {
				break;
			}

			do {
				b2 = base64DecodeChars[data[i++]];
			} while (i < len && b2 == -1);
			if (b2 == -1) {
				break;
			}
			buf.write((int) ((b1 << 2) | ((b2 & 0x30) >>> 4)));

			do {
				b3 = data[i++];
				if (b3 == 61) {
					return buf.toByteArray();
				}
				b3 = base64DecodeChars[b3];
			} while (i < len && b3 == -1);
			if (b3 == -1) {
				break;
			}
			buf.write((int) (((b2 & 0x0f) << 4) | ((b3 & 0x3c) >>> 2)));

			do {
				b4 = data[i++];
				if (b4 == 61) {
					return buf.toByteArray();
				}
				b4 = base64DecodeChars[b4];
			} while (i < len && b4 == -1);
			if (b4 == -1) {
				break;
			}
			buf.write((int) (((b3 & 0x03) << 6) | b4));
		}
		return buf.toByteArray();
	}

}
