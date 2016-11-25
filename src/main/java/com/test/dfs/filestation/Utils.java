package com.filestation.touchMFS;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;


/**
 * @author hzzhouxinran@corp.netease.com
 *         Date: 2011/10
 */
public class Utils {


	private static final char[] digits = {
			'0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'a', 'b',
			'c', 'd', 'e', 'f'};

	/**
	 * 邮件Header区字段名合法的字符范围
	 */
	private static final BitSet fieldChars = new BitSet();

	static {
		for (int i = 0x21; i <= 0x39; i++) {
			fieldChars.set(i);
		}
		for (int i = 0x3b; i <= 0x7e; i++) {
			fieldChars.set(i);
		}
	}

	public static String generateDigest(String content) {
		MessageDigest digest;
		try {
			digest = java.security.MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			return null;
		}
		digest.update(content.getBytes());
		byte[] hash = digest.digest();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < hash.length; i++) {
			sb.append(getByteAsHexString(hash[i]));
		}
		return sb.toString();
	}


	public static String getBytesAsHexString(byte[] b, int offset, int length) {
		StringBuffer sb = new StringBuffer();
		for (int i = offset; i < offset + length; i++) {
			sb.append(getByteAsHexString(b[i]));
		}
		return sb.toString();
	}

	public static String getByteAsHexString(byte b) {
		char[] buf = new char[2];
		int radix = 1 << 4;
		int mask = radix - 1;
		buf[1] = digits[(int) (b & mask)];
		b >>>= 4;
		buf[0] = digits[(int) (b & mask)];
		return new String(buf);
	}

	/**
	 * 去除字符串首尾指定的字符
	 *
	 * @param str
	 * @param c
	 * @return
	 */
	public static String trimChar(String str, char c) {
		if (str == null) return null;

		int len = str.length();
		int st = 0;
		char[] val = str.toCharArray();

		while ((st < len) && (val[st] == c)) {
			st++;
		}
		while ((st < len) && (val[len - 1] == c)) {
			len--;
		}

		return ((st > 0) || (len < val.length)) ? str.substring(st, len) : str;
	}

	/**
	 * 将指定字符串src，以每两个字符分割转换为16进制形式 如："2B44EFD9" --> byte[]{0x2B, 0x44, 0xEF,
	 * 0xD9}
	 *
	 * @param src String
	 * @return byte[]
	 */
	public static byte[] decodeMD5HexString(String src) throws Exception {
		if (src.length() != 32) {
			throw new Exception("The length of MD5 string is not 32! ");
		}

		byte[] ret = new byte[16];
		for (int i = 0; i < 16; i++) {
			int b1 = Character.digit(src.charAt(i * 2), 16);
			int b2 = Character.digit(src.charAt(i * 2 + 1), 16);

			ret[i] = (byte) ((b1 << 4) ^ b2);
		}
		return ret;
	}

	/**
	 * 删除文件夹下所有内容，包括此文件夹
	 *
	 * @param f 指定文件夹
	 * @throws IOException
	 */
	public static void delDirAll(File f) throws IOException {
		if (!f.exists())//文件夹不存在
			return;

		boolean rslt = true;//保存中间结果

		if (!(rslt = f.delete())) {//先尝试直接删除
			//若文件夹非空。枚举、递归删除里面内容
			File subs[] = f.listFiles();
			for (int i = 0; i <= subs.length - 1; i++) {
				if (subs[i].isDirectory())
					delDirAll(subs[i]);//递归删除子文件夹内容
				rslt = subs[i].delete();//删除子文件夹本身
			}
			rslt = f.delete();//删除此文件夹本身
		}

		if (!rslt)
			throw new IOException("无法删除:" + f.getName());

		return;
	}

	/**
	 * 给定的文件是否可进行预览
	 * @param filename 文件名
	 * @param size 文件大小
	 * @return
	 */


	/**
	 * 关闭输入流
	 *
	 * @param is
	 */
	public static void close(InputStream is) {
		if (is != null) {
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭输入流
	 *
	 * @param reader
	 */
	public static void close(Reader reader) {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭输出流
	 *
	 * @param os
	 */
	public static void close(OutputStream os) {
		if (os != null) {
			try {
				os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 关闭输出流
	 *
	 * @param writer
	 */
	public static void close(Writer writer) {
		if (writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static byte[] long2byte(long md5high, long md5low) {
		byte[] md5 = new byte[16];
		ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putLong(md5high);
		System.arraycopy(buffer.array(), 0, md5, 0, 8);
		buffer = ByteBuffer.allocate(8);
		buffer.putLong(md5low);
		System.arraycopy(buffer.array(), 0, md5, 8, 8);
		return md5;
	}


}
