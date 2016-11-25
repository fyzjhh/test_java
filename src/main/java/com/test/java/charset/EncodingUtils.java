package com.test.java.charset;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * 
 * @author wangchen
 * 
 * 
 */

public final class EncodingUtils {

	private static final String DIGEST_KEY_STR = "3go8&$8*3*3h0k(2)2";

	public static final String DIGEST_KEY_STR_FileStation = "2c250582181a4a52d044275a0c518ff5";

	private static byte[] digestKey;

	private static final int DIGEST_KEY_LEN = DIGEST_KEY_STR.length();

	private static final char[] BCD_LOOKUP = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	private EncodingUtils() {

	}

	static {

		try {

			digestKey = DIGEST_KEY_STR.getBytes("ISO_8859_1");

		} catch (UnsupportedEncodingException e) {

			e.printStackTrace();

		}

	}

	private static String base64edDigest(String src, byte[] salt, int keyLen) {

		return base64Enc(getMD5Digest(simpleKeyXOR(src, salt, keyLen)));

	}

	/**
	 * 
	 * ����base64����
	 * 
	 * 
	 * 
	 * @param bsrc
	 * 
	 *            ����������
	 * 
	 * @return
	 */

	public static String base64Enc(byte[] bsrc) {

		String retVal = (new BASE64Encoder()).encode(bsrc);

		retVal = replaceSpecChars(retVal);

		return retVal;

	}

	/**
	 * 
	 * ����Base64����
	 * 
	 * 
	 * 
	 * @param src
	 * 
	 *            ����������
	 * 
	 * @return
	 */

	public static byte[] base64Dec(String src) {

		try {

			return (new BASE64Decoder()).decodeBuffer(src);

		} catch (IOException e) {

			e.printStackTrace();

			return null;

		}

	}

	/**
	 * 
	 * ��ȡbase64�����������ִ�
	 * 
	 * 
	 * 
	 * @param src
	 * 
	 *            �������ִ�
	 * 
	 * @return
	 */

	public static String base64edDigest(String src) {

		return base64edDigest(src, digestKey, DIGEST_KEY_LEN);

	}

	/**
	 * 
	 * ��������ֵת����ʮ�������ַ�����ʾ
	 * 
	 * 
	 * 
	 * @param bcd
	 * 
	 *            ������ֵ
	 * 
	 * @return
	 */

	public static final String bytesToHexStr(byte[] bcd) {

		StringBuilder sb = new StringBuilder(bcd.length * 2);

		for (int i = 0; i < bcd.length; i++) {

			sb.append(BCD_LOOKUP[(bcd[i] >>> 4) & 0x0f]);

			sb.append(BCD_LOOKUP[bcd[i] & 0x0f]);

		}

		return sb.toString();

	}

	/**
	 * 
	 * ��ʮ�������ַ�����ʾת����������ֵ
	 * 
	 * 
	 * 
	 * @param s
	 * 
	 *            ʮ�������ַ���
	 * 
	 * @return
	 */

	public static final byte[] hexStrToBytes(String s) {

		byte[] bytes;

		bytes = new byte[s.length() / 2];

		for (int i = 0; i < bytes.length; i++) {

			bytes[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2),
					16);

		}

		return bytes;

	}

	private static String replaceSpecChars(String in) {

		char[] ca = in.toCharArray();

		for (int i = 0; i < ca.length; i++) {

			if (ca[i] == '/')

				ca[i] = '_';

			else if (ca[i] == '+')

				ca[i] = '-';

		}

		return new String(ca);

	}

	private static byte[] simpleKeyXOR(String src, byte[] key, int keyLen) {

		byte[] bsrc = null;

		try {

			bsrc = src.getBytes("ISO_8859_1");

			for (int i = 0; i < bsrc.length; i++) {

				bsrc[i] = (byte) (bsrc[i] ^ key[i % keyLen]);

			}

		} catch (UnsupportedEncodingException e) {

			e.printStackTrace();

			return null;

		}

		return bsrc;

	}

	/**
	 * 
	 * ��ȡ���ݵ�md5ֵ
	 * 
	 * 
	 * 
	 * @param bsrc
	 * 
	 *            md5����Դ
	 * 
	 * @return
	 */

	private static byte[] getMD5Digest(byte[] bsrc) {

		try {

			MessageDigest alg = MessageDigest.getInstance("MD5");

			alg.update(bsrc);

			return alg.digest();

		} catch (NoSuchAlgorithmException e) {

			e.printStackTrace();

			return null;

		}

	}

	/**
	 * 
	 * ���ִ���MD5ժҪ������Hex������ִ�
	 * 
	 * 
	 * 
	 * @param src
	 * 
	 *            ��������ִ�
	 * 
	 * @return
	 */

	public static String getMD5Content(String src) {

		try {

			return bytesToHexStr(getMD5Digest(src.getBytes("ISO_8859_1")));

		} catch (UnsupportedEncodingException e) {

			return null;

		}

	}

	/**
	 * 
	 * ���ִ���MD5ժҪ������base64������ִ�
	 * 
	 * 
	 * 
	 * @param src
	 * 
	 *            ��������ִ�
	 * 
	 * @return
	 */

	public static String getMD5ContentBase64(String src) {

		try {

			return base64Enc(getMD5Digest(src.getBytes("ISO_8859_1")));

		} catch (UnsupportedEncodingException e) {

			return null;

		}

	}

}
