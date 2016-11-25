package com.test.java;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

public class TestBase64 {
	static BASE64Encoder encoder = new BASE64Encoder();
	static BASE64Decoder decoder = new BASE64Decoder();

	public static void main(String[] args) throws Exception {
		// D7F6B2BBB5BD0A8A859D4CB0C9
		String s = "D7F6B2BBB5BD0A8A859D4CB0C9";
		String tmp = "";
		int vi = 0;
		int len = s.length() / 2;
		byte[] bs = new byte[len];
		for (int i = 0; i < len; i++) {
			tmp = s.substring(i * 2, i * 2 + 2);
			vi = Integer.parseInt(tmp, 16);
			bs[i] = (byte) vi;
		}

		System.out.println(encode64(bs));
		// System.out.println(test_charset(new byte[] { 71, 114, 97, 99, 101,
		// -45,
		// -59, -47, -59 }, "gbk"));
		// write_char();

		// String s;
		// byte[] byte_arr;
		// String base64_str;
		// String p_str;
		//
		// s = "一脸无辜";
		// byte_arr = s.getBytes("gbk");
		// base64_str = encode64(byte_arr);
		// p_str = s + "," + Arrays.toString(byte_arr) + "," + base64_str;
		// System.out.println(p_str);
		//
		// base64_str = "0rvBs87eubw=";
		// byte_arr = decode64(base64_str);
		// s = new String(byte_arr, "gbk");
		// p_str = s + "," + Arrays.toString(byte_arr) + "," + base64_str;
		// System.out.println(p_str);

		System.out.println("====success====");
	}

	// BASE64解码
	public static byte[] decode64(String s) {
		if (s == null)
			return null;

		try {
			byte[] b = decoder.decodeBuffer(s);
			return b;
		} catch (Exception e) {
			return null;
		}
	}

	// BASE64 编码
	public static String encode64(byte[] bs) {
		if (bs == null)
			return null;
		return encoder.encode(bs);
	}

}
