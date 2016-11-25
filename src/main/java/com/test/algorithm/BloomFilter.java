package com.test.algorithm;

import java.util.BitSet;

/**
 * 
 * @author xkey
 */
public class BloomFilter {

	private static final int DEFAULT_SIZE = 1 << 23;// 布隆过滤器的比特长度
	private static final int[] seeds = { 3, 5, 7, 11, 13, 31, 37, 61 };// 这里要选取质数，能很好的降低错误率
	private static BitSet bits = new BitSet(DEFAULT_SIZE);

	public static boolean testValue(String value) {
		if (value == null)
			return false;
		boolean ret = true;
		for (int i = 0; i < seeds.length; i++) {
			ret = ret && bits.get(hash(value, seeds[i]));
		}
		return ret;
	}

	public static void main(String[] args) {
		String[] add_values = new String[] { "ztLKx+j3xbXL+9Kvbw==",
				"0KG6rrqu2Lw=", "xP3N+7XE19PB+g==", "oaKhoqGistDJy6GioaKhog==",
				"0rm0s6GiudG4vrTl2Lw=", "ttTD5sirysdTQqH9", "0arG+A==",
				"tPPHx7Wl1tAtzOzPws7etdA=", "0u6zvy3U9A==", "0KHC7bjnMDAwMA==",
				"ye3PtaHzyP3Hp7PosK7YvA==", "xMfSu8rAoaPDzs/r", "2LzQobnP19M=",
				"0MLK1tKyxNzN5sj9ufo=", "sanX37/xzb0xMbrF", "8bKyu/Gy2Lw=",
				"ztKyu9DQxOPJzw==", "zfXJ3Of5", "09DKwsO7ysJO0rvPwg==",
				"obDQodKv2Lx5aci7ut3Xpw==", "0LDW7Lfj", "0vXM7GNsb3VkeQ==",
				"tbnI+Lrcsru07Q==", "TWOhqMThwuo=", "ob7KtcGm1qTD99K7x9Chvw==",
				"oaLV4s/C1ea1xIL7wcs=", "c2FkZmFzZnNkZmFkZmE=",
				"zfiwyc2oz/yho82oxvDAtKOh", "uPjBptChusVCQg==",
				"wM/X07+zy8DE48PHtLXFo7XE", "z+C1sb/g",
				"obK2vqGzdmVyX8nL8auk6Q==", "bG9ubHlyag==",
				"Xi5eSlRMsLLA1svAXi5e", "yPTS/sj0z9a1xNCh1rTXxaGj", "a2dqaXc=",
				"x8XE0A==", "x+m1vcnutKawrrW91+668w==", "uOfQpsHLoaOho6Gj",
				"2K1fX190aW5n", "Li4u9+j36y4uLg==",
				"f39/f39/f39/f8DryMvDzn+h5Q==", "1/zX/tf81/7X/Nf+1/zX/Q==",
				"wfqjqdHV", "wumx1Mqyw7TH6b/2", "ztK1xM7CyOGhosTjtq4=",
				"odG1pLWkodE=", "LS0tvsO36rXEsK4=", "0f3R3ruosd+/46Gi",
				"zLjMuMfpLA==", "uPi458ztvcXWurfs", "y+ax483mzeYtLTAuMA==",
				"uenAtN+5", "1rvOqqeiybGnog==", "sMHK08zh",
				"vtmxqLXnu7CjujEwMDg2", "1+/M7Mq5Li4=",
				"tPK1xMTjTba8srvIz7XDoaM=", "09DSu9bWxqvWtL3Q0a27t6TF",
				"t7G7qsLkvqEuLi6kpOnk0tHKxQ==", "xbzNu8i7",
				"0ru1ttK7uPbQocXz09F8", "yMLIwjAwMA==", "w865+jUyMDA=",
				"sKOyvMzY2LzPo7Ku", "z7S6w8TjtcS+1buouPi457Ld",
				"svjD4NPa0rnN7Q==", "yP25+ti81cXS5rTv", "t+fB96Sy0KHN9dfT",
				"1/7X/tf+1/7YvMSq0KHE7w==", "u8PP6y4uzrTAtA==",
				"wK+xytCh0MLO0rCuxOM=", "sNTB6KOs", "ZmFzZGYxc2Fk",
				"ztKju6Gioa7H8w==", "YXJwdA==", "VHNupeG2vtKp",
				"safXxcTj0rK+zbXDwOQ=", "2K+xu8Wwx6ew2di81tWzycql",
				"RGV2aWzA9g==", "f8mxuPbIy9XmtcS6w8TR", "vsa57dChsLI=",
				"stPAw7XE0KGyy8Tx", "MzYxoeOkztTLtq/QrA==",
				"oJTYvIHDx5bEo7fCpM7Ev7Hq", "LbnixsfB2w==",
				"ztK0wX7E48yrv9O1+cHLIX5+", "0tTVvda51b3Yr9W9utuzycnL",
				"VGlGRmFuWQ==", "y8DJ8bXEyMs=", "x+m7sH9/f39Fcm9zaW9u2Lw=",
				"VE9UstDp5A==", "z/7D99au1b0=", "x+nIpKHFyP0=",
				"tqy5z7rNzve5zw==", "ztKyu7vhxOO90Lu9xOPC6mI=",
				"tc219+zhjKia8OaK", "18+wwg==", "urm6ubTzyqa4tQ==",
				"uMm19M7SuPbE47XEw87P6y4=" };
		for (int i = 0; i < add_values.length; i++) {
			addValue(add_values[i]);
		}

		String[] test_values = new String[] { "xkeyideal@gmail.com",
				"zfiwyc2oz/yho82oxvDAtKOh", "uPjBptChusVCQg==",
				"wM/X07+zy8DE48PHtLXFo7XE", "z+C1sb/g" };
		String r;
		boolean ret;
		for (int i = 0; i < test_values.length; i++) {
			ret = testValue(test_values[i]);
			if (ret) {
				addValue(test_values[i]);
			}
			r = test_values[i] + "\t" + ret;
			System.out.println(r);
		}

	}

	private static void addValue(String value) {
		if (value != null) {
			for (int i = 0; i < seeds.length; i++) {
				bits.set(hash(value, seeds[i]));
			}
		}
	}

	public static int hash(String value, int seed) {
		int result = 0;

		int len = value.length();
		for (int i = 0; i < len; i++) {
			result = seed * result + value.charAt(i);
		}
		return (DEFAULT_SIZE - 1) & result;
	}
}
