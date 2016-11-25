package com.test.al;

public class AlString {

	public static int binarySearch(String str, char des) {

		int low = 0;
		int high = str.length() - 1;
		while (low <= high) {
			int middle = (low + high) / 2;
			if (des == str.charAt(middle)) {
				return middle;
			} else if (des < str.charAt(middle)) {
				high = middle - 1;
			} else {
				low = middle + 1;
			}
		}
		return -1;
	}

	/*
	 * 
	 * 给定一个0-1串s，长度为n，下标从0开始，求一个位置k，满足0<=k<=n, 并且子串s[0..k - 1]中的0的个数与子串s[k..n -
	 * 1]中1的个数相等。 注意： （1） 如果k = 0, s[0..k - 1]视为空串 （2） 如果k = n, s[k..n - 1]视为空串
	 * （3） 如果存在多个k值，输处任何一个都可以 （4） 如果不存在这样的k值，请输出-1 Input
	 * 就一行，包含一个0-1串S，长度不超过1000000。 Output 题目要求的k值 Input示例 01 Output示例 1
	 */

	public static int bs(String str) {

		int low = 0;
		int len = str.length();
		int high = str.length();
		while (low <= high) {
			int middle = (low + high) / 2;
			String leftStr = str.substring(0, middle);
			String rightStr = str.substring(middle, len);
			int leftnum = findCharNum(leftStr, '0');
			int rightnum = findCharNum(rightStr, '1');
			if (leftnum == rightnum) {
				return middle;
			}
			if (leftnum < rightnum) {
				low = middle + 1;
			}
			if (leftnum > rightnum) {
				high = middle - 1;
			}
		}
		return -1;
	}

	public static boolean findIndexInStr(String s, int idx) {
		int len = s.length();

		String leftStr = s.substring(0, idx);
		String rightStr = s.substring(idx, len);
		if (findCharNum(leftStr, '0') == findCharNum(rightStr, '1')) {
			System.out.println(idx);
			return true;
		} else {
			int newleft_idx = idx / 2;
			if (newleft_idx > 1) {
				if (findIndexInStr(s, newleft_idx)) {
					return true;
				}
			}
			int newright_idx = idx + idx / 2;
			if (newright_idx < s.length()) {
				if (findIndexInStr(s, newright_idx)) {
					return true;
				}
			}
		}
		return false;

	}

	public static int findCharNum(String s, char c) {

		int ret = 0;
		int len = s.length();
		for (int i = 0; i < len; i++) {
			if (c == (s.charAt(i))) {
				ret++;
			}
		}
		return ret;
	}

	public static long t_05(String s) {
		char[] ch = s.toCharArray();
		int len = ch.length;
		int num_5 = 0;
		int num_0 = 0;

		for (int i = 0; i < len; i++) {
			if ('5' == ch[i]) {
				num_5++;
			}
			if ('0' == ch[i]) {
				num_0++;
			}
		}
		if (num_0 == 0) {
			return -1L;
		}

		int num = (num_5 - num_5 % 9);
		String ret = "";
		for (int i = 0; i < num; i++) {
			ret += "5";
		}
		for (int i = 0; i < num_0; i++) {
			ret += "0";
		}

		return new Long(ret);

	}

	public static void main(String args[]) throws Exception {
//		String[] strs = new String[] { "010100001111", "0101000011111111" };
//		for (int i = 0; i < strs.length; i++) {
//			String s = strs[i];
//			System.out.println(s + " ====");
//			int ret = bs(s);
//			System.out.println(ret);
//			findIndexInStr(s, s.length() / 2);
//		}

		String[] strs = new String[] { "5050", "50505050" , "5" , "5050505055555" };
		for (int i = 0; i < strs.length; i++) {
			String s = strs[i];
			System.out.println(s + " ====");
			long ret = t_05(s);
			System.out.println(ret);
			
		}
	}
}
