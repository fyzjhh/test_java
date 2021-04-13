package com.test.algorithm.other;
/*

 */
public class Palindromic {


	public static boolean isPalindrome(String s) {

		for (int i = 0; i < s.length() / 2; i++) {
			if (s.charAt(i) != s.charAt(s.length() - 1 - i)) {
				return false;
			}
		}

		return true;
	}



	public static int test(String str) {
		int len = str.length();
		int ret = 0;
		int i = 0;
		int j = len - 1;

		if (j > i) {
			char c1 = str.charAt(i);
			char c2 = str.charAt(j);

			if (c1 == c2) {
				String substr = str.substring(i + 1, j);
				ret = test(substr);
			} else {
				String substr1 = str.substring(i, j);
				String substr2 = str.substring(i + 1, j + 1);
				int r1 = test(substr1);
				int r2 = test(substr2);
				ret = Math.min(r1, r2) + 1;
			}

		}

		return ret;
	}

	public static void main(String args[]) throws Exception {
		// String[] strs = new String[] { "010100001111", "0101000011111111" };
		// for (int i = 0; i < strs.length; i++) {
		// String s = strs[i];
		// System.out.println(s + " ====");
		// int ret = bs(s);
		// System.out.println(ret);
		// findIndexInStr(s, s.length() / 2);
		// }

		String[] strs = new String[] { "abcdefg", "abcdefgggggfedcb" };
		for (int i = 0; i < strs.length; i++) {
			String s = strs[i];
			System.out.println(s + " ====");
			long ret = test(s);
			System.out.println(ret);

		}
	}
}
