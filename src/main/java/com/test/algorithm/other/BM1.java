package com.test.algorithm;

import java.util.Arrays;

/**
 * 模式匹配的BM（ Boyer-Moore）算法
 * 
 * @author likebamboo
 * @create 2013-10-16
 * @see http 
 *      ://www.ruanyifeng.com/blog/2013/05/boyer-moore_string_search_algorithm
 *      .html ， http://blog.chinaunix.net/uid-24774106-id-2901288.html
 */
public class BM1 {

	/**
	 * 支持中文匹配
	 */
	private static final int CHAR_MAX = 127;

	/**
	 * 计算坏字符
	 * 
	 * @param pattern
	 */
	private int[] badChar(String pattern) {
		int[] bad = new int[CHAR_MAX];
		for (int i = 0; i < CHAR_MAX; i++) {
			bad[i] = pattern.length();
		}
		for (int i = 0; i < pattern.length(); i++) {
			bad[pattern.charAt(i)] = i;
		}
		return bad;
	}

	/**
	 * 计算后缀 ,suffix数组的定义：suffix[i] = 以i为边界, 与模式串后缀匹配的最大长度
	 * 
	 * @param pattern
	 */
	private int[] suffix(String pattern) {
		int[] suffix = new int[pattern.length()];
		suffix[pattern.length() - 1] = pattern.length();

		for (int i = pattern.length() - 2; i >= 0; i--) {
			int k = i, j = pattern.length() - 1, result = 0;
			while (pattern.charAt(k--) == pattern.charAt(j--) && k > 0) {
				result++;
			}
			suffix[i] = result;
		}
		return suffix;
	}

	/**
	 * 计算好的后缀
	 * 
	 * @param pattern
	 * @return
	 */
	private int[] goodSuffix(String pattern) {
		int[] goodSuffix = new int[pattern.length()];
		int[] suffix = new int[pattern.length()];

		suffix = suffix(pattern);

		for (int i = 0; i < pattern.length(); i++) {
			goodSuffix[i] = pattern.length();
		}

		int j = 0;
		/* 最前和最后的i+1个字符一致 */
		for (int i = pattern.length() - 1; i >= 0; i--) {
			if (suffix[i] == i + 1) { /* consider the pattern "ABCDMNPABCD" */
				for (; j < pattern.length() - 1 - i; j++) {
					// consider the pattern BBBBMNPBBBB
					if (goodSuffix[j] == pattern.length()) {
						goodSuffix[j] = pattern.length() - 1 - i;
					}
				}
			}
		}

		// 处理普通好后缀，既是好后缀长度为（patternLen - 坏字符处）
		for (int i = 0; i < pattern.length() - 1; i++) {
			goodSuffix[pattern.length() - 1 - suffix[i]] = pattern.length() - 1
					- i;
		}
		return goodSuffix;
	}

	/**
	 * BM算法
	 * 
	 * @param text
	 * @param pattern
	 * @return
	 */
	private int pattern(String text, String pattern) {
		int i = 0, j = 0;
		int badSuffix[] = new int[CHAR_MAX];
		int[] goodSuffix = new int[pattern.length()];
		int find = 0;
		int move = 0;

		badSuffix = badChar(pattern);
		goodSuffix = goodSuffix(pattern);

		System.out.println("pattern :" +pattern);
		System.out.println("badSuffix :" +Arrays.toString(badSuffix));
		System.out.println("goodSuffix :" +Arrays.toString(goodSuffix));
		
		while (j <= text.length() - pattern.length()) {
			for (i = pattern.length() - 1; i >= 0
					&& pattern.charAt(i) == text.charAt(j + i); i--) {
			}
			if (i < 0) {
				// FindThePattern(text, pattern, j);
				System.out.println("find a match at index :" + j + "\t"
						+ text.substring(j, j + pattern.length()));
				j += goodSuffix[0];
				find++;
			} else {
				if (i - badSuffix[text.charAt(j + i)] > 0) {
					move = i - badSuffix[text.charAt(j + i)];
				} else {
					move = 1; // 不走回头路
				}
				move = i - badSuffix[text.charAt(j + i)];
				j += Math.max(goodSuffix[i], move);
			}
		}

		return find;
	}

	public static void main(String[] args) {
		// String text = "eeABDCGGDDSGJABDCJPODGABDJD DABDCee";
		// String pattern = "ABDC";

		String text = "1201200001221312300123";
		

		BM1 test = new BM1();
		
		String[] patterns =new String[]{ "1","123","1221","123xxxxx123"};
		for (int i = 0; i < patterns.length; i++) {
			String pattern = patterns[i];
			System.out.println("find " + test.pattern(text, pattern) + "match");
			System.out.println();
		}
	}
}
