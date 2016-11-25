package com.test.algorithm.math;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;

public class Sort {
	private static boolean isPrime(long num) {
		boolean ret = true;
		if (num <= 1) {
			ret = false;
		}
		long sprtNum = (long) Math.sqrt(num);
		for (long i = 2; i <= sprtNum; i++) {
			if (num % i == 0) {
				ret = false;
				break;
			}
		}
		return ret;
	}

	private static List<Long> get_primes_less_than_n(long num) {
		List<Long> ret = new ArrayList<Long>();
		if (num < 5) {
			return ret;
		}

		ret.add(2L);
		ret.add(3L);

		int k = -1;
		for (long n = 5; n < num;) {
			k++;
			for (int i = 0; i < ret.size(); i++) {
				if (ret.get(i) * ret.get(i) > n) {
					ret.add(n);
					break;
				} else if (n % ret.get(i) == 0) {
					break;
				}
			}
			if (k % 2 == 0)
				n += 2;
			else
				n += 4;
		}

		return ret;
	}

	public static void main(String[] args) {
		// test_isPrime();
		test_get_primes_less_than_n();
	}

	public static long[] gen(int num, long min, long max) {

		Random random = new Random();
		long[] ret = new long[num];
		for (int i = 0; i < num; i++) {
			long s = Math.abs(random.nextLong()) % (max - min + 1) + min;
			ret[i] = s;
		}
		return ret;
	}

	private static void test_get_primes_less_than_n() {
		// long[] a = gen(5, 1, 100);
		long[] a = { 10, 100, 1000, 10000 };
		for (int i = 0; i < a.length; i++) {

			long start = System.currentTimeMillis();
			List<Long> rets = get_primes_less_than_n(a[i]);
			long end = System.currentTimeMillis();
			long spenttime = end - start;

			System.out.println(addComma(a[i], 3) + "\t" + rets.size() + "\t"
					+ Arrays.toString(rets.toArray()));
		}
	}

	private static String addComma(Object o, int num) {
		String str = o.toString();
		// 将传进数字反转
		String reverseStr = new StringBuilder(str).reverse().toString();

		String strTemp = "";
		for (int i = 0; i < reverseStr.length(); i++) {
			if (i * num + num > reverseStr.length()) {
				strTemp += reverseStr.substring(i * num, reverseStr.length());
				break;
			}
			strTemp += reverseStr.substring(i * num, i * num + num) + ",";
		}
		// 将最后一个逗号去除
		if (strTemp.endsWith(",")) {
			strTemp = strTemp.substring(0, strTemp.length() - 1);
		}

		return new StringBuilder(strTemp).reverse().toString();
	}

	private static void test_isPrime() {
		// long[] a = gen(50, 1000, 1000000000);
		long[] a = gen(50, 1000, 10000000);
		for (int i = 0; i < a.length; i++) {
			long start = System.currentTimeMillis();
			boolean ret = isPrime(a[i]);
			long end = System.currentTimeMillis();
			long spenttime = end - start;

			System.out.println(addComma(a[i], 3) + "\t" + spenttime + "\t"
					+ ret);
		}
	}
}