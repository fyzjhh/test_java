package com.test.algorithm.other;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("rawtypes")
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
	 * ����һ��0-1��s������Ϊn���±��0��ʼ����һ��λ��k������0<=k<=n, �����Ӵ�s[0..k - 1]�е�0�ĸ������Ӵ�s[k..n -
	 * 1]��1�ĸ�����ȡ� ע�⣺ ��1�� ���k = 0, s[0..k - 1]��Ϊ�մ� ��2�� ���k = n, s[k..n - 1]��Ϊ�մ�
	 * ��3�� ������ڶ��kֵ���䴦�κ�һ�������� ��4�� ���������������kֵ�������-1 Input
	 * ��һ�У�����һ��0-1��S�����Ȳ�����1000000�� Output ��ĿҪ���kֵ Inputʾ�� 01 Outputʾ�� 1
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

	public static void main2(String args[]) throws Exception {
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

	/*
	 * 最长不重复连续子字符串
	 */
	public static void find1(String s) {
		int len = s.length();

		// i表示子字符串的长度
		// j表示可能性
		boolean find = false;
		for (int i = len; i > 0; i--) {
			if (find) {
				break;
			}
			String substr = null;
			for (int j = 0; j <= (len - i); j++) {
				int endidx = j + i;
				substr = s.substring(j, endidx);
				if (hasDup(substr) == false) {
					System.out.println(substr + " NO ");
					find = true;
				} else {
					System.out.println(substr + " YES ");
				}

			}
		}

	}

	public static boolean hasDup(String s) {

		Map<String, Integer> maps = new HashMap<String, Integer>();
		int len = s.length();
		for (int i = 0; i < len; i++) {

			String c = s.substring(i, i + 1);
			if (maps.get(c) == null) {
				maps.put(c, 1);
			} else {
				// maps.put(c, (maps.get(c)+1) );
				return true;
			}
		}
		return false;
	}

	public static int knapsack(int values[], int weights[], int totalWeight) {
		// Get the total number of items.
		// Could be wt.length or val.length. Doesn't matter

		int len1 = values.length;
		int len2 = weights.length;
		int len = len1 > len2 ? len2 : len1;

		int item_num = len;

		int[][] array = new int[item_num + 1][totalWeight + 1];

		// What if the knapsack's capacity is 0 - Set
		// all columns at row 0 to be 0
		for (int col = 0; col <= totalWeight; col++) {
			array[0][col] = 0;
		}

		// What if there are no items at home.
		// Fill the first row with 0
		for (int row = 0; row <= item_num; row++) {
			array[row][0] = 0;
		}

		for (int item = 1; item <= item_num; item++) {
			for (int weight = 1; weight <= totalWeight; weight++) {

				if (weights[item - 1] <= weight) {
					// 如果第 i 个背包不大于总承重，则最优解要么是包含第 i 个背包的最优解，
					// 要么是不包含第 i 个背包的最优解， 取两者最大值，这里采用了分类讨论法
					// 第 i 个背包的重量 iweight 和价值 ivalue
					int val_yes = values[item - 1]
							+ array[item - 1][weight - weights[item - 1]];

					int val_no = array[item - 1][weight];
					array[item][weight] = Math.max(val_yes, val_no);
				} else {
					// 如果第 i 个背包重量大于总承重，则最优解存在于前 i-1 个背包中，
					// 注意：第 i 个背包是 bags[i-1]
					array[item][weight] = array[item - 1][weight];
				}
			}

		}

		// Printing the matrix
		for (int[] rows : array) {
			for (int col : rows) {
				System.out.format("%5d", col);
			}
			System.out.println();
		}

		return array[item_num][totalWeight];
	}

	public static int knapsack1(int values[], int weights[], int totalWeight) {
		// Get the total number of items.
		// Could be wt.length or val.length. Doesn't matter

		int len1 = values.length;
		int len2 = weights.length;
		int len = len1 > len2 ? len2 : len1;

		int item_num = len;
		int max_value = 0;

		boolean find = false;

		for (int i = 1; i <= item_num; i++) {
			if (find) {
				break;
			}

			for (int j = 0; j < weights.length; j++) {

			}
		}

		return 0;

	}

	public static void combination(String[] strs) {

		// String strs[] = { "A", "B", "C", "D", "E" };

		int nCnt = strs.length;

		int nBit = (0xFFFFFFFF >>> (32 - nCnt));

		for (int i = 1; i <= nBit; i++) {
			for (int j = 0; j < nCnt; j++) {
				if ((i << (31 - j)) >> 31 == -1) {
					System.out.print(strs[j]);
				}
			}
			System.out.println("");
		}

	}

	public static void print(List l) {
		for (int i = 0; i < l.size(); i++) {
			int[] a = (int[]) l.get(i);
			for (int j = 0; j < a.length; j++) {
				System.out.print(a[j] + "\t");
			}
			System.out.println();
		}
	}

	/**
	 * 从n个数字中选择m个数字
	 * 
	 * @param a
	 * @param m
	 * @return
	 * @throws Exception
	 */
	public static List combine(int[] a, int m) throws Exception {
		int n = a.length;
		if (m > n) {
			return null;
		}

		List result = new ArrayList();

		int[] bs = new int[n];
		for (int i = 0; i < n; i++) {
			bs[i] = 0;
		}
		// 初始化
		for (int i = 0; i < m; i++) {
			bs[i] = 1;
		}
		boolean flag = true;
		boolean tempFlag = false;
		int pos = 0;
		int sum = 0;
		// 首先找到第一个10组合，然后变成01，同时将左边所有的1移动到数组的最左边
		do {
			sum = 0;
			pos = 0;
			tempFlag = true;
			result.add(print(bs, a, m));

			for (int i = 0; i < n - 1; i++) {
				if (bs[i] == 1 && bs[i + 1] == 0) {
					bs[i] = 0;
					bs[i + 1] = 1;
					pos = i;
					break;
				}
			}
			// 将左边的1全部移动到数组的最左边

			for (int i = 0; i < pos; i++) {
				if (bs[i] == 1) {
					sum++;
				}
			}
			for (int i = 0; i < pos; i++) {
				if (i < sum) {
					bs[i] = 1;
				} else {
					bs[i] = 0;
				}
			}

			// 检查是否所有的1都移动到了最右边
			for (int i = n - m; i < n; i++) {
				if (bs[i] == 0) {
					tempFlag = false;
					break;
				}
			}
			if (tempFlag == false) {
				flag = true;
			} else {
				flag = false;
			}

		} while (flag);
		result.add(print(bs, a, m));

		return result;
	}

	private static int[] print(int[] bs, int[] a, int m) {
		int[] result = new int[m];
		int pos = 0;
		for (int i = 0; i < bs.length; i++) {
			if (bs[i] == 1) {
				result[pos] = a[i];
				pos++;
			}
		}
		return result;
	}

	public static void main(String args[]) throws Exception {
		// String[] strs = new String[] { "123", "1221", "123xxxxx123" };
		// for (int i = 0; i < strs.length; i++) {
		// String s = strs[i];
		// System.out.println(s + " ====");
		// find1(s);
		// System.out.println();
		// }

		// int weights[] = { 5, 4, 6, 3, 7, 8, 10 };
		// int values[] = { 10, 40, 30, 50, 45, 70, 80 };
		// int totalWeight = 20;

		// int weights[] = { 1, 2, 3, 4, 5 };
		// int values[] = { 10, 40, 70, 60, 80 };
		// int totalWeight = 7;
		//
		// System.out.println(knapsack(values, weights, totalWeight));

//		String[] strs = new String[] { "A", "B", "C" };
//		combination(strs);

		int weights[] = { 1, 2, 3, 4, 5 };
		List l =combine(weights, 3);
		print(l);
		
	}
}
