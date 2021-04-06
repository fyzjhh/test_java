package com.test.algorithm;

import java.util.Arrays;

public class PaiLie {

	public void runPermutation(int[] a, int n) {

		if (null == a || a.length == 0 || n <= 0 || n > a.length)
			return;

		int[] b = new int[n];// 辅助空间，保存待输出组合数
		getCombination(a, n, 0, b, 0);
	}

	public void getCombination(int[] a, int n, int begin, int[] b, int index) {

		if (n == 0) {// 如果够n个数了，输出b数组

			getAllPermutation(b, 0);// 得到b的全排列
			System.out.println(Arrays.toString(b));
			return;
		}

		for (int i = begin; i < a.length; i++) {

			b[index] = a[i];
			getCombination(a, n - 1, i + 1, b, index + 1);
		}

	}

	public void getAllPermutation(int[] a, int index) {

		/* 与a的元素个数相同则输出 */
		if (index == a.length - 1) {
			System.out.println(Arrays.toString(a));
			return;
		}

		for (int i = index; i < a.length; i++) {

			swap(a, index, i);
//			System.out.println("swaped forward " + index + " " + i + " "
//					+ Arrays.toString(a));
			getAllPermutation(a, index + 1);
			swap(a, index, i);
//			System.out.println("swaped backward " + index + " " + i + " "
//					+ Arrays.toString(a));
		}
	}

	public void swap(int[] a, int i, int j) {

		int temp = a[i];
		a[i] = a[j];
		a[j] = temp;
	}

	public static void main(String[] args) {

		PaiLie robot = new PaiLie();

		int[] a = { 1, 2, 3, 4 };
		int n = 3;
		robot.runPermutation(a, n);

	}

}