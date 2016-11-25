package com.test.java;
public class T_BinFind {

	public static int test(int[] arr, int des) {

		int ret = -1;

		int low = 0;
		int high = arr.length - 1;
		int mid = 0;
		
		while (low <= high) {
			mid = (low + high) / 2;
			if (arr[mid] == des) {
				return mid;
			}
			if (arr[mid] < des) {
				low = mid + 1;
			}
			if (arr[mid] > des) {
				high = mid - 1;
			}
		}

		return ret;
	}

	public static void main(String args[]) throws Exception {

		int[] array = new int[] { 1, 3, 6, 7, 10, 22, 23, 25 };
		System.out.println(test(array,-12));
		System.out.println(test(array,1));
		System.out.println(test(array,7));
		System.out.println(test(array,8));
		System.out.println(test(array,25));
		System.out.println(test(array,35));

	}
}
