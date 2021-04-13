package com.test.algorithm.other;

import java.util.Arrays;
import java.util.Random;

public class Sort {
	private static int getPivot(int begin, int end) {

		return (begin + end) >> 1;
	}

	// һ������
	private static int partition(int[] array, int begin, int end) {
		int pivot = getPivot(begin, end);
		int tmp = array[pivot];
		array[pivot] = array[end];
		while (begin != end) {
			while (array[begin] < tmp && begin < end)
				begin++;
			if (/* array[begin] > tmp && */begin < end) {
				array[end] = array[begin];
				end--;
			}
			while (array[end] > tmp && end > begin)
				end--;
			if (/* array[end] < tmp && */end > begin) {
				array[begin] = array[end];
				begin++;
			}
		}
		// ��ʱ�����±�ָ��ͬһ��Ԫ��.�����λ�����ҷ���(���ε�)
		array[begin] = tmp;
		return begin;
	}

	private static void quicksort(int[] array, int begin, int end) {

		if (end - begin < 1)
			return;
		int pivot = partition(array, begin, end);
		quicksort(array, begin, pivot);
		quicksort(array, pivot + 1, end);
	}

	public static void quicksort(int[] array) {
		quicksort(array, 0, array.length - 1);
	}

	static void bubblesort(Comparable[] data) {
		Comparable temp;
		for (int i = 0; i < data.length; i++) {
			for (int j = data.length - 1; j > i; j--) {
				if (data[j].compareTo(data[j - 1]) < 0) {
					temp = data[j];
					data[j] = data[j - 1];
					data[j - 1] = temp;
				}
			}
		}
	}

	public static void mergeSort(Comparable[] data) {
		Comparable[] temp = new Comparable[data.length];
		mergeSort(data, temp, 0, data.length - 1);

	}

	private static void mergeSort(Comparable[] data, Comparable[] temp,
			int start, int end) {

		int mid = (start + end) / 2;
		System.out.println(start + ", " + mid + ", " + end);
		if (start == end)
			return;

		mergeSort(data, temp, start, mid);
		mergeSort(data, temp, mid + 1, end);
		for (int i = start; i <= end; i++) {
			System.out.println("i=" + i);
			temp[i] = data[i];
		}
		int i1 = start;
		int i2 = mid + 1;
		for (int cur = start; cur <= end; cur++) {
			if (i1 == mid + 1)
				data[cur] = temp[i2++];
			else if (i2 > end)
				data[cur] = temp[i1++];
			else if (temp[i1].compareTo(temp[i2]) < 0)
				data[cur] = temp[i1++];
			else
				data[cur] = temp[i2++];
		}
	}

	public static void insertSort(Comparable[] data) {

		for (int i = 1; i < data.length; i++) {
			Comparable key = data[i];
			int j = 0;
			for (j = i; j > 0 && key.compareTo(data[j - 1]) < 0; j--) {
				data[j] = data[j - 1];
			}
			data[j] = key;
		}
	}

	public static int[] bucketSort(int[] data, int min, int max) {
		int bucketLen = max - min;
		int[] buckets = new int[bucketLen];
		for (int i = 0; i < data.length; i++) {
			buckets[data[i]] = data[i];
		}
		return buckets;
	}

	public static int[] gen(int num, int min, int max) {

		Random random = new Random();
		int[] ret = new int[num];
		for (int i = 0; i < num; i++) {
			int s = random.nextInt(max) % (max - min + 1) + min;
			ret[i] = s;
		}
		return ret;
	}

	public static int[] test_mergesort(int[] arr) {

		int num = arr.length;
		int[] ret = new int[num];
		for (int i = 0; i < num; i++) {

		}
		return ret;
	}
	public static void main(String[] args) {
		int[] ret = gen(20, 100, 999);
		System.out.println(Arrays.toString(ret));
	}

	private static void testsort() {
		int[] a = { 49, 38, 65, 97, 76, 13, 27, 49, 55, 04 };
		int[] b = { 6, 3, 14, 1, 18, 11 };
		// quicksort(b);
		// for (int i = 0; i < b.length; i++)
		// System.out.print(b[i] + ",");
		int[] buckets = bucketSort(b, 1, 20);
		for (int i = 0; i < buckets.length; i++) {
			if (buckets[i] > 0) {
				System.out.println(buckets[i]);
			}
		}
	}
}