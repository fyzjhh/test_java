package com.test.algorithm.other;

import java.io.BufferedInputStream;
import java.util.Arrays;
import java.util.Scanner;

public class MSTPrim {

	private int[][] arr;// �ڽӾ���
	private boolean flag[]; // ������ǽڵ�i�Ƿ��Ѽ��뵽MST
	private int n; // ������
	private int sum;// ��СȨֵ��
	static final int maxInt = Integer.MAX_VALUE;

	public MSTPrim(int[][] arr, int n) {
		this.arr = arr;
		this.n = n;
		flag = new boolean[n];
	}

	public static void main(String[] args) {
		Scanner s = new Scanner(new BufferedInputStream(System.in));

		int n = 7;
		int arr[][] = { { maxInt, 28, maxInt, maxInt, maxInt, 10, maxInt },
				{ 28, maxInt, 16, maxInt, maxInt, maxInt, 14 },
				{ maxInt, 16, maxInt, 12, maxInt, maxInt, maxInt },
				{ maxInt, maxInt, 12, maxInt, 22, maxInt, 18 },
				{ maxInt, maxInt, maxInt, 22, maxInt, 25, 24 },
				{ 10, maxInt, maxInt, maxInt, 25, maxInt, maxInt },
				{ maxInt, 14, maxInt, 18, 24, maxInt, maxInt } };
		System.out.println(new MSTPrim(arr, n).prim());

	}

	public int prim() {
		sum = 0;
		flag[0] = true; // ѡȡ��һ���ڵ�
		int mst[] = new int[n];// �洢��СȨֵ�ߵ����
		Arrays.fill(mst, 0);// ��СȨֵ�ߵ����Ĭ��Ϊ0

		for (int k = 1; k < n; k++) { // ѭ��n-1��
			int min = maxInt, min_i = 0;
			for (int i = 0; i < n; i++) {// ѡһ��Ȩֵ��С�ġ�
				if (!flag[i] && arr[0][i] < min) {
					min = arr[0][i];
					min_i = i;
				}
			}

			flag[min_i] = true; // ����
			System.out.print("��" + mst[min_i] + "-" + min_i);

			for (int i = 0; i < n; i++) { // ����
				if (!flag[i] && arr[0][i] > arr[min_i][i]) {// ��ͬһ��δ����������Ѽ���������ӣ�ȡȨֵ��С�ġ�
					arr[0][i] = arr[min_i][i];
					mst[i] = min_i;// ������СȨֵ�ߵ����
				}

			}
			System.out.println("--" + arr[0][min_i]);
			sum += arr[0][min_i];// ����Ȩֵ
		}

		return sum;

	}

}
