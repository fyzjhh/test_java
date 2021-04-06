package com.test.algorithm;

import java.io.BufferedInputStream;
import java.util.Arrays;
import java.util.Scanner;

public class MSTPrim {

	private int[][] arr;// 邻接矩阵
	private boolean flag[]; // 用来标记节点i是否已加入到MST
	private int n; // 顶点数
	private int sum;// 最小权值和
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
		flag[0] = true; // 选取第一个节点
		int mst[] = new int[n];// 存储最小权值边的起点
		Arrays.fill(mst, 0);// 最小权值边的起点默认为0

		for (int k = 1; k < n; k++) { // 循环n-1次
			int min = maxInt, min_i = 0;
			for (int i = 0; i < n; i++) {// 选一条权值最小的。
				if (!flag[i] && arr[0][i] < min) {
					min = arr[0][i];
					min_i = i;
				}
			}

			flag[min_i] = true; // 加入
			System.out.print("边" + mst[min_i] + "-" + min_i);

			for (int i = 0; i < n; i++) { // 更新
				if (!flag[i] && arr[0][i] > arr[min_i][i]) {// 若同一个未加入点与多个已加入点相连接，取权值较小的。
					arr[0][i] = arr[min_i][i];
					mst[i] = min_i;// 更新最小权值边的起点
				}

			}
			System.out.println("--" + arr[0][min_i]);
			sum += arr[0][min_i];// 加上权值
		}

		return sum;

	}

}
