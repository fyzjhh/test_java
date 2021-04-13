package com.jhh.other;

import java.io.PrintWriter;
import java.util.Scanner;

public class Num {
	// ��N��Ϊ���ɸ������ĺͣ��ж����ֲ�ͬ�Ļ��ַ�ʽ�����磺n = 4��{4} {1,3} {2,2} {1,1,2} {1,1,1,1}����5��

	int ret = 0;

	public Num(int ret) {
		this.ret = ret;
	}

	public int getRet() {
		return ret;
	}

	public void setRet(int ret) {
		this.ret = ret;
	}

	public void zuhe(int[] arr, int n) {
		int[] out = new int[n];
		zuheinter(arr, n, 0, out, 0);
	}

	public void zuheinter(int[] arr, int n, int start, int[] out, int index) {

		if (n == 0) {

			int sum = 0;
			for (int i = 0; i < out.length; i++) {
				sum += out[i];
			}
			if (sum == arr[arr.length - 1]) {
//				System.out.println(Arrays.toString(out));
				this.ret++;
			} else {
				// System.out.println("false ");
			}

			return;
		}
		for (int i = 0; i < arr.length; i++) {

			if (index == 0 || arr[i] >= out[index - 1]) {
				out[index] = arr[i];
				zuheinter(arr, n - 1, i + 1, out, index + 1);
			}
		}

	}

	public void test(int num) {

		int[] arr = new int[num];
		for (int i = 0; i < num; i++) {
			arr[i] = i + 1;
		}
		for (int i = 1; i <= num; i++) {
			zuhe(arr, i);
		}
	}

	public static void main(String args[]) throws Exception {

		// int[] strs = new int[] { 4, 6, 11 };
		// for (int i = 0; i < strs.length; i++) {
		// int s = strs[i];
		// Num num = new Num(0);
		// System.out.println(s + " ====");
		// num.test(s);
		// System.out.println(num.ret);
		// }

		Scanner in = new Scanner(System.in);
		PrintWriter out = new PrintWriter(System.out);
		
		Num num = new Num(0);
		int xin = in.nextInt();
		num.test(xin);
		out.print(num.ret);
		
		out.flush();

	}
}
