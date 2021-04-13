package com.test.algorithm.other;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

public class ExternalSort {
	public static final int CAPACITY = 3;

	public static void sort(File in, File out) throws IOException {
		File[][] files = {
				{ new File(in.getPath() + ".a0"),
						new File(in.getPath() + ".a1") },
				{ new File(in.getPath() + ".b0"),
						new File(in.getPath() + ".b1") } };
		split(in, files[0]);
		int runLength = CAPACITY;
		int i = 0;
		while (merge(files[i], files[1 - i], runLength)) {
			i = 1 - i;
			runLength *= 2;
		}
		if (!files[1 - i][0].renameTo(out))
			System.out.println("Rename file error!");
		for (int k = 0; k < 2; k++)
			for (int j = 0; j < 2; j++) {
				if (1 - i == k && j == 0)
					continue;
				if (!files[k][j].delete())
					System.out.println("Delete " + files[k][j].getPath()
							+ " error!");
			}
	}

	protected static void split(File in, File[] out) throws IOException {
		Scanner input = new Scanner(new FileInputStream(in));
		PrintWriter[] output = { new PrintWriter(out[0]),
				new PrintWriter(out[1]) };
		int i = 0;
		while (input.hasNext()) {
			String[] run = new String[CAPACITY];
			for (int j = 0; (input.hasNext() && (j < CAPACITY)); j++) {
				run[j] = input.nextLine();
			}
			int len = Strings.sort(run);// ���ֵ����С��������
			for (int j = 0; j < len; j++) {
				output[i].println(run[j]);
			}
			/*
			 * / output[i].flush(); /
			 */
			i = 1 - i;
		}
		output[0].close();
		output[1].close();
		input.close();
	}

	protected static boolean merge(File[] in, File[] out, int runLength)
			throws IOException {
		/**
		 * Merge runs, of maximum length runLength, in the files in[0] and
		 * in[1], into runs twice this length in out[0] and out[1]. Return true
		 * if both output files are needed.
		 */
		boolean bothOutputsUsed = false;
		Scanner[] input = { new Scanner(new FileInputStream(in[0])),
				new Scanner(new FileInputStream(in[1])) };
		PrintWriter[] output = { new PrintWriter(out[0]),
				new PrintWriter(out[1]) };
		int i = 0;
		while ((input[0].hasNext() || input[1].hasNext())) {
			ExternalSortRun[] runs = {
					new ExternalSortRun(input[0], runLength),
					new ExternalSortRun(input[1], runLength) };
			if (i == 1) {
				bothOutputsUsed = true;
			}
			while ((runs[0].hasNext() || runs[1].hasNext())) {
				if (!runs[1].hasNext()
						|| (runs[0].hasNext() && runs[0].peek().compareTo(
								runs[1].peek()) < 0))
				/**
				 * �ø��ӵĲ������ڴ���������һ������ĩβ�������
				 * ���runs[1]������ĩβ�����Ǿ�ֱ�Ӵ�runs[0]�л�ȡ��һ��Ԫ�أ���֮��Ȼ��
				 */
				{
					output[i].println(runs[0].next());
				} else {
					output[i].println(runs[1].next());
				}
				/*
				 * / output[i].flush(); /
				 */
			}
			i = 1 - i;
		}
		output[0].close();
		output[1].close();
		input[0].close();
		input[1].close();
		/** Don't forget to close the iostream. */
		return bothOutputsUsed;
	}

	public static void main(String[] args) throws IOException {
		sort(new File(args[0]), new File(args[1]));
	}
}

class Strings {
	public static int sort(String[] str_s) {
		int len = 0;
		for (int i = 0; i < str_s.length; i++)
			if (str_s[i] != null)
				len++;
		// int len=str_s.length;
		/**
		 * this may cause a null pointer exception if Array str_s[] is not
		 * completely initialized.
		 */
		String temp;
		for (int i = 0; i < len - 1; i++)
			for (int j = 0; j < len - i - 1; j++) {
				if ((str_s[j].compareTo(str_s[j + 1])) > 0) {
					temp = str_s[j];
					str_s[j] = str_s[j + 1];
					str_s[j + 1] = temp;
				}
			}
		return len;
	}
}