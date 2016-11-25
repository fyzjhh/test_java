package com.test.java;
import java.io.PrintWriter;
import java.util.Scanner;

public class PerfectNum {

	public static boolean test(String s) {

		int val = Integer.parseInt(s);
		for (int i = 0; i < s.length(); i++) {
			int subval = Integer.parseInt(s.substring(i, i+1));
			if (val % subval != 0) {
				return false;
			}
		}
		return true;

	}


	public static void main(String args[]) throws Exception {

		// int[] strs = new int[] { 4,6,11 };
		// for (int i = 0; i < strs.length; i++) {
		// int s = strs[i];
		// Num1 num = new Num1(0);
		// System.out.println(s + " ====");
		// num.test(s);
		// System.out.println(num.ret);
		// }

		Scanner in = new Scanner(System.in);
		PrintWriter out = new PrintWriter(System.out);

		while(true){
		String s = in.nextLine();
		out.print(s+"\t"+ test(s));
		out.flush();
		}
	}
}
