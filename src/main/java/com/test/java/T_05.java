package com.test.java;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.Scanner;

public class T_05 {

	public static String t_05(String s) {
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
			return "-1";
		}

		int num = (num_5 - num_5 % 9);
		String ret = "";
		for (int i = 0; i < num; i++) {
			ret += "5";
		}
		for (int i = 0; i < num_0; i++) {
			ret += "0";
		}
		if (ret.startsWith("0")) {
			ret = "0";
		}
		return ret;

	}

	public static void main(String args[]) throws Exception {

		
		Scanner in = new Scanner(System.in);
		PrintWriter out = new PrintWriter(System.out);

		String len = in.nextLine();
		String s = in.nextLine();
		BigInteger ret = new BigInteger(t_05(s));
		out.print(ret);

		out.flush();

	}
}
