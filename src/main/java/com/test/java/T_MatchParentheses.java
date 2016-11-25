package com.test.java;
import java.io.PrintWriter;
import java.util.Scanner;

class Stack {

	public static void push(char[] arr, int topOfStack, char x) {

		arr[topOfStack] = x;
	}

	public static char pop(char[] arr, int topOfStack) {
		char x = arr[topOfStack];
		return x;
	}

}

public class T_MatchParentheses {

	public static void find1(String s) {
		int len = s.length();

		boolean find = false;

		int max_len = 0;
		int max_cnt = 0;
		for (int i = len; i > 0; i--) {
			if (find) {
				break;
			}

			String substr = null;
			for (int j = 0; j <= (len - i); j++) {
				int endidx = j + i;
				substr = s.substring(j, endidx);
				if (test(substr) == false) {
					// System.out.println(substr + " NO ");
				} else {
					find = true;
					max_len = substr.length();
					j = j + max_len;
					max_cnt++;
				}
			}
		}
		if (find) {
			System.out.println(max_len + " " + max_cnt);
		}else{
			System.out.println(0 + " " + 1);
		}
			
	}

	public static boolean test(String s) {

		char[] input = s.toCharArray();
		char[] stack = new char[input.length];
		int topOfStack = -1;
		boolean result = true;

		for (int i = 0; i < input.length; i++) {
			switch (input[i]) {
			case '(':
				Stack.push(stack, ++topOfStack, input[i]);
				break;
			default:
				if (topOfStack == -1) {
					result = false;
					break;
				} else {
					char top = Stack.pop(stack, topOfStack--);

					if (input[i] == ')' && top != '(') {
						result = false;
					}
					break;
				}
			}
		}
		if (topOfStack > -1) {
			return false;
		} else {

			return (result);
		}

	}

	public static void main(String args[]) throws Exception {

		Scanner in = new Scanner(System.in);
		PrintWriter out = new PrintWriter(System.out);

		 String s = in.nextLine();
//		String s = ")((())))(()())";
		find1(s);
		out.println();

		out.flush();

	}
}
