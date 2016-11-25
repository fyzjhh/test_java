package com.test.java;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;

@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
public class TestJava {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		DateFormat spacedatetimeformat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		Date d = spacedatetimeformat.parse("2016-09-01 10:50:30");
		Calendar gc = GregorianCalendar.getInstance();
		gc.setTime(d);

		System.out.println(gc.getTimeZone());
		System.out.println(gc.getTime().toGMTString());
		/*
		 * boolean m =
		 * "tab_item_sell".matches(" tab_item_sell|tab_char_create"); //
		 * String[] s = new String[] { "12,13,14,15,13,15" }; //
		 * get_arrangement(s);
		 * 
		 * // saolei(16, 16, 128);
		 * 
		 * String s = "我"; byte[] bytes = s.getBytes("unicode"); for (int i = 0;
		 * i < bytes.length; i++) { String str = Integer.toHexString(bytes[i] &
		 * 0xff); System.out.println(str); } int a = 10; int b = 200 / 20;
		 * System.out.println(a == b); // get_separator();
		 */
		System.out.println("====success====");

	}

	private static void get_arrangement(Object[] args) {
		String input = args[0].toString();

		String[] test = input.split(",");
		if (test.length <= 1) {
			return;
		}

		// 去掉重复
		HashSet<String> sets = new HashSet<String>();
		for (int i = 0; i < test.length - 1; i++) {
			sets.add(test[i]);
		}
		ArrayList<String> lists = new ArrayList<String>(sets);
		Collections.sort(lists);

		// 排列
		String r;
		for (int i = 0; i < lists.size(); i++) {
			for (int j = i + 1; j < lists.size(); j++) {
				r = (test[i] + "," + test[j]);
				System.out.println(r);
				r = (test[j] + "," + test[i]);
				System.out.println(r);
			}
		}

	}

	private static void get_separator() {
		if (System.getProperty("line.separator").equals("\r\n")) {
			System.out.println(" windows");
		} else if (System.getProperty("line.separator").equals("\r")) {
			System.out.println(" Mac");
		} else if (System.getProperty("line.separator").equals("\n")) {
			System.out.println(" Unix/Linux");
		}
	}

	private static void saolei(int x, int y, int num_lei) {
		if (x <= 1 || y <= 1) {
			return;
		}
		int len = x * y;
		float ratio = len / num_lei;
		if (ratio < 2f || ratio > 10f) {
			return;
		}

		List lei = new ArrayList(num_lei);
		int install_lei = 0;
		while (install_lei < num_lei) {
			int rand = (int) (Math.random() * len);
			if (lei.contains(rand)) {
				continue;
			} else {
				lei.add(rand);
				install_lei++;
			}
		}

		int[][] arr = new int[x][y];
		for (int i = 0; i < x; i++) {
			for (int j = 0; j < y; j++) {

				int if_lei = i * y + j;
				if (lei.contains(if_lei)) {
					arr[i][j] = -1;
				} else {

					int left_row = i;
					int left_col = j - 1;
					int right_row = i;
					int right_col = j + 1;
					int up_row = i - 1;
					int up_col = j;
					int down_row = i + 1;
					int down_col = j;

					int ret = 0;
					if (left_row >= 0 && left_row < x && left_col >= 0
							&& left_col < y) {
						int r = left_row * y + left_col;
						if (lei.contains(r)) {
							ret++;
						}
					}
					if (right_row >= 0 && right_row < x && right_col >= 0
							&& right_col < y) {
						int r = right_row * y + right_col;
						if (lei.contains(r)) {
							ret++;
						}
					}
					if (up_row >= 0 && up_row < x && up_col >= 0 && up_col < y) {
						int r = up_row * y + up_col;
						if (lei.contains(r)) {
							ret++;
						}
					}
					if (down_row >= 0 && down_row < x && down_col >= 0
							&& down_col < y) {
						int r = down_row * y + down_col;
						if (lei.contains(r)) {
							ret++;
						}
					}
					arr[i][j] = ret;
				}

			}
		}

		for (int i = 0; i < x; i++) {
			for (int j = 0; j < y; j++) {
				String s = String.valueOf(arr[i][j]);
				if (arr[i][j] == -1) {
					s = "+";
				}
				System.out.print(s + "\t");
			}
			System.out.println();
		}
	}

	private static List out_threenmoreone(int i) {
		int ret = i;
		List l = new ArrayList();
		l.add(ret);
		while (ret != 1) {
			ret = threenmoreone(ret);
			l.add(ret);
		}
		return l;
	}

	private static int threenmoreone(int n) {
		if (n % 2 == 1) {
			return 3 * n + 1;
		} else {
			return n / 2;
		}
	}

}
