package com.test.java;
public class T_EditDistance {
	public static int ld(String s, String t) {
		int sLen = s.length();
		int tLen = t.length();
		int d[][];
		int si;
		int ti;
		char ch1;
		char ch2;
		int cost;
		if (sLen == 0) {
			return tLen;
		}
		if (tLen == 0) {
			return sLen;
		}
		d = new int[sLen + 1][tLen + 1];
		for (si = 0; si <= sLen; si++) {
			d[si][0] = si;
		}
		for (ti = 0; ti <= tLen; ti++) {
			d[0][ti] = ti;
		}
		for (si = 1; si <= sLen; si++) {
			ch1 = s.charAt(si - 1);
			for (ti = 1; ti <= tLen; ti++) {
				ch2 = t.charAt(ti - 1);
				if (ch1 == ch2) {
					cost = 0;
				} else {
					cost = 1;
				}
				int a1 = d[si - 1][ti] + 1;
				int a2 = d[si][ti - 1] + 1;
				int a3 = d[si - 1][ti - 1] + cost;
				d[si][ti] = Math.min(Math.min(a1, a2), a3);
			}
		}
		return d[sLen][tLen];
	}



	public static void main(String[] args) {
		String tar = "�㽭����";
		String src = "����ʡ";
		float ed= T_EditDistance.ld(src, tar);
		float sim = 1 - ed/Math.max(src.length(), tar.length());
		System.out.println("sim=" + sim);
	}
}
