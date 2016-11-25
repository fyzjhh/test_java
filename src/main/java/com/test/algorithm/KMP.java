package com.test.algorithm;

public class KMP {
	public static int[] next1(char[] s) {
		int length = s.length;
		int next[] = new int[length];
		
		return next;
	}
	
	public static int[] next(char[] s) {
		int length = s.length;
		int next[] = new int[length];
		next[0] = -1;
		next[1] = 0;
		for (int i = 2; i < length; i++) {
			/* ���ǰ��������ǰһλ�����next�ۣ�λ��ȵĻ�����ô��ǰ�����λ��next��������ǰһλ��next�ۣ��ټ�1 */
			int temp = next[i - 1];
			while (temp != -1 && s[i - 1] != s[temp]) {
				temp = next[temp];
			}
			if (temp == -1)
				next[i] = 0;
			else
				next[i] = temp + 1;
		}
		return next;
	}

	public static int compare(char[] t, char[] s) {
		int tlen = t.length;
		int slen = s.length;
		int[] next = next(s);
		int i, j;
		for (i = 0, j = 0; i < tlen - slen && j < slen;) {
			if (t[i] == s[j]) {
				i++;
				j++;
			} else {
				j = next[j];
				if (j == -1) {
					i++;
					j++;
				}
			}
		}
		if (j < slen)
			return -1;
		else
			return i;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		char s[] = "ababcabcd".toCharArray();
		char t[] = "zhangabliababacling".toCharArray();
		int temp = compare(t, s);
		System.out.println(temp);

	}
	
	

}