package com.test.algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BM {
	private String ref;
	private String seq;
	// BM algorithm, Bad character table
	private Map<Character, Integer> bmBc1;
	private Map<Character, ArrayList<Integer>> bmBc;
	private int[] bmGs;
	private ArrayList<Integer> match_pos;

	public BM(String seq) {
		this.seq = seq;
		bmBc();
		bmGs();
	}

	private void bmBc1() {
		bmBc1 = new HashMap<Character, Integer>();

		for (int i = 0; i < seq.length(); i++) {
			bmBc1.put(seq.charAt(i), i);
		}
	}

	private void bmBc() {
		bmBc = new HashMap<Character, ArrayList<Integer>>();
		for (int i = seq.length() - 1; i >= 0; i--) {
			Character key = seq.charAt(i);
			if (!bmBc.containsKey(key))
				bmBc.put(key, new ArrayList<Integer>());

			bmBc.get(key).add(i);
		}
	}

	private void bmGs() {
		String Pr = new StringBuilder(seq).reverse().toString();
		Z zz = new Z();
		zz.calculateZ(Pr);
		int[] Zvalue = zz.getZ();
		int n = seq.length();
		bmGs = new int[n];
		int i;
		for (int j = 2; j < n; j++) {
			i = n - Zvalue[n - j - 1];
			if (i < n)
				bmGs[i] = j;
		}
	}

	public void printBcTable() {
		for (Map.Entry entry : bmBc1.entrySet()) {
			System.out.println(entry.getKey() + " -> " + entry.getValue());
		}
	}

	public void search1(String ref) {
		this.ref = ref;
		int n = ref.length();
		int m = seq.length();
		int i = 0;
		while (i <= n - m) {
			int j = m - 1;
			while (j >= 0 && seq.charAt(j) == ref.charAt(i + j)) {
				j--;
			}
			if (j == -1) {
				match_pos.add(i);
				i++;
				continue;
			}

			Character bc = ref.charAt(i + j);
			if (bmBc1.containsKey(bc)) {
				i += Math.max(j - bmBc1.get(bc), 1);
			} else {
				i += j + 1;
			}
		}
	}

	public void search(String ref) {
		this.ref = ref;
		match_pos = new ArrayList<Integer>();
		int n = ref.length();
		int m = seq.length();
		int i = 0;
		while (i <= n - m) {
			int j = m - 1;
			while (j >= 0 && seq.charAt(j) == ref.charAt(i + j)) {
				j--;
			}
			if (j == -1) {
				match_pos.add(i);
				i++;
				continue;
			}

			int bc_offset = 1;
			Character bc = ref.charAt(i + j);

			if (bmBc.containsKey(bc)) {
				boolean flag = false;
				Integer p;
				for (p = 0; p < bmBc.get(bc).size(); p++) {
					if (bmBc.get(bc).get(p) < j) {
						flag = true;
						break;
					}
				}
				if (flag) {
					bc_offset = j - bmBc.get(bc).get(p);
				}
			} else {
				bc_offset = j + 1;
			}

			int gs_offset = 1;
			if (j + 1 < m) {
				if (bmGs[j + 1] == 0) {
					gs_offset = m;
				} else {
					gs_offset = m - bmGs[j + 1] - 1;
				}
			}

			// i += bc_offset;
			// i += gs_offset;
			i += Math.max(bc_offset, gs_offset);

		}
	}

	public void printPos() {
		for (int i = 0; i < match_pos.size(); i++) {
			System.out.println(match_pos.get(i));
		}
	}

	public void printMatchPosition() {
		MatchPosition mp = new MatchPosition(ref, seq, match_pos);
		mp.print();
	}
}

class MatchPosition {
	private String T;
	private String P;
	private ArrayList<Integer> match_pos;

	public MatchPosition(String T, String P, ArrayList<Integer> match_pos) {
		this.T = T;
		this.P = P;
		this.match_pos = match_pos;
	}

	public void print() {
		if (match_pos.size() == 0) {
			System.out.println("No match found...");
		}

		for (Integer match : match_pos) {
			System.out.println("match position: " + match);
			System.out.println(T);
			for (int i = 0; i < match; i++) {
				System.out.print(" ");
			}
			for (int i = match; i < match + P.length(); i++) {
				System.out.print("|");
			}
			System.out.println();
			for (int i = 0; i < match; i++) {
				System.out.print(" ");
			}
			System.out.println(P);
			System.out.println();
		}

	}
}