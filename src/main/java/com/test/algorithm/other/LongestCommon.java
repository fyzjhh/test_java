package com.test.algorithm;

public class LongestCommon {

	// 最长公共子序列（Longest Common Subsequence）指的是两个字符串中的最长公共子序列，不要求子序列连续。
	public static int compute_lcs_seq(char[] str1, char[] str2) {
		int substringLength1 = str1.length;
		int substringLength2 = str2.length;

		// 构造二维数组记录子问题A[i]和B[j]的LCS的长度
		int[][] opt = new int[substringLength1 + 1][substringLength2 + 1];

		// 从后向前，动态规划计算所有子问题。也可从前到后。
		for (int i = substringLength1 - 1; i >= 0; i--) {
			for (int j = substringLength2 - 1; j >= 0; j--) {
				if (str1[i] == str2[j])
					opt[i][j] = opt[i + 1][j + 1] + 1;// 状态转移方程
				else
					opt[i][j] = Math.max(opt[i + 1][j], opt[i][j + 1]);// 状态转移方程
			}
		}
		System.out.println("substring1:" + new String(str1));
		System.out.println("substring2:" + new String(str2));
		System.out.print("LCS:");

		int i = 0, j = 0;
		while (i < substringLength1 && j < substringLength2) {
			if (str1[i] == str2[j]) {
				System.out.print(str1[i]);
				i++;
				j++;
			} else if (opt[i + 1][j] >= opt[i][j + 1]) {
				i++;
			} else {
				j++;
			}
		}
		System.out.println();
		return opt[0][0];
	}

	// 最长公共子串（Longest Common Substring）指的是两个字符串中的最长公共子串，要求子串一定连续。
	public static int compute_lcs_string(char[] str1, char[] str2) {
		int size1 = str1.length;
		int size2 = str2.length;
		if (size1 == 0 || size2 == 0)
			return 0;

		// the start position of substring in original string
		int start1 = -1;
		int start2 = -1;
		// the longest length of common substring
		int longest = 0;

		// record how many comparisons the solution did;
		// it can be used to know which algorithm is better
		int comparisons = 0;

		for (int i = 0; i < size1; ++i) {
			int m = i;
			int n = 0;
			int length = 0;
			while (m < size1 && n < size2) {
				++comparisons;
				if (str1[m] != str2[n]) {
					length = 0;
				} else {
					++length;
					if (longest < length) {
						longest = length;
						start1 = m - longest + 1;
						start2 = n - longest + 1;
					}
				}

				++m;
				++n;
			}
		}

		// shift string2 to find the longest common substring
		for (int j = 1; j < size2; ++j) {
			int m = 0;
			int n = j;
			int length = 0;
			while (m < size1 && n < size2) {
				++comparisons;
				if (str1[m] != str2[n]) {
					length = 0;
				} else {
					++length;
					if (longest < length) {
						longest = length;
						start1 = m - longest + 1;
						start2 = n - longest + 1;
					}
				}

				++m;
				++n;
			}
		}
		System.out.printf(
				"from %d of str1 and %d of str2, compared for %d times\n",
				start1, start2, comparisons);
		return longest;
	}
//
//	public static void diff_file(String[] args) {
//
//        // read in lines of each file
//        In in0 = new In(args[0]);
//        In in1 = new In(args[1]);
//        String[] x = in0.readAllLines();
//        String[] y = in1.readAllLines();
//
//        // number of lines of each file
//        int M = x.length;
//        int N = y.length;
//
//        // opt[i][j] = length of LCS of x[i..M] and y[j..N]
//        int[][] opt = new int[M+1][N+1];
//
//        // compute length of LCS and all subproblems via dynamic programming
//        for (int i = M-1; i >= 0; i--) {
//            for (int j = N-1; j >= 0; j--) {
//                if (x[i].equals(y[j]))
//                    opt[i][j] = opt[i+1][j+1] + 1;
//                else 
//                    opt[i][j] = Math.max(opt[i+1][j], opt[i][j+1]);
//            }
//        }
//
//        // recover LCS itself and print out non-matching lines to standard output
//        int i = 0, j = 0;
//        while(i < M && j < N) {
//            if (x[i].equals(y[j])) {
//                i++;
//                j++;
//            }
//            else if (opt[i+1][j] >= opt[i][j+1]) System.out.println("< " + x[i++]);
//            else                                 System.out.println("> " + y[j++]);
//        }
//
//        // dump out one remainder of one string if the other is exhausted
//        while(i < M || j < N) {
//            if      (i == M) System.out.println("> " + y[j++]);
//            else if (j == N) System.out.println("< " + x[i++]);
//        }
//    }
	public static void main(String args[]) throws Exception {
		// int r = compute_lcs_string("jianghehui".toCharArray(),
		// "he".toCharArray());
		// System.out.println(r);

		int r = compute_lcs_seq("jianghehui".toCharArray(), "hxe".toCharArray());
		System.out.println(r);
	}
}