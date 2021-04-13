package com.test.algorithm.other;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GuessNum {
	public int guessNumber(int n, int num) {
		int low = 1;
		int high = n;
		while (low <= high) {
			int mid1 = low + (high - low) / 3;
			int mid2 = high - (high - low) / 3;
			int res1 = num - mid1;
			int res2 = num - mid2;
			if (res1 == 0)
				return mid1;
			if (res2 == 0)
				return mid2;
			else if (res1 < 0)
				high = mid1 - 1;
			else if (res2 > 0)
				low = mid2 + 1;
			else {
				low = mid1 + 1;
				high = mid2 - 1;
			}
		}
		return -1;
	}

	public List<Integer> fenjie(int mynum) {

		List<Integer> x = new ArrayList<Integer>();
		int i = 2;
		while (i <= mynum) {

			if (mynum % i == 0) {
				mynum = mynum / i;
				x.add(i);
			} else {
				i++;
			}
		}
		return x;
	}

	public int fourpart(int N, int mynum) {

		int ret = -1;
		int low = 0;
		int high = N;
		int dg = 0;
		while (low <= high) {
			dg++;
			System.out.println(dg + " " + low + " " + high);
			int mid1 = low + (high - low) / 4;
			int mid2 = low + (high - low) / 4 + (high - low) / 4;
			int mid3 = low + (high - low) / 4 + (high - low) / 4 + (high - low)
					/ 4;

			if (mynum == low) {
				ret = low;
				break;
			} else if (mynum == mid1) {
				ret = mid1;
				break;
			} else if (mynum == mid2) {
				ret = mid2;
				break;
			} else if (mynum == mid3) {
				ret = mid3;
				break;
			} else if (mynum == high) {
				ret = high;
				break;
			} else {
				if (mynum < low) {
					ret = -1;
					break;
				} else if (mynum < mid1) {
					low = low + 1;
					high = mid1 - 1;
				} else if (mynum < mid2) {
					low = mid1 + 1;
					high = mid2 - 1;
				} else if (mynum < mid3) {
					low = mid2 + 1;
					high = mid3 - 1;
				} else if (mynum < high) {
					low = mid3 + 1;
					high = high - 1;
				} else {
					ret = -1;
					break;
				}
			}

		}

		return ret;

	}

	public static void main(String args[]) throws Exception {

		GuessNum num = new GuessNum();
		List<Integer> ret = num.fenjie(168 * 7 * 9);
		System.out.println(Arrays.toString(ret.toArray()));

		// System.out.println(num.fourpart(10000, 2356));

	}
}
