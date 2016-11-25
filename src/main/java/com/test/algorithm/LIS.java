package com.test.algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

@SuppressWarnings("rawtypes")
public class LIS {
	static int[] arr = { 1, 3, 13, 20, 55, 66, 11, 26 };

    // [1, -1, 2, -3, 4, -5, 6, -7]
    // [1, 2, 1, 3, 1, 4, 1]
    // 时间复杂度:O(N*N)
    public static void find1(int[] a)
    {
        int length = a.length;
        int[] list = new int[length];// 存储第i个元素之前的最长递增序列值
        List<Integer> result = new ArrayList<Integer>(); // 存储最长递增序列
        for (int i = 0; i < length; i++)
        {
            list[i] = 1;
            for (int j = 0; j < i; j++)
            {
                if (a[j] < a[i] && list[j] + 1 > list[i])
                {
                    list[i] = list[j] + 1;
                    if (result.isEmpty())
                    {
                        result.add(list[j]);
                    }
                    if (!result.contains(list[i]))
                    {
                        result.add(list[i]);
                    }
                }
            }
        }
        System.out.println("第i个元素时最长递增序列：" + Arrays.toString(list));
        // 寻找list中最大值
        int max = list[0];
        for (int i = 0; i < length; i++)
        {
            if (list[i] > max)
            {
                max = list[i];
            }
        }
//        {5,6,7,1,2,8,3,4,5,9};
        System.out.println("最长递增序列长度：" + max);
        System.out.println("最长递增序列：" + result);
    }
    
	public static Stack calLis(int[] arr) {
		int top, temp;
		int len = arr.length;

		Stack<Integer> stack = new Stack<Integer>();
		top = -1;
		for (int i = 0; i < len; i++) {
			temp = arr[i];
			if (i == 0) {
				stack.push(temp);
				top++;
				continue;
			}
			/* 比栈顶元素大数就入栈 */
			if (temp > stack.elementAt(top)) {
				stack.push(temp);
				top++;
			} else {
				int low = 0, high = top;
				int mid;
				/* 二分检索栈中比temp大的第一个数 */
				while (low <= high) {
					mid = (low + high) / 2;
					if (temp > stack.elementAt(mid)) {
						low = mid + 1;
					} else {
						high = mid - 1;
					}
				}
				/* 用temp替换 */
				stack.setElementAt(temp, low);
			}
		}
		return stack;
	}

	public static void main(String args[]) throws Exception {
		
//		Stack ret = calLis(arr);
//		System.out.println("len:" + ret.size() + ", data:" + ret.toString());
		
		int[] a = {5,6,7,1,2,8,3,4,5,9};
		find1(a);
	}
}
