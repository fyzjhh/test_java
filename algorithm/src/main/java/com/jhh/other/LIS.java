package com.test.algorithm.other;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

@SuppressWarnings("rawtypes")
public class LIS {
	static int[] arr = { 1, 3, 13, 20, 55, 66, 11, 26 };

    // [1, -1, 2, -3, 4, -5, 6, -7]
    // [1, 2, 1, 3, 1, 4, 1]
    // ʱ�临�Ӷ�:O(N*N)
    public static void find1(int[] a)
    {
        int length = a.length;
        int[] list = new int[length];// �洢��i��Ԫ��֮ǰ�����������ֵ
        List<Integer> result = new ArrayList<Integer>(); // �洢���������
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
        System.out.println("��i��Ԫ��ʱ��������У�" + Arrays.toString(list));
        // Ѱ��list�����ֵ
        int max = list[0];
        for (int i = 0; i < length; i++)
        {
            if (list[i] > max)
            {
                max = list[i];
            }
        }
//        {5,6,7,1,2,8,3,4,5,9};
        System.out.println("��������г��ȣ�" + max);
        System.out.println("��������У�" + result);
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
			/* ��ջ��Ԫ�ش�������ջ */
			if (temp > stack.elementAt(top)) {
				stack.push(temp);
				top++;
			} else {
				int low = 0, high = top;
				int mid;
				/* ���ּ���ջ�б�temp��ĵ�һ���� */
				while (low <= high) {
					mid = (low + high) / 2;
					if (temp > stack.elementAt(mid)) {
						low = mid + 1;
					} else {
						high = mid - 1;
					}
				}
				/* ��temp�滻 */
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
