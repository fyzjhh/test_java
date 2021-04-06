package com.test.algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Canopy {

	public static void main(String[] args) {

		ArrayList<Long> lists = new ArrayList<Long>();
		for (int j = 0; j < 10; j++) {
			lists.add((long) (Math.random() * 1000));
		}
		Map<Long, ArrayList<Long>> canopy_map = run(lists, 200L, 50L);
		Iterator<Long> it = canopy_map.keySet().iterator();
		while (it.hasNext()) {
			Long center = (Long) it.next();
			System.out.println(center + "\t"
					+ Arrays.toString(canopy_map.get(center).toArray()));
		}
	}

	/*
	 * （1）、将数据集向量化得到一个list后放入内存，选择两个距离阈值：T1和T2，其中T1 >
	 * T2，对应上图，实线圈为T1，虚线圈为T2，T1和T2的值可以用交叉校验来确定；
	 * 
	 * （2）、从list中任取一点P，用低计算成本方法快速计算点P与所有Canopy之间的距离（如果当前不存在Canopy，则把点P作为一个Canopy）
	 * ，如果点P与某个Canopy距离在T1以内，则将点P加入到这个Canopy；
	 * 
	 * （3）、如果点P曾经与某个Canopy的距离在T2以内，则需要把点P从list中删除，这一步是认为点P此时与这个Canopy已经够近了，
	 * 因此它不可以再做其它Canopy的中心了；
	 * 
	 * （4）、重复步骤2、3，直到list为空结束。
	 */
	public static Map<Long, ArrayList<Long>> run(ArrayList<Long> lists,
			Long t1, Long t2) {

		Map<Long, ArrayList<Long>> canopy_map = new HashMap<Long, ArrayList<Long>>();
		int len = lists.size();
		for (int i = 0; i < len;) {

			Long f = lists.get(i);
			if (canopy_map.get(f) == null) {
				canopy_map.put(f, new ArrayList<Long>());
			}
			Iterator<Long> it = canopy_map.keySet().iterator();
			boolean flag = false;
			while (it.hasNext()) {
				Long center = (Long) it.next();
				if (distance(center, f) <= t1) {
					canopy_map.get(center).add(f);
				}
				if (distance(center, f) <= t2) {
					lists.remove(i);
				} else {
					flag = true;
				}
			}
			if (flag) {
				i++;
			}
		}
		return canopy_map;
	}

	// 计算距离
	private static Long distance(Long nd1, Long nd2) {
		return Math.abs(nd1 - nd2);
	}

}
