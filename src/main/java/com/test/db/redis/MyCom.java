package com.test.redis;

import java.util.Comparator;

public class MyCom implements Comparator {

	public int compare(Object o1, Object o2) {
		if (o1 instanceof String) {
			int i1 = Integer.parseInt((String) o1);
			int i2 = Integer.parseInt((String) o2);
			return (i1 >= i2) ? 1 : 0;
		}
		return 0;

	}

}
