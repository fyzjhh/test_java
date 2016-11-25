package com.test.java;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TestJavaMemory implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3014716914084285225L;

	private static final String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";

	static DateFormat dtft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws Exception {
		int rountcnt = 2400000;
		int sleep = 100;
		if (args.length == 2) {
			rountcnt = Integer.parseInt(args[0]);
			sleep = Integer.parseInt(args[1]);
		}
		testhugepage(rountcnt, sleep);

		System.out.println("normal exit===========");
	}

	private static void testhugepage(int rountcnt, int sleep) throws Exception {
		System.out.println("====start====");

		Long startTime = System.currentTimeMillis();
		List<String> list = new ArrayList<String>();
		int i = 0;
		for (i = 1; i <= rountcnt; i++) {
			String tmpStr = base + i;
			list.add(tmpStr);
			if (i % 10000 == 0) {
				Thread.sleep(sleep);
				Long endTime = System.currentTimeMillis();
				System.out.println("i=" + i + "\t"
						+ dtft.format(new Date(endTime - startTime)));
			}
		}
		for (int m = 0; m < sleep; m++) {
			System.out.println("====sleep cnt : " + m);
			Thread.sleep(1000 * 60);
		}
		System.out.println("====end====");
	}

}
