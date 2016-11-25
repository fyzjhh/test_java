package com.test.dianhun;

import java.text.ParseException;
import java.util.Date;

public class Hadoop_Platform_Test extends Hadoop_Ad {

	public Hadoop_Platform_Test() {

		super();
		workDir = "/data/tmp/resultdir";
		action_date = dateformat.format(new Date());
		try {
			action_date = addDay(action_date, -1);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// action_date = "2014-04-20";
	}

	public void test_get_product_register_count() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_product_register_count";
		args = teststr.split(" +");
		runOut(args);
	}

}
