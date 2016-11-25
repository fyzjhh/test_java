package com.test.dianhun;

import java.text.ParseException;
import java.util.Date;

public class Hadoop_Gm_Test extends Hadoop_Gm {

	public Hadoop_Gm_Test() {

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

	public void test_get_userhiddenfightscoredistribution_byzoneid()
			throws Exception {
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_userhiddenfightscoredistribution_byzoneid -o 2014-04-01,2014-04-08,2,jhh1";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userhiddenfightscoredistribution() throws Exception {
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_userhiddenfightscoredistribution -o 2014-03-10,2014-03-14,0,0,jhh1";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_user_fightscore_minus_withlevelandmoney() throws Exception {
		String teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_user_fightscore_minus_withlevelandmoney -o 2014-07-01,2014-07-21,777661_84515279,jhh1";
		args = teststr.split(" +");
		runOut(args);
	}
	public void test_get_user_fightscore_minus() throws Exception {
		String teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_user_fightscore_minus -o 2014-08-01,2014-08-12,777661_84515279_25680811_25308949_46719020,jhh2";
		args = teststr.split(" +");
		runOut(args);
	}
	public void test_get_userhiddenfightscore() throws Exception {
		String teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_userhiddenfightscore -o 2014-03-26,2014-03-29,25308949_46719020,jhh1";
		args = teststr.split(" +");
		runOut(args);
	}
}
