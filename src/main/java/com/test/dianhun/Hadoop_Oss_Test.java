package com.test.dianhun;

import java.text.ParseException;
import java.util.Date;

public class Hadoop_Oss_Test extends Hadoop_Oss {

	public Hadoop_Oss_Test() {

		super();
		workDir = "/data/tmp/resultdir";
		action_date = dateformat.format(new Date());
		try {
			action_date = addDay(action_date, -1);
		} catch (ParseException e) {
			e.printStackTrace();
		}
//		 action_date = "2014-08-05";
	}

	public void test_get_corp_fight_score_distribution() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_corp_fight_score_distribution ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_corp_equip_score_distribution() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_corp_equip_score_distribution ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_corp_person_distribution() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_corp_person_distribution ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_corp_level_distribution() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_corp_level_distribution ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_herofightscore() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_herofightscore ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_zhibo_online_person() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_zhibo_online_person ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_zhibo_top() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_zhibo_top ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_zhibo_map_count() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_zhibo_map_count ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_zhibo_person_count() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_zhibo_person_count ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_fight_score_distribution() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_fight_score_distribution ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_corptaskpersoncount() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_corptaskpersoncount ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_equipscore() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_equipscore ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_match_count() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_match_count ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_match_person_count() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_match_person_count ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_dropeditem() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_dropeditem ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_achievementcompletionrate() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_achievementcompletionrate ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userlostlevel() throws Exception {
		String base_date = "2014-04-01";

		for (int i = 0; i < 1; i++) {
			String start1 = addMonth(base_date, i);
			teststr = " -w " + workDir + " -a " + action_date
					+ " -s get_userlostlevel -o 1," + start1;
			args = teststr.split(" +");
			runOut(args);
		}
	}

	public void test_get_reguserlostcount() throws Exception {
		String base_date = "2014-04-01";

		for (int i = 0; i < 1; i++) {
			String start1 = addMonth(base_date, i);
			teststr = " -w " + workDir + " -a " + action_date
					+ " -s get_reguserlostcount -o " + start1;
			args = teststr.split(" +");
			runOut(args);
		}
	}

	public void test_get_userlogincount() throws Exception {

		String base_date = "2014-04-21";
		for (int i = 0; i < 1; i++) {
			String str = addMonth(base_date, i);
			teststr = " -w " + workDir + " -a " + action_date
					+ " -s get_userlogincount -o 0," + str;
			args = teststr.split(" +");
			System.out.println(str);
			runOut(args);
		}

	}
}
