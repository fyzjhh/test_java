package com.test.dianhun;

import java.text.ParseException;
import java.util.Date;

public class Hadoop_Temp_Test extends Hadoop_Temp {

	// String action_date = EMPTY;

	public Hadoop_Temp_Test() {

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

	public void test_get_userinfobyip() throws Exception {
//		String a = "218.28.238.58|2014-08-30+17:00:00|2014-08-30+23:00:00|2014-08-01|2014-12-01|零度矩阵网吧_218.28.79.46|2014-08-13+17:00:00|2014-08-13+23:00:00|2014-08-01|2014-12-01|南阳市动感网吧_222.89.175.45|2014-08-04+17:00:00|2014-08-04+23:00:00|2014-08-01|2014-12-01|金坐标网吧_218.28.119.90|2014-08-04+17:00:00|2014-08-04+23:00:00|2014-08-01|2014-12-01|金坐标网吧_61.163.4.124|2014-08-05+17:00:00|2014-08-05+23:00:00|2014-08-01|2014-12-01|蓝月亮网吧_222.88.79.230|2014-08-05+17:00:00|2014-08-05+23:00:00|2014-08-01|2014-12-01|蓝月亮网吧_222.89.169.70|2014-08-21+17:00:00|2014-08-21+23:00:00|2014-08-01|2014-12-01|三星网吧_222.88.156.38|2014-08-10+17:00:00|2014-08-10+23:00:00|2014-08-01|2014-12-01|喜洋洋网吧_218.28.79.34|2014-08-06+17:00:00|2014-08-06+23:00:00|2014-08-01|2014-12-01|幸福网吧_219.150.242.140|2014-08-06+17:00:00|2014-08-06+23:00:00|2014-08-01|2014-12-01|幸福网吧_61.136.80.202|2014-08-06+17:00:00|2014-08-06+23:00:00|2014-08-01|2014-12-01|开心网络会馆_123.8.250.97|2014-08-06+17:00:00|2014-08-06+23:00:00|2014-08-01|2014-12-01|开心网络会馆_218.28.88.243|2014-08-09+17:00:00|2014-08-09+23:00:00|2014-08-01|2014-12-01|星海网吧_222.89.175.39|2014-08-12+17:00:00|2014-08-12+23:00:00|2014-08-01|2014-12-01|红燕网吧_218.28.87.230|2014-08-24+17:00:00|2014-08-24+23:00:00|2014-08-01|2014-12-01|幸福鸟枣林店_222.88.154.135|2014-08-24+17:00:00|2014-08-24+23:00:00|2014-08-01|2014-12-01|幸福鸟枣林店_218.29.7.73|2014-08-04+17:00:00|2014-08-04+23:00:00|2014-08-01|2014-12-01|中青网吧_219.156.133.98|2014-08-04+17:00:00|2014-08-04+23:00:00|2014-08-01|2014-12-01|中青网吧_61.163.45.36|2014-08-05+17:00:00|2014-08-05+23:00:00|2014-08-01|2014-12-01|中青网络_218.28.95.163|2014-08-08+17:00:00|2014-08-08+23:00:00|2014-08-01|2014-12-01|百兆网吧_222.88.154.111|2014-08-08+17:00:00|2014-08-08+23:00:00|2014-08-01|2014-12-01|百兆网吧_61.163.4.106|2014-08-07+17:00:00|2014-08-07+23:00:00|2014-08-01|2014-12-01|淡水鱼网吧_222.88.226.146|2014-09-14+17:00:00|2014-09-14+23:00:00|2014-08-01|2014-12-01|枫情网吧枫情网络连锁_218.28.91.12|2014-09-14+17:00:00|2014-09-14+23:00:00|2014-08-01|2014-12-01|枫情网吧枫情网络连锁_218.28.92.82|2014-09-14+17:00:00|2014-09-14+23:00:00|2014-08-01|2014-12-01|枫情网吧枫情网络连锁_218.28.234.34|2014-08-20+17:00:00|2014-08-20+23:00:00|2014-08-01|2014-12-01|国通战略时代网吧_222.89.64.106|2014-08-19+17:00:00|2014-08-19+23:00:00|2014-08-01|2014-12-01|虹雨网吧_222.139.154.19|2014-08-19+17:00:00|2014-08-19+23:00:00|2014-08-01|2014-12-01|虹雨网吧_123.7.52.174|2014-08-08+17:00:00|2014-08-08+23:00:00|2014-08-01|2014-12-01|火烈鸟网吧_222.88.81.173|2014-08-08+17:00:00|2014-08-08+23:00:00|2014-08-01|2014-12-01|火烈鸟网吧_218.28.22.227|2014-08-10+17:00:00|2014-08-10+23:00:00|2014-08-01|2014-12-01|锦宏网吧_61.158.173.47|2014-09-08+17:00:00|2014-09-08+23:00:00|2014-08-01|2014-12-01|橘子网吧二店_222.89.251.73|2014-09-08+17:00:00|2014-09-08+23:00:00|2014-08-01|2014-12-01|橘子网吧二店_123.162.24.24|2014-09-19+17:00:00|2014-09-19+23:00:00|2014-08-01|2014-12-01|康吧行网吧_123.162.49.183|2014-09-19+17:00:00|2014-09-19+23:00:00|2014-08-01|2014-12-01|康吧行网吧_61.158.172.9|2014-09-19+17:00:00|2014-09-19+23:00:00|2014-08-01|2014-12-01|康吧行网吧_61.158.169.3|2014-09-18+17:00:00|2014-09-18+23:00:00|2014-08-01|2014-12-01|磨坊网络_222.89.251.48|2014-09-18+17:00:00|2014-09-18+23:00:00|2014-08-01|2014-12-01|磨坊网络_218.29.6.91|2014-08-10+17:00:00|2014-08-10+23:00:00|2014-08-01|2014-12-01|麒麟网吧_61.163.89.251|2014-08-05+17:00:00|2014-08-05+23:00:00|2014-08-01|2014-12-01|盛世网吧_222.89.165.142|2014-08-14+17:00:00|2014-08-14+23:00:00|2014-08-01|2014-12-01|腾讯网吧_221.14.148.166|2014-08-14+17:00:00|2014-08-14+23:00:00|2014-08-01|2014-12-01|腾讯网吧_123.52.99.58|2014-08-04+17:00:00|2014-08-04+23:00:00|2014-08-01|2014-12-01|网星网咖网络会所_218.28.7.122|2014-08-04+17:00:00|2014-08-04+23:00:00|2014-08-01|2014-12-01|网星网咖网络会所_61.163.162.193|2014-09-14+17:00:00|2014-09-14+23:00:00|2014-08-01|2014-12-01|月轩北关店_222.89.240.23|2014-09-14+17:00:00|2014-09-14+23:00:00|2014-08-01|2014-12-01|月轩北关店_222.89.240.137|2014-09-11+17:00:00|2014-09-11+23:00:00|2014-08-01|2014-12-01|月轩奇缘店_61.158.172.30|2014-09-11+17:00:00|2014-09-11+23:00:00|2014-08-01|2014-12-01|月轩奇缘店_222.89.251.226|2014-09-13+17:00:00|2014-09-13+23:00:00|2014-08-01|2014-12-01|月轩文化店_61.158.172.3|2014-09-13+17:00:00|2014-09-13+23:00:00|2014-08-01|2014-12-01|月轩文化店_171.8.66.190|2014-08-08+17:00:00|2014-08-08+23:00:00|2014-08-01|2014-12-01|中录时空网络会所_218.28.235.98|2014-08-08+17:00:00|2014-08-08+23:00:00|2014-08-01|2014-12-01|中录时空网络会所_222.88.156.37|2014-08-15+17:00:00|2014-08-15+23:00:00|2014-08-01|2014-12-01|中意网吧_218.28.95.193|2014-08-15+17:00:00|2014-08-15+23:00:00|2014-08-01|2014-12-01|中意网吧_221.13.131.25|2014-08-15+17:00:00|2014-08-15+23:00:00|2014-08-01|2014-12-01|乐意网吧_218.28.88.244|2014-08-12+17:00:00|2014-08-12+23:00:00|2014-08-01|2014-12-01|幸福鸟麒麟路店_222.88.240.13|2014-08-12+17:00:00|2014-08-12+23:00:00|2014-08-01|2014-12-01|幸福鸟麒麟路店_222.89.248.49|2014-09-10+17:00:00|2014-09-10+23:00:00|2014-08-01|2014-12-01|橘子凯旋路店_61.158.169.155|2014-09-10+17:00:00|2014-09-10+23:00:00|2014-08-01|2014-12-01|橘子凯旋路店_221.210.2.142|2014-08-21+17:00:00|2014-08-21+23:00:00|2014-08-01|2014-12-01|华馨园网络电机二店_222.172.80.82|2014-08-21+17:00:00|2014-08-21+23:00:00|2014-08-01|2014-12-01|华馨园网络电机二店_221.209.14.92|2014-08-20+17:00:00|2014-08-20+23:00:00|2014-08-01|2014-12-01|畅通网吧";
		// 41 2 0 
		String b = "42.232.54.167|2014-08-09+17:00:00|2014-08-09+23:00:00|2014-08-01|2014-12-01|星海网吧";
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_userinfobyip  -o jhh," + b;
		args = teststr.split(" +");
		runOut(args);
	}
	
	public void test_get_guozhan_honorvalue() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_guozhan_honorvalue -o 2014-06-09,2014-06-24";

		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_hero_card() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_hero_card -o 2014-06-01,2014-06-30";

		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_user_fihgt_score() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_user_fihgt_score -o 2014-06-01,2014-06-08";

		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_abnormaluserinfo() throws Exception {
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_abnormaluserinfo -o 2014-05-26,2014-06-09,/data/tmp/jhh/t.txt";

		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_loginusercount() throws Exception {
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_loginusercount -o 2014-06-05_14:00:00,2014-06-05_17:45:00";

		args = teststr.split(" +");
		runOut(args);
		// teststr = " -w "
		// + workDir
		// + " -a "
		// + action_date
		// +
		// " -s get_loginusercount -o 2014-04-29_07:00:00,2014-04-29_07:00:00";
		// args = teststr.split(" +");
		// runOut(args);
		// teststr = " -w "
		// + workDir
		// + " -a "
		// + action_date
		// +
		// " -s get_loginusercount -o 2014-05-06_08:00:00,2014-05-06_09:00:00";
		// args = teststr.split(" +");
		// runOut(args);
		// teststr = " -w "
		// + workDir
		// + " -a "
		// + action_date
		// +
		// " -s get_loginusercount -o 2014-05-08_09:00:00,2014-05-08_11:00:00";
		// args = teststr.split(" +");
		// runOut(args);
	}

	public void test_get_talent() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_talent -o 2014-05-22,2014-06-01";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_loginusercount_addr() throws Exception {
		// teststr = " -w "
		// + workDir
		// + " -a "
		// + action_date
		// +
		// " -s get_loginusercount_addr -o 2014-05-30_09:00:00,2014-05-30_13:00:00";
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_loginusercount_addr -o 2014-06-05_10:00:00,2014-06-05_13:00:00";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userinfo4() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_userinfo4 -o 2014-07-27";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userinfo_lifecycle() throws Exception {
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_userinfo_lifecycle -o 2014-07-12,2014-07-26,2014-07-12,2014-08-26";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userinfo4_batch() throws Exception {
		test_dates = new String[] { "2014-07-19", "2014-07-20", "2014-07-26",
				"2014-07-27", };
		// workDir = "/data/tmp/j_resultdir";
		for (int i = 0; i < test_dates.length; i++) {
			action_date = test_dates[i];
			teststr = " -w " + workDir + " -a " + action_date
					+ " -s get_userinfo4 -o " + test_dates[i];
			System.out.println(teststr);
			args = teststr.split(" +");
			runOut(args);
		}
	}

	public void test_get_userinfo3() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_userinfo3 -o 2014-04-07,2014-05-07";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userinfo3_userlevel() throws Exception {
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_userinfo3_userlevel -o 2014-04-07,2014-05-07,/data/tmp/noid.txt";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_tryplaycount_batch() throws Exception {
		String base_date = "2014-05-02";
		date_cnt = 1;

		for (int j = 0; j < date_cnt; j++) {
			String action_date = addDay(base_date, j);
			teststr = " -w " + workDir + " -a " + action_date
					+ " -s get_tryplaycount ";
			args = teststr.split(" +");
			runOut(args);
		}

	}

	public void test_get_tryplaycount() throws Exception {

		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_tryplaycount ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userid_bycondition() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_userid_bycondition -o 2014-05-31,2014-06-02";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userinfo_app() throws Exception {
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_userinfo_app -o 2014-05-26,2014-06-26,/data/tmp/login_526_626.txt";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userinfo1() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_userinfo1 -o 2014-03-22";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_userinfo1_batch() throws Exception {
		// String[] dates = new String[] { "2014-03-29", "2014-04-05",
		// "2014-04-12", "2014-04-19" };
		test_dates = new String[] { "2014-05-24", "2014-05-25", "2014-05-31",
				"2014-06-01", };
		workDir = " /data/tmp/j_resultdir";
		for (int i = 0; i < test_dates.length; i++) {
			action_date = test_dates[i];
			teststr = "./step-hadoop-temp.sh -w " + workDir + " -a "
					+ action_date + " -s get_userinfo1 -o " + test_dates[i];
			System.out.println(teststr);
			// args = teststr.split(" +");
			// runOut(args);
		}

	}

	public void test_get_userinfo2_batch() throws Exception {
		// String[] dates = new String[] { "2014-03-22", "2014-03-29" };
		test_dates = new String[] { "2014-05-10", "2014-05-17", "2014-05-24",
				"2014-05-25", };
		for (int i = 0; i < test_dates.length; i++) {
			teststr = " -w " + workDir + " -a " + test_dates[i]
					+ " -s get_userinfo2 -o " + test_dates[i];
			args = teststr.split(" +");
			runOut(args);
		}
	}

	public void test_get_activeuser_viplevel() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_activeuser_viplevel -o 2014-06-01,2014-08-17";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_lostuseritemcount() throws Exception {
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_lostuseritemcount -o 2014-03-20,2014-03-27,2014-03-28,2014-04-03";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_lostuserkickedcount() throws Exception {
		teststr = " -w "
				+ workDir
				+ " -a "
				+ action_date
				+ " -s get_lostuserkickedcount -o 2014-03-20,2014-03-27,2014-03-28,2014-04-03";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_heroplaystats() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_heroplaystats -o 2014-01-01,2014-04-03";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_usercharcreate() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_usercharcreate -o 29472,2014-01-28,2014-02-25 ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_usercharcreate1() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_usercharcreate1  ";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_useraction() throws Exception {
		// String[] files = new String[] { "131", "360", "52pk", "PPS",
		// "verycd",
		// "京东", "净网", "叶子猪", "多玩", "太平洋", "孔雀资讯", "微博", "新浪", "欣琨网吧",
		// "爱拍", "爱酷游", "电玩", "网星", "腾讯", "逗游", "银橙" };
		String[] files = new String[] { "all.txt" };
		for (int i = 0; i < files.length; i++) {
			teststr = " -w " + workDir + " -a " + action_date
					+ " -s get_useraction -o 2013-01-01,2013-12-01,/data/tmp/"
					+ files[i];
			args = teststr.split(" +");
			runOut(args);
		}

	}

	public void test_get_vip_chongzhi_top() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_vip_chongzhi_top -o 2014-07-01,2014-07-31";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_honorvalue() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_honorvalue -o 2014-02-07,2014-02-15";
		args = teststr.split(" +");
		runOut(args);
	}

	public void test_get_soldiercostcount() throws Exception {
		teststr = " -w " + workDir + " -a " + action_date
				+ " -s get_soldiercostcount -o 2014-04-12,2014-05-12";
		args = teststr.split(" +");
		runOut(args);
	}
}
