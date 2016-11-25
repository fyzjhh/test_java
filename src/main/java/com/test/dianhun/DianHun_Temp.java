package com.test.dianhun;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.codehaus.jackson.map.ObjectMapper;

//统计的环境
//
//测试环境：
//机器：192.168.12.142
//数据库：192.168.12.5
//
//正式环境：
//机器：123.103.17.150
//数据库：223.203.210.60
//修复数据可能需要到不同机器上跑
//
//3类统计:
//临时的
//oss的
//媒体的
public class DianHun_Temp extends Tools implements DianHunSql {

	static String[][] register_dates_dates = {};

	static String iuinfile = null;
	// static int partuidcnt = 200;
	static String current_sub_action = null;

	public static void cal_register_dates_dates() {
		String[] tmpstr1 = other_args.split(SEMI);
		register_dates_dates = new String[tmpstr1.length][];
		for (int i = 0; i < tmpstr1.length; i++) {
			String[] tmpstr2 = tmpstr1[i].split(COMMA);
			register_dates_dates[i] = tmpstr2;
		}
	}

	public static void cal_register_dates() throws Exception {
		register_day_cnt = Integer.parseInt(other_args.split(COMMA)[0]);
		register_dates = new String[register_day_cnt];
		for (int n = 0; n < register_day_cnt; n++) {
			String tmp_date = addDay(action_date, 0 - n);
			register_dates[n] = tmp_date;
		}
	}

	public static void cal_action_dates_byrange() throws Exception {

		String start_date = action_date_range.split(WAVY)[0];
		String end_date = action_date_range.split(WAVY)[1];

		Date d1 = spacedatetimeformat.parse(start_date);
		Date d2 = spacedatetimeformat.parse(end_date);

		int diffdate = (int) ((d2.getTime() - d1.getTime()) / 86400000) + 1;
		action_dates = new String[diffdate];
		String tmp = EMPTY;
		for (int n = 0; n < diffdate; n++) {
			tmp = addDay(start_date, n);
			action_dates[n] = tmp;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_loginstats() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		start_timestamp = spacedatetimeformat.format(new Date());

		// open_LoginConn();
		Class.forName("com.mysql.jdbc.Driver");
		loginhostportstr = "183.136.237.184:3307";
		String url = "jdbc:mysql://" + loginhostportstr;
		loginConn = DriverManager.getConnection(url, user, pass);
		printLogStr(loginhostportstr + " connection opened !");
		ips = new String[] { "183.136.237.184:3306", "183.136.237.184:3307",
				"183.136.237.184:3308", "183.136.237.184:3311",
				"183.136.237.184:3312", "183.136.237.184:3313",
				"183.136.237.184:3314", "183.136.237.184:3315" };

		open_Conns();

		String ad_id = other_args.split(COMMA)[0];
		String reg_startdate = other_args.split(COMMA)[1];
		String reg_stopdate = other_args.split(COMMA)[2];
		String stats_startdate = other_args.split(COMMA)[3];
		String stats_stopdate = other_args.split(COMMA)[4];
		String stats_name = other_args.split(COMMA)[5];

		// query user id
		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate) + 1;
		String merge_tab_account = "tab_accountlog_addr";
		drop_login_merge_table(merge_tab_account);
		create_login_merge_table(merge_tab_account, reg_stopdate, reg_date_cnt);

		String tmp_userid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tmp_userid);
		create_tmp_table(tmp_userid);
		sqlstr = "select distinct A.iuin from test.merge_tab_accountlog_addr A where A.media_id=par_ad_id ;";
		sqlstr = sqlstr.replaceAll("par_ad_id", ad_id);
		if ("-1".equalsIgnoreCase(ad_id)) {
			sqlstr = "select distinct A.iuin from test.merge_tab_accountlog_addr A where A.media_id!=0 ;";
		}

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String loadfile = workDir + step + load_suffix;
		FileOutputStream fos = new FileOutputStream(loadfile, false);
		OutputStreamWriter loadosw = new OutputStreamWriter(fos);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			loadosw.write(v1 + NEWLINE);
		}
		loadosw.flush();
		loadosw.close();

		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tmp_userid
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		int date_cnt = cal_DiffDay(stats_stopdate, stats_startdate) + 1;

		String merge_tab_login = "tab_login";
		drop_login_merge_table(merge_tab_login);
		create_login_merge_table(merge_tab_login, stats_stopdate, date_cnt);

		String tmp_tabname1 = tmp_tab_pre + "useronedata";
		drop_login_tmp_table(tmp_tabname1);
		create_login_tmp_table(tmp_tabname1);

		sqlstr = " select B1.iuin , case when B2.iUin is null then 0 else count(B2.iUin) end as login_count from "
				+ tmp_userid
				+ " B1 left join test.merge_tab_login B2 on B1.iuin=B2.iUin group by B1.iuin ;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String insertsql = "replace into " + tmp_tabname1
				+ "(iuin,result_value) values (?,?) ;";
		PreparedStatement insertstmt = loginConn.prepareStatement(insertsql);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			int v2 = rs.getInt(2);
			insertstmt.setInt(1, v1);
			insertstmt.setInt(2, v2);
			insertstmt.addBatch();
		}
		insertstmt.executeBatch();

		// login time and score
		String merge_tab_townlogin = "tab_town_login";
		drop_merge_table(merge_tab_townlogin);
		create_merge_table(merge_tab_townlogin, stats_stopdate, date_cnt);

		sqlstr = "select B1.iuin , case when B2.id is null then 0 else max(B2.online_time) end as login_time , case when B2.id is null then 0 else max(B2.score) end as score from "
				+ tmp_userid
				+ " B1 left join test.merge_tab_town_login B2 on B1.iuin=B2.id group by B1.iuin ;";

		String tmp_tabname2 = tmp_tab_pre + "usertwodata";
		drop_login_tmp_table(tmp_tabname2);
		create_login_tmp_table(tmp_tabname2);

		final String insertsql1 = "replace into " + tmp_tabname2
				+ "(iuin,value1,value2) values (?,?,?) ;";

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {

						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);

						PreparedStatement insertstmt = loginConn
								.prepareStatement(insertsql1);
						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							int v3 = rs.getInt(3);
							insertstmt.setInt(1, v1);
							insertstmt.setInt(2, v2);
							insertstmt.setInt(3, v3);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// 1

		// sqlstr = "select count(distinct A.iuin) from" + tmp_tab_pre
		// + "m3gcn_loginstats_login_count A ;";
		//
		// logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		// stmt = loginConn.createStatement();
		// rs = stmt.executeQuery(sqlstr);
		//
		// int loginiuincount = 0;
		// while (rs.next()) {
		// loginiuincount = rs.getInt(1);
		// }
		// int[] score_count = new int[4];
		//
		// String[] scores = new String[] { "( A.score=0 )",
		// "( A.score>0 and A.score<1500 )", "( A.score=1500 )",
		// "( A.score>1500 )" };
		//
		// String basesql = "select count(distinct A.iuin) from" + tmp_tab_pre
		// + "m3gcn_loginstats_login_timeandscore A where par_score";
		//
		// for (int k = 0; k < scores.length; k++) {
		//
		// sqlstr = basesql.replaceAll("par_score", scores[k]);
		//
		// logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		// stmt = loginConn.createStatement();
		// rs = stmt.executeQuery(sqlstr);
		//
		// while (rs.next()) {
		// score_count[k] = rs.getInt(1);
		// }
		// }
		// resultstr = reg_startdate + TAB + stats_startdate + TAB
		// + loginiuincount + TAB + score_count[0] + TAB + score_count[1]
		// + TAB + score_count[2] + TAB + score_count[3] + NEWLINE;
		// resultosw.write(resultstr);
		// resultosw.flush();

		// 2

		String[] login_counts = new String[] { "( T.result_value =0 )",
				"( T.result_value =1 )", "( T.result_value =2 )",
				"( T.result_value >=3 )", };
		String[] login_times = new String[] {
				"( T.login_time>=0 and T.login_time<300 )",
				"( T.login_time>=300 and T.login_time<3600 )",
				"( T.login_time>=3600 and T.login_time<86400 )",
				"( T.login_time>=86400 )", };
		String[] scores = new String[] { "( T.score=0 )",
				"( T.score>0 and T.score<=100 )",
				"( T.score>100 and T.score<=1500 )", "( T.score>1500 )" };

		String basesql = "select count(*) from ( select A.iuin , A.result_value , sum(B.value1) as login_time, max(B.value2) as score from "
				+ tmp_tabname1
				+ " A , "
				+ tmp_tabname2
				+ " B where A.iuin=B.iuin group by A.iuin ) T where par_login_count and par_login_time and par_score ;";
		Map<String, String> rets = new HashMap<String, String>(64);
		for (int i = 0; i < login_counts.length; i++) {
			for (int j = 0; j < login_times.length; j++) {
				for (int k = 0; k < scores.length; k++) {

					sqlstr = basesql
							.replaceAll("par_login_count", login_counts[i])
							.replaceAll("par_login_time", login_times[j])
							.replaceAll("par_score", scores[k]);

					logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
					printLogStr(logstr);
					stmt = loginConn.createStatement();
					rs = stmt.executeQuery(sqlstr);

					while (rs.next()) {
						int v1 = rs.getInt(1);
						String key = String.valueOf(i + 1) + UNDERLINE
								+ String.valueOf(j + 1) + UNDERLINE
								+ String.valueOf(k + 1);
						rets.put(key, String.valueOf(v1));
					}
				}
			}
		}

		// write to json
		ObjectMapper objectMapper = new ObjectMapper();
		stop_timestamp = spacedatetimeformat.format(new Date());
		resultstr = stats_name + TAB + start_timestamp + TAB + stop_timestamp;
		resultstr = resultstr + TAB + ad_id + TAB + reg_startdate + TAB
				+ reg_stopdate + TAB + stats_startdate + TAB + stats_stopdate;
		resultstr = resultstr + TAB + objectMapper.writeValueAsString(rets)
				+ NEWLINE;
		resultosw.write(resultstr);
		resultosw.flush();

		close_LoginConn();
		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_guozhan_userinfo() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		start_timestamp = spacedatetimeformat.format(new Date());

		ips = new String[] { "114.80.107.133:3306" };
		open_Conns();
		open_LoginConn();
		String guozhan_stopdate = other_args.split(COMMA)[0];
		int guozhan_date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);
		String map_stopdate = other_args.split(COMMA)[2];
		int map_date_cnt = Integer.parseInt(other_args.split(COMMA)[3]);

		// query user id

		String merge_race = "tab_map_race";
		drop_merge_table(merge_race);
		create_merge_table(merge_race, guozhan_stopdate, guozhan_date_cnt);

		String merge_townleave = "tab_town_leave";
		drop_merge_table(merge_townleave);
		create_merge_table(merge_townleave, guozhan_stopdate, guozhan_date_cnt);

		String merge_misc = "tab_misc";
		drop_merge_table(merge_misc);
		create_merge_table(merge_misc, guozhan_stopdate, guozhan_date_cnt);

		String tmp_userid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tmp_userid);
		create_tmp_table(tmp_userid);
		sqlstr = "replace into " + tmp_userid + " select distinct iuin from "
				+ merge_tab_pre + merge_race
				+ " where iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84') ;";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				stmt.execute(sqlstr);

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		String tmp_user_1 = tmp_tab_pre + "userthreedata_1" + UNDERLINE + step;
		drop_tmp_table(tmp_user_1);
		create_tmp_table(tmp_user_1);

		sqlstr = "replace into "
				+ tmp_user_1
				+ " select T0.iuin iuin, count(distinct T1.RaceID) playcnt, count(distinct case when T1.iIsWin=1 then RaceID end) wincnt, count(distinct case when T1.iIsWin=0 then RaceID end) losecnt from "
				+ tmp_userid
				+ " T0 join  "
				+ merge_tab_pre
				+ merge_race
				+ " T1  on T0.iuin=T1.iUin and T1.iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84')  group by T0.iuin ";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				stmt.execute(sqlstr);

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		String tmp_user_2 = tmp_tab_pre + "userthreedata_2" + UNDERLINE + step;
		drop_tmp_table(tmp_user_2);
		create_tmp_table(tmp_user_2);
		sqlstr = "replace into "
				+ tmp_user_2
				+ " select T0.iuin iuin , max(T2.Nation) Nation,max(T2.Score) Score,max(T2.EquipScore) EquipScore from "
				+ tmp_userid + " T0 join  " + merge_tab_pre + merge_townleave
				+ " T2 on T0.iuin=T2.PlayerID  group by T0.iuin ";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				stmt.execute(sqlstr);

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		String tmp_user_3 = tmp_tab_pre + "useronedata_3" + UNDERLINE + step;
		drop_tmp_table(tmp_user_3);
		create_tmp_table(tmp_user_3);
		sqlstr = "replace into "
				+ tmp_user_3
				+ " select T0.iuin iuin,sum(T3.param1) param1 from "
				+ tmp_userid
				+ " T0 join "
				+ merge_tab_pre
				+ merge_misc
				+ " T3 on T0.iuin=T3.char_id and T3.type=9 and T3.sub_type=1607 group by T0.iuin ";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				stmt.execute(sqlstr);

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		sqlstr = "select T11.iuin , T11.value1,T11.value2,T11.value3,T12.value1,T12.value2,T12.value3,T13.result_value from "
				+ tmp_user_1
				+ " T11 , "
				+ tmp_user_2
				+ " T12 , "
				+ tmp_user_3
				+ " T13 where T11.iuin=T12.iuin and T11.iuin=T13.iuin  ";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					int v3 = rs.getInt(3);
					int v4 = rs.getInt(4);
					int v5 = rs.getInt(5);
					int v6 = rs.getInt(6);
					int v7 = rs.getInt(7);
					int v8 = rs.getInt(8);
					resultosw.write(step + TAB + action_date + TAB + v1 + TAB
							+ v2 + TAB + v3 + TAB + v4 + TAB + v5 + TAB + v6
							+ TAB + v7 + TAB + v8 + NEWLINE);
				}

				resultosw.flush();

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		String merge_race2 = "tab_map_race";
		drop_merge_table(merge_race2);
		create_merge_table(merge_race2, map_stopdate, map_date_cnt);

		String merge_guanka = "tab_map_guanka";
		drop_merge_table(merge_guanka);
		create_merge_table(merge_guanka, map_stopdate, map_date_cnt);

		sqlstr = "select iuin , mapid , playcnt from ( "
				+ " select T0.iuin as iuin ,T1.iMapID as mapid , count(distinct T1.RaceID) as playcnt from "
				+ tmp_userid
				+ " T0 left join "
				+ merge_tab_pre
				+ merge_race2
				+ " T1 on T0.iuin=T1.iUin group by T0.iuin,T1.iMapID  "
				+ " union "
				+ " select T0.iuin as iuin ,T1.iMapId as mapid, count(distinct T1.RaceID) as playcnt from "
				+ tmp_userid + " T0 left join " + merge_tab_pre + merge_guanka
				+ " T1 on T0.iuin=T1.iUin group by T0.iuin,T1.iMapId ) T "
				+ " order by iuin , playcnt desc ";
		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					String v2 = rs.getString(2);
					int v3 = rs.getInt(3);
					resultosw.write(step + TAB + action_date + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}

				resultosw.flush();

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_loginstats_byuid() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		start_timestamp = spacedatetimeformat.format(new Date());

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();
		ips = all_ips;
		open_Conns();

		// String ad_id = other_args.split(COMMA)[0];
		String reg_startdate = other_args.split(COMMA)[1];
		String reg_stopdate = other_args.split(COMMA)[2];
		String stats_startdate = other_args.split(COMMA)[3];
		String stats_stopdate = other_args.split(COMMA)[4];
		// String stats_name = other_args.split(COMMA)[5];

		// query user id
		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate) + 1;
		String merge_tab_account = "tab_accountlog_addr";
		drop_login_merge_table(merge_tab_account);
		create_login_merge_table(merge_tab_account, reg_stopdate, reg_date_cnt);

		String tab_uid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tab_uid);
		create_tmp_table(tab_uid);

		String loadfile = "D:/temp/aa.txt";

		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tab_uid
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		int date_cnt = cal_DiffDay(stats_stopdate, stats_startdate) + 1;

		// String merge_tab_login = "tab_login";
		// drop_login_merge_table(merge_tab_login);
		// create_login_merge_table(merge_tab_login, stats_stopdate, date_cnt);
		//
		// tmp_tablename = tmp_tab_pre+"m3gcn_loginstats_login_count";
		// drop_login_tmp_table(tmp_tablename);
		// create_login_tmp_table(tmp_tablename);
		//
		// sqlstr =
		// " select B1.iuin , case when B2.iUin is null then 0 else count(B2.iUin) end as login_count from "
		// + tab_uid
		// +
		// " B1 left join "+merge_tab_pre+"tab_login B2 on B1.iuin=B2.iUin group by B1.iuin ;";
		//
		// logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		// stmt = loginConn.createStatement();
		// rs = stmt.executeQuery(sqlstr);
		//
		// String insertsql = "replace into " + tmp_tablename
		// + "(iuin,login_count) values (?,?) ;";
		// PreparedStatement insertstmt = loginConn.prepareStatement(insertsql);
		// while (rs.next()) {
		// int v1 = rs.getInt(1);
		// int v2 = rs.getInt(2);
		// insertstmt.setInt(1, v1);
		// insertstmt.setInt(2, v2);
		// insertstmt.addBatch();
		// }
		// insertstmt.executeBatch();

		// login time and score
		String merge_tab_townlogin = "tab_town_login";
		drop_merge_table(merge_tab_townlogin);
		create_merge_table(merge_tab_townlogin, stats_stopdate, date_cnt);

		sqlstr = "select B1.iuin , case when B2.id is null then 0 else max(B2.online_time) end as login_time , case when B2.id is null then 0 else max(B2.score) end as score from "
				+ tab_uid
				+ " B1 left join "
				+ merge_tab_pre
				+ "tab_town_login B2 on B1.iuin=B2.id group by B1.iuin ;";

		String tmp_tabname = tmp_tab_pre + "usertwodata" + UNDERLINE + step;
		;
		drop_login_tmp_table(tmp_tabname);
		create_login_tmp_table(tmp_tabname);
		replacesql = "replace into " + tmp_tabname
				+ "(iuin,login_time,score) values (?,?,?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {

						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);

						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);
						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							int v3 = rs.getInt(3);
							insertstmt.setInt(1, v1);
							insertstmt.setInt(2, v2);
							insertstmt.setInt(3, v3);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// sqlstr =
		// "select A.iuin , A.login_count , sum(B.login_time) as login_time, max(B.score) as score from"
		// + tmp_tab_pre+"m3gcn_loginstats_login_count A ," +
		// tmp_tab_pre+"m3gcn_loginstats_login_timeandscore B where A.iuin=B.iuin group by A.iuin ;";
		sqlstr = "select B.iuin , sum(B.login_time) as login_time, max(B.score) as score from"
				+ tmp_tab_pre
				+ "m3gcn_loginstats_login_timeandscore B group by B.iuin ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			int v2 = rs.getInt(2);
			int v3 = rs.getInt(3);
			resultosw.write(v1 + TAB + v2 + TAB + v3 + NEWLINE);
		}

		resultosw.flush();

		close_LoginConn();
		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	// 将用户数据分类
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_classfyuserdata() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		String basedir = "D:/temp/jhh/";
		String[] uidfiles = new String[] { "x_0306_131", "x_0306_360",
				"x_0306_52pk", "x_0306_PPS", "x_0306_verycd", "x_0306_京东",
				"x_0306_净网", "x_0306_叶子猪", "x_0306_多玩", "x_0306_太平洋",
				"x_0306_孔雀资讯", "x_0306_微博", "x_0306_新浪", "x_0306_欣琨网吧",
				"x_0306_爱拍", "x_0306_爱酷游", "x_0306_电玩", "x_0306_网星",
				"x_0306_腾讯", "x_0306_逗游", "x_0306_银橙" };

		String userdatafile = basedir + "get_useraction.result";

		String sql = EMPTY;

		// load result data
		String tab_userdata = tmp_tab_pre + "userstr4data" + UNDERLINE + step;
		drop_login_tmp_table(tab_userdata);
		create_login_tmp_table(tab_userdata);

		sql = "LOAD DATA LOCAL INFILE 'par_file' replace into table par_table fields terminated by '\t' lines terminated by '\n' (iuin,value1,value2,value3,value4) ;";
		sqlstr = sql.replaceAll("par_file", userdatafile).replaceAll(
				"par_table", tab_userdata);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// show map for table i and file
		for (int i = 0; i < uidfiles.length; i++) {
			logstr = i + TAB + uidfiles[i];
			printLogStr(logstr);
		}

		sql = "LOAD DATA LOCAL INFILE 'par_file' replace into table par_table fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		// load data to table
		for (int i = 0; i < uidfiles.length; i++) {

			String tab_uid = tmp_tab_pre + "userid_" + i + UNDERLINE + step;
			drop_login_tmp_table(tab_uid);
			create_login_tmp_table(tab_uid);

			sqlstr = sql.replaceAll("par_file", basedir + uidfiles[i])
					.replaceAll("par_table", tab_uid);
			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			stmt.execute(sqlstr);
		}
		sql = "select t2.* from par_table t1 join " + tab_userdata
				+ " t2 on t1.iuin=t2.iuin ;";

		// query
		for (int i = 0; i < uidfiles.length; i++) {

			String tab_uid = tmp_tab_pre + "userid_" + i + UNDERLINE + step;

			sqlstr = sql.replaceAll("par_table", tab_uid);
			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			FileOutputStream fos = new FileOutputStream(basedir + uidfiles[i]
					+ ".result", false);
			OutputStreamWriter tmposw = new OutputStreamWriter(fos, "UTF8");

			while (rs.next()) {
				String v1 = rs.getString(1);
				String v2 = rs.getString(2);
				String v3 = rs.getString(3);
				String v4 = rs.getString(4);
				String v5 = rs.getString(5);
				tmposw.write(v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB + v5
						+ NEWLINE);
			}
			tmposw.flush();
			tmposw.close();
		}

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void cal_action_dates() throws ParseException {
		action_dates = new String[15];
		for (int m4 = 0; m4 < 15; m4++) {
			String tmp_date = addDay(register_date, m4);
			action_dates[m4] = tmp_date;
		}
	}

	public static void get_loginuser_byip() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		String[] clientips = new String[] { "182.132.251.34",
				"118.125.226.203", "182.132.224.12", "110.188.107.142",
				"171.211.191.113", "110.188.108.32", };
		// String clientips = other_args.split(COMMA)[2];
		String tmp_lastdate = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);
		String merge_tab = "tab_login";
		drop_login_merge_table(merge_tab);
		create_login_merge_table(merge_tab, tmp_lastdate, date_cnt);

		String sql_base = "select B.vClientIp,B.iUin,count(1) from "
				+ merge_tab_pre
				+ "tab_login B where B.vClientIp in (par_clientips) group by B.vClientIp,B.iUin; ";

		String clientipsstr = EMPTY;
		for (int i = 0; i < clientips.length; i++) {
			String clientip = clientips[i];
			clientipsstr = clientipsstr + COMMA + S_QUOTE + clientip + S_QUOTE;
		}
		if (clientipsstr.startsWith(COMMA)) {
			clientipsstr = clientipsstr.substring(1);
		}
		sqlstr = sql_base.replaceAll("par_clientips", clientipsstr);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			String v1 = rs.getString(1);
			int v2 = rs.getInt(2);
			int v3 = rs.getInt(3);
			resultosw.write(v1 + TAB + v2 + TAB + v3 + NEWLINE);
		}
		resultosw.flush();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_login_count_all_byip() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = login_ips;
		open_Conns();
		ad_ids = new int[] { 23155, 23206, 27577, 27579, 27782, 27782, 27782,
				27782, 27782 };
		String[] userlogin_ips = new String[] { "60.15.103.18    ",
				"222.133.42.66   ", "121.22.46.242   ", "218.28.96.10    ",
				"221.211.181.146 ", "60.218.110.134  ", "218.10.254.102  ",
				"221.203.29.9    ", "221.210.126.26  ", };
		String[] v_register_days = new String[] { "2013-08-02", "2013-08-12",
				"2013-08-05", "2013-08-01", "2013-08-12", "2013-08-03",
				"2013-08-06", "2013-08-07", "2013-08-05" };
		String basesql = "select count(B.iUin) from test.merge1_tab_accountlog_addr A , test.merge1_tab_login B where A.iuin=B.iUin and A.media_id in ( par_ad_id ) and (A.dtLogTime>='par_register_starttime' and A.dtLogTime<'par_register_stoptime') and (B.dtLogTime>='par_action_starttime' and B.dtLogTime<'par_action_stoptime') and B.vClientIp in (par_userlogin_ip) limit 1 ;";
		for (int i = 0; i < ad_ids.length; i++) {
			ad_id = ad_ids[i];
			String userlogin_ip = S_QUOTE + userlogin_ips[i].trim() + S_QUOTE;
			String v_register_day = v_register_days[i];

			String register_starttime = v_register_day + SPACE
					+ ZERO_HOURMINSEC;
			String register_stoptime = addDay(v_register_day, 1) + SPACE
					+ ZERO_HOURMINSEC;
			register_date_range = register_starttime + WAVY + register_stoptime;
			for (int j = 0; j < 15; j++) {
				String action_starttime = addDay(v_register_day, j) + SPACE
						+ ZERO_HOURMINSEC;
				String action_stoptime = addDay(v_register_day, j + 1) + SPACE
						+ ZERO_HOURMINSEC;
				action_date_range = action_starttime + WAVY + action_stoptime;
				logstr = ad_id + TAB + userlogin_ip + TAB + action_date_range
						+ TAB + register_date_range;
				printLogStr(logstr);
				resultosw.write(logstr + TAB);
				sqlstr = basesql
						.replaceAll("par_action_starttime", action_starttime)
						.replaceAll("par_action_stoptime", action_stoptime)
						.replaceAll("par_register_starttime",
								register_starttime)
						.replaceAll("par_register_stoptime", register_stoptime)
						.replaceAll("par_ad_id", String.valueOf(ad_id))
						.replaceAll("par_userlogin_ip", userlogin_ip);

				for (int l = 0; l < conns.length; l++) {
					logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
					printLogStr(logstr);
					conn = conns[l];
					try {
						stmt = conn.createStatement();
						rs = stmt.executeQuery(sqlstr);

						if (rs.next()) {
							int v = rs.getInt(1);
							resultosw.write(String.valueOf(v) + NEWLINE);
						}
					} catch (SQLException e) {
						printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
						continue;
					}
				}

				resultosw.flush();
			}
		}
		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_login_count_distinct_byip() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = login_ips;
		open_Conns();
		ad_ids = new int[] { 23155, 23206, 27577, 27579, 27782, 27782, 27782,
				27782, 27782 };
		String[] userlogin_ips = new String[] { "60.15.103.18    ",
				"222.133.42.66   ", "121.22.46.242   ", "218.28.96.10    ",
				"221.211.181.146 ", "60.218.110.134  ", "218.10.254.102  ",
				"221.203.29.9    ", "221.210.126.26  ", };
		String[] v_register_days = new String[] { "2013-08-02", "2013-08-12",
				"2013-08-05", "2013-08-01", "2013-08-12", "2013-08-03",
				"2013-08-06", "2013-08-07", "2013-08-05" };
		String basesql = "select count(distinct B.iUin) from test.merge1_tab_accountlog_addr A , test.merge1_tab_login B where A.iuin=B.iUin and A.media_id in ( par_ad_id ) and (A.dtLogTime>='par_register_starttime' and A.dtLogTime<'par_register_stoptime') and (B.dtLogTime>='par_action_starttime' and B.dtLogTime<'par_action_stoptime') and B.vClientIp in (par_userlogin_ip) limit 1 ;";
		for (int i = 0; i < ad_ids.length; i++) {
			ad_id = ad_ids[i];
			String userlogin_ip = S_QUOTE + userlogin_ips[i].trim() + S_QUOTE;
			String v_register_day = v_register_days[i];

			String register_starttime = v_register_day + SPACE
					+ ZERO_HOURMINSEC;
			String register_stoptime = addDay(v_register_day, 1) + SPACE
					+ ZERO_HOURMINSEC;
			register_date_range = register_starttime + WAVY + register_stoptime;
			for (int j = 0; j < 15; j++) {
				String action_starttime = addDay(v_register_day, j) + SPACE
						+ ZERO_HOURMINSEC;
				String action_stoptime = addDay(v_register_day, j + 1) + SPACE
						+ ZERO_HOURMINSEC;
				action_date_range = action_starttime + WAVY + action_stoptime;
				logstr = ad_id + TAB + userlogin_ip + TAB + action_date_range
						+ TAB + register_date_range;
				printLogStr(logstr);
				resultosw.write(logstr + TAB);
				sqlstr = basesql
						.replaceAll("par_action_starttime", action_starttime)
						.replaceAll("par_action_stoptime", action_stoptime)
						.replaceAll("par_register_starttime",
								register_starttime)
						.replaceAll("par_register_stoptime", register_stoptime)
						.replaceAll("par_ad_id", String.valueOf(ad_id))
						.replaceAll("par_userlogin_ip", userlogin_ip);

				for (int l = 0; l < conns.length; l++) {
					logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
					printLogStr(logstr);
					conn = conns[l];
					try {
						stmt = conn.createStatement();
						rs = stmt.executeQuery(sqlstr);

						if (rs.next()) {
							int v = rs.getInt(1);
							resultosw.write(String.valueOf(v) + NEWLINE);
						}
					} catch (SQLException e) {
						printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
						continue;
					}
				}

				resultosw.flush();
			}
		}
		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_userlevel_bytimerange() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		String tmp_lastdate = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		open_LoginConn();
		ips = all_ips;
		open_Conns();

		String merge_tab_reg = "tab_accountlog_addr";
		drop_merge_table(merge_tab_reg);
		create_merge_table(merge_tab_reg, tmp_lastdate, date_cnt);
		String merge_tab_townlogin = "tab_town_login";
		drop_merge_table(merge_tab_townlogin);
		create_merge_table(merge_tab_townlogin, tmp_lastdate, date_cnt);

		String tmp_tabname = tmp_tab_pre + "m3gcn_userlevel_bytimerange";
		drop_login_tmp_table(tmp_tabname);
		create_login_tmp_table(tmp_tabname);

		sqlstr = sql_userlevel_bytimerange;
		replacesql = "replace into " + tmp_tabname
				+ "(iuin,result_value) values (?,?) ;";
		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				logstr = "conn=" + ips[l] + " start to load to login db ";
				printLogStr(logstr);

				PreparedStatement insertstmt = loginConn
						.prepareStatement(replacesql);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);

					insertstmt.setInt(1, v1);
					insertstmt.setInt(2, v2);
					insertstmt.addBatch();
				}
				insertstmt.executeBatch();

				logstr = "conn=" + ips[l] + " stop to load to login db ";
				printLogStr(logstr);

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		String sql1 = " select count(iuin) from (select iuin, par_userlevel from "
				+ tmp_tabname + "  group by iuin ) T where userlevel<'05' ; ";
		sql1 = sql1.replaceAll("par_userlevel", sql_userlevel_common_1);
		logstr = "conn=" + loginhostportstr + ",sql=" + sql1;
		printLogStr(logstr);
		Statement q1 = loginConn.createStatement();
		ResultSet rs1 = q1.executeQuery(sql1);

		while (rs1.next()) {
			int v1 = rs1.getInt(1);
			resultosw.write(v1 + NEWLINE);
		}

		resultosw.flush();

		drop_merge_table(merge_tab_reg);
		drop_merge_table(merge_tab_townlogin);
		drop_login_tmp_table(tmp_tabname);

		close_LoginConn();
		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	/*
	 * 
	 * 
	 * for i in `ls *.result` ; do
	 * 
	 * cmd=" awk '{a[\$1]+=\$2} END{for(m in a) print
	 * m\"\t\"a[m] }'  $i >> ${i}2 " echo -e "$cmd\n" eval $cmd
	 * 
	 * done
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_newusercard() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String tls1 = EMPTY;
		String tls2 = EMPTY;
		String tls3 = EMPTY;
		String tls4 = EMPTY;
		String tls5 = EMPTY;
		String start_date = other_args.split(COMMA)[0];
		String stop_date = other_args.split(COMMA)[1];
		String cardfile = other_args.split(COMMA)[2];
		BufferedReader rf = new BufferedReader(new FileReader(cardfile));
		date_cnt = cal_DiffDay(stop_date, start_date) + 1;
		drop_merge_table("tab_town_login");
		create_merge_table("tab_town_login", stop_date, date_cnt);
		drop_login_merge_table("tab_login");
		create_login_merge_table("tab_login", stop_date, date_cnt);

		while ((tls1 = rf.readLine()) != null && (tls2 = rf.readLine()) != null) {

			tls1 = tls1 + SPACE + ZERO_HOURMINSEC;
			tls2 = addDay(tls2, 1);
			tls2 = tls2 + SPACE + ZERO_HOURMINSEC;
			tls3 = rf.readLine();
			tls4 = rf.readLine();
			tls5 = rf.readLine();
			rf.readLine();
			rf.readLine();

			logstr = "deal card " + tls3;
			printLogStr(logstr);

			sqlstr = sql_newusercard.replaceAll("par_startint", tls4)
					.replaceAll("par_stopint", tls5)
					.replaceAll("par_starttime", tls1)
					.replaceAll("par_stoptime", tls2);

			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			String loadfile = workDir + "tmp/" + repfilename(tls3)
					+ load_suffix;
			FileOutputStream fos = new FileOutputStream(loadfile, true);
			OutputStreamWriter loadosw = new OutputStreamWriter(fos);
			while (rs.next()) {
				int v1 = rs.getInt(1);
				loadosw.write(v1 + NEWLINE);
			}
			loadosw.flush();
			loadosw.close();

			String tab_newuser = tmp_tab_pre + "userid" + UNDERLINE + step;
			drop_tmp_table(tab_newuser);
			create_tmp_table(tab_newuser);
			sqlstr = "LOAD DATA LOCAL INFILE '"
					+ loadfile
					+ "' replace into table "
					+ tab_newuser
					+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

			ExecutorService exec = null;
			HashMap taskMap = null;

			exec = Executors.newFixedThreadPool(conns.length);
			taskMap = new HashMap<String, Future>();
			for (int l = 0; l < conns.length; l++) {

				final String connStr = ips[l];
				final Connection conn = conns[l];

				Callable call = new Callable() {
					public String call() throws Exception {
						String ret = OK;
						logstr = "conn=" + connStr + ",sql=" + sqlstr;
						printLogStr(logstr);
						try {
							Statement stmt = conn.createStatement();
							stmt.execute(sqlstr);
						} catch (SQLException e) {
							printLogStr(connStr + COMMA + e.getMessage()
									+ NEWLINE);
							ret = FAILED;
						}
						return ret;
					}
				};
				Future task = exec.submit(call);
				taskMap.put(connStr, task);
			}

			Iterator iter = taskMap.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				String key = (String) entry.getKey();
				Future val = (Future) entry.getValue();
				String ret = (String) val.get();
				printLogStr(key + TAB + ret);
			}

			exec.shutdown();

			String ifn = loadfile.substring(loadfile.lastIndexOf("/") + 1,
					loadfile.lastIndexOf(load_suffix));

			// login_count
			current_sub_action = "login_count";
			resultfile = workDir + current_sub_action + UNDERLINE + ifn
					+ result_suffix;
			openResultFile();

			sqlstr = "select AA.iuin , count(1) from " + tab_newuser
					+ " AA left join " + merge_tab_pre
					+ "tab_login BB on  AA.iuin=BB.iuin group by AA.iuin; ";
			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				int v1 = rs.getInt(1);
				int v2 = rs.getInt(2);
				resultosw.write(v1 + TAB + v2 + NEWLINE);
			}
			resultosw.flush();
			closeResultFile();

			String tmp_tabname = tmp_tab_pre + "usertwodata";
			replacesql = "replace into " + tmp_tabname
					+ "(iuin,value1,value2) values (?,?,?) ;";

			// login time and score
			drop_login_tmp_table(tmp_tabname);
			create_login_tmp_table(tmp_tabname);

			sqlstr = "select AA.iuin , max(BB.online_time) , max(BB.score) from "
					+ tab_newuser
					+ " AA left join "
					+ merge_tab_pre
					+ "tab_town_login BB on  AA.iuin=BB.id group by AA.iuin;";

			exec = Executors.newFixedThreadPool(conns.length);
			taskMap = new HashMap<String, Future>();
			for (int l = 0; l < conns.length; l++) {

				final String connStr = ips[l];
				final Connection conn = conns[l];

				Callable call = new Callable() {
					public String call() throws Exception {
						String ret = OK;
						logstr = "conn=" + connStr + ",sql=" + sqlstr;
						printLogStr(logstr);
						try {
							PreparedStatement insertstmt = loginConn
									.prepareStatement(replacesql);
							stmt = conn.createStatement();
							rs = stmt.executeQuery(sqlstr);
							while (rs.next()) {
								int v1 = rs.getInt(1);
								int v2 = rs.getInt(2);
								int v3 = rs.getInt(3);
								insertstmt.setInt(1, v1);
								insertstmt.setInt(2, v2);
								insertstmt.setInt(3, v3);
								insertstmt.addBatch();
							}
							insertstmt.executeBatch();
							insertstmt.close();
						} catch (SQLException e) {
							printLogStr(connStr + COMMA + e.getMessage()
									+ NEWLINE);
							ret = FAILED;
						}
						return ret;
					}
				};
				Future task = exec.submit(call);
				taskMap.put(connStr, task);
			}

			iter = taskMap.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				String key = (String) entry.getKey();
				Future val = (Future) entry.getValue();
				String ret = (String) val.get();
				printLogStr(key + TAB + ret);
			}

			exec.shutdown();

			current_sub_action = "login_time";
			resultfile = workDir + current_sub_action + UNDERLINE + ifn
					+ result_suffix;
			openResultFile();

			sqlstr = "select AA.iuin , sum(AA.value1) from " + tmp_tabname
					+ " AA group by AA.iuin; ";
			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				int v1 = rs.getInt(1);
				int v2 = rs.getInt(2);
				resultosw.write(v1 + TAB + v2 + NEWLINE);
			}
			resultosw.flush();
			closeResultFile();

			// score
			current_sub_action = "score";
			resultfile = workDir + current_sub_action + UNDERLINE + ifn
					+ result_suffix;
			openResultFile();
			sqlstr = "select AA.iuin , max(AA.value2) from " + tmp_tabname
					+ " AA group by AA.iuin; ";
			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				int v1 = rs.getInt(1);
				int v2 = rs.getInt(2);
				resultosw.write(v1 + TAB + v2 + NEWLINE);
			}
			resultosw.flush();
			closeResultFile();
		}

		rf.close();
		close_LoginConn();
		close_Conns();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_createchar_count() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();

		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		// String char_startdate = "2013-12-25";
		// String char_stopdate = "2013-12-29";
		// String loadfile = "D:/temp/uid254.txt";
		String char_startdate = other_args.split(COMMA)[0];
		String char_stopdate = other_args.split(COMMA)[1];
		String loadfile = other_args.split(COMMA)[2];

		String tab_tmpuserid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_login_tmp_table(tab_tmpuserid);
		create_login_tmp_table(tab_tmpuserid);

		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tab_tmpuserid
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query user id
		int date_cnt = cal_DiffDay(char_stopdate, char_startdate) + 1;
		String tmp_tabname = "tab_char_create";
		for (int i = 1; i < 4; i++) {
			String merge_tab_char = tmp_tabname + "_" + i;
			drop_login_merge_table(merge_tab_char);
			create_login_merge_table(merge_tab_char, char_stopdate, date_cnt);
		}

		for (int i = 6; i < 11; i++) {
			String merge_tab_char = tmp_tabname + "_" + i;
			drop_login_merge_table(merge_tab_char);
			create_login_merge_table(merge_tab_char, char_stopdate, date_cnt);
		}

		String basesql = " ( select distinct BB.iUin from " + tab_tmpuserid
				+ " AA join par_tablename BB on AA.iuin=BB.iuin ) union ";

		sqlstr = "select count(distinct iUin) from (";
		for (int i = 1; i < 4; i++) {
			sqlstr += basesql.replaceAll("par_tablename", "" + merge_tab_pre
					+ "tab_char_create_" + i);
		}
		for (int i = 6; i < 11; i++) {
			sqlstr += basesql.replaceAll("par_tablename", "" + merge_tab_pre
					+ "tab_char_create_" + i);
		}
		sqlstr = sqlstr.substring(0, sqlstr.length() - 6);
		sqlstr += " ) T;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultosw.write(loadfile + TAB + char_startdate + TAB + v1
					+ NEWLINE);
		}
		resultosw.flush();

		closeResultFile();

		close_LoginConn();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_regandcrtchar_logincount() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();

		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		String reg_startdate = other_args.split(COMMA)[0];
		String reg_stopdate = other_args.split(COMMA)[1];
		String char_startdate = other_args.split(COMMA)[0];
		String char_stopdate = other_args.split(COMMA)[1];
		String action_startdate = other_args.split(COMMA)[2];
		String action_stopdate = other_args.split(COMMA)[3];

		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate) + 1;
		int action_date_cnt = cal_DiffDay(action_stopdate, action_startdate) + 1;

		// query register user
		String tmp_userid_1 = tmp_tab_pre + "userid_1_" + step;
		drop_login_tmp_table(tmp_userid_1);
		create_login_tmp_table(tmp_userid_1);

		String merge_accountlogaddr = "tab_accountlog_addr";
		drop_login_merge_table(merge_accountlogaddr);
		create_login_merge_table(merge_accountlogaddr, reg_stopdate,
				reg_date_cnt);
		sqlstr = "replace into " + tmp_userid_1 + " select iuin from "
				+ merge_tab_pre + merge_accountlogaddr;

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.executeQuery(sqlstr);

		// query crt char user id
		String tmp_userid_2 = tmp_tab_pre + "userid_2_" + step;
		drop_login_tmp_table(tmp_userid_2);
		create_login_tmp_table(tmp_userid_2);

		int date_cnt = cal_DiffDay(char_stopdate, char_startdate) + 1;
		String merge_charcreate = "tab_char_create";
		for (int i = 1; i < 4; i++) {
			String merge_tab_char = merge_charcreate + "_" + i;
			drop_login_merge_table(merge_tab_char);
			create_login_merge_table(merge_tab_char, char_stopdate, date_cnt);
		}

		for (int i = 6; i < 11; i++) {
			String merge_tab_char = merge_charcreate + "_" + i;
			drop_login_merge_table(merge_tab_char);
			create_login_merge_table(merge_tab_char, char_stopdate, date_cnt);
		}

		String basesql = " ( select distinct T2.iuin from " + tmp_userid_1
				+ " T1 join par_tablename T2 on T1.iuin=T2.iuin ) union ";

		sqlstr = " replace into " + tmp_userid_2 + "  select iuin from (";
		for (int i = 1; i < 4; i++) {
			sqlstr += basesql.replaceAll("par_tablename", "" + merge_tab_pre
					+ merge_charcreate + "_" + i);
		}
		for (int i = 6; i < 11; i++) {
			sqlstr += basesql.replaceAll("par_tablename", "" + merge_tab_pre
					+ merge_charcreate + "_" + i);
		}
		sqlstr = sqlstr.substring(0, sqlstr.length() - 6);
		sqlstr += " ) T;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.executeQuery(sqlstr);

		// final exec
		String merge_login = "tab_login";
		drop_login_merge_table(merge_login);
		create_login_merge_table(merge_login, action_stopdate, action_date_cnt);

		sqlstr = "select count( distinct T1.iuin ) from " + tmp_userid_2
				+ " T1 join " + merge_tab_pre + merge_login
				+ " T2 on T1.iuin =T2.iUin ";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultosw.write(v1 + NEWLINE);
		}

		resultosw.flush();

		closeResultFile();

		close_LoginConn();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_regregion_byuid() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();

		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		String loadfile = "D:/temp/aa.txt";
		String reg_startdate = "2013-05-01";
		String reg_stopdate = "2013-12-20";

		String tab_tmpuserid = tmp_tab_pre + "userid";
		drop_login_tmp_table(tab_tmpuserid);
		create_login_tmp_table(tab_tmpuserid);

		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tab_tmpuserid
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query user id
		int date_cnt = cal_DiffDay(reg_stopdate, reg_startdate) + 1;
		String tmp_tabname = "tab_accountlog_addr";

		drop_login_merge_table(tmp_tabname);
		create_login_merge_table(tmp_tabname, reg_stopdate, date_cnt);

		sqlstr = "select A.iuin ,B.ip, B.country,B.region,B.city,B.dtLogtime from "
				+ tab_tmpuserid
				+ " A left join "
				+ merge_tab_pre
				+ ""
				+ tmp_tabname + " B on A.iuin=B.iuin ;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			String v2 = rs.getString(2);
			String v3 = rs.getString(3);
			String v4 = rs.getString(4);
			String v5 = rs.getString(5);
			String v6 = rs.getString(6);

			resultosw.write(v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB + v5
					+ TAB + v6 + NEWLINE);
		}
		resultosw.flush();

		closeResultFile();

		close_LoginConn();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_passport_all() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();

		action_date_range = "2013-10-08 00:00:00~2013-10-10 00:00:00";
		cal_action_dates_byrange();
		for (int m5 = 0; m5 < action_dates.length; m5++) {
			action_date = action_dates[m5];

			start_date = action_date;
			start_year = start_date.substring(0, 4);
			start_month = start_date.substring(5, 7);
			start_day = start_date.substring(8, 10);

			stop_date = addDay(start_date, 1);
			start_time = ZERO_HOURMINSEC;
			stop_time = ZERO_HOURMINSEC;

			start_datetime = start_date + SPACE + start_time;
			stop_datetime = stop_date + SPACE + stop_time;

			sqlstr = sql_passport_all.replaceAll("par_year", start_year)
					.replaceAll("par_month", start_month)
					.replaceAll("par_day", start_day)
					.replaceAll("par_starttime", start_datetime)
					.replaceAll("par_stoptime", stop_datetime)
					.replaceAll("par_table", action_table);
			for (int l = 0; l < conns.length; l++) {
				logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
				printLogStr(logstr);
				conn = conns[l];
				int zoneId = zoneIds[l];
				try {
					stmt = conn.createStatement();
					rs = stmt.executeQuery(sqlstr);

					while (rs.next()) {
						String v1 = rs.getString(1);
						int v2 = rs.getInt(2);
						resultosw.write(step + TAB + action_date + TAB + zoneId
								+ TAB + v1 + TAB + v2 + NEWLINE);
					}
				} catch (SQLException e) {
					printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
					continue;
				}
			}

			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_login_all_noad() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		register_day_cnt = Integer.parseInt(other_args.split(COMMA)[0]);
		cal_register_dates();
		open_LoginConn();

		for (int j = 0; j < register_dates.length; j++) {
			register_date = register_dates[j];
			logstr = register_date;
			printLogStr(logstr);

			start_date = register_date;
			start_year = start_date.substring(0, 4);
			start_month = start_date.substring(5, 7);
			start_day = start_date.substring(8, 10);

			stop_time = ZERO_HOURMINSEC;
			stop_date = addDay(start_date, 1);

			start_datetime = start_date + SPACE + start_time;
			stop_datetime = stop_date + SPACE + stop_time;

			sqlstr = sql_login_all_noad.replaceAll("par_year", start_year)
					.replaceAll("par_month", start_month)
					.replaceAll("par_day", start_day)
					.replaceAll("par_starttime", start_datetime)
					.replaceAll("par_stoptime", stop_datetime);

			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			if (rs.next()) {
				int v = rs.getInt(1);
				resultosw.write(register_date + TAB + v + NEWLINE);
			}

			resultosw.flush();
		}

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_login_distinct_noad() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		register_day_cnt = Integer.parseInt(other_args.split(COMMA)[0]);
		cal_register_dates();
		open_LoginConn();

		for (int j = 0; j < register_dates.length; j++) {
			register_date = register_dates[j];
			logstr = register_date;
			printLogStr(logstr);

			action_date = register_date;
			start_date = action_date;
			start_year = start_date.substring(0, 4);
			start_month = start_date.substring(5, 7);
			start_day = start_date.substring(8, 10);

			stop_time = ZERO_HOURMINSEC;
			stop_date = addDay(start_date, 1);

			start_datetime = start_date + SPACE + start_time;
			stop_datetime = stop_date + SPACE + stop_time;

			sqlstr = sql_login_distinct_noad.replaceAll("par_year", start_year)
					.replaceAll("par_month", start_month)
					.replaceAll("par_day", start_day)
					.replaceAll("par_starttime", start_datetime)
					.replaceAll("par_stoptime", stop_datetime);

			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			if (rs.next()) {
				int v = rs.getInt(1);
				resultosw.write(step + TAB + action_date + TAB + v + NEWLINE);
			}

			resultosw.flush();
		}

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_register_noad() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		register_day_cnt = Integer.parseInt(other_args.split(COMMA)[0]);
		cal_register_dates();
		open_LoginConn();

		for (int j = 0; j < register_dates.length; j++) {
			register_date = register_dates[j];
			logstr = register_date;
			printLogStr(logstr);

			start_date = register_date;
			start_year = start_date.substring(0, 4);
			start_month = start_date.substring(5, 7);
			start_day = start_date.substring(8, 10);

			stop_time = ZERO_HOURMINSEC;
			stop_date = addDay(start_date, 1);

			start_datetime = start_date + SPACE + start_time;
			stop_datetime = stop_date + SPACE + stop_time;

			sqlstr = sql_register_noad.replaceAll("par_year", start_year)
					.replaceAll("par_month", start_month)
					.replaceAll("par_day", start_day)
					.replaceAll("par_starttime", start_datetime)
					.replaceAll("par_stoptime", stop_datetime);

			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			if (rs.next()) {
				int v = rs.getInt(1);
				resultosw.write(register_date + TAB + v + NEWLINE);
			}

			resultosw.flush();
		}

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_user_login_count() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		cal_register_dates();

		String tmp_lastdate = other_args.split(COMMA)[1];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[2]);

		open_LoginConn();

		String merge_tab = "tab_login";
		drop_login_merge_table(merge_tab);
		create_login_merge_table(merge_tab, tmp_lastdate, date_cnt);

		String sql_user_login_count = "select BB.iUin , count(1) from ( select A.iuin from `tab_accountlog_addr_par_register_year-par_register_month`.`par_register_day` A where A.media_id=par_mediaid ) AA join `tab_login_par_year-par_month`.`par_day` BB on AA.iuin=BB.iUin group by BB.iUin;";
		for (int i = 0; i < ad_ids.length; i++) {
			ad_id = ad_ids[i];
			// register_dates = register_dates_dates[i];
			for (int j = 0; j < register_dates.length; j++) {
				register_date = register_dates[j];

				logstr = ad_id + COMMA + register_date;
				printLogStr(logstr);

				register_year = register_date.substring(0, 4);
				register_month = register_date.substring(5, 7);
				register_day = register_date.substring(8, 10);
				action_date = register_date;
				start_year = action_date.substring(0, 4);
				start_month = action_date.substring(5, 7);
				start_day = action_date.substring(8, 10);

				sqlstr = sql_user_login_count
						.replaceAll("par_register_year", register_year)
						.replaceAll("par_register_month", register_month)
						.replaceAll("par_register_day", register_day)
						.replaceAll("par_year", start_year)
						.replaceAll("par_month", start_month)
						.replaceAll("par_day", start_day)
						.replaceAll("par_mediaid", String.valueOf(ad_id));

				logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = loginConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					Integer v1 = rs.getInt(1);
					String v2 = rs.getString(2);
					if (v2 == null)
						v2 = "0";
					resultosw.write(ad_id + TAB + register_date + TAB + v1
							+ TAB + v2 + NEWLINE);
				}
				resultosw.flush();

			}
		}

		drop_login_merge_table(merge_tab);

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_user_login_count_bytimerange() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		cal_register_dates();

		String tmp_lastdate_login = other_args.split(COMMA)[1];
		int date_cnt_login = Integer.parseInt(other_args.split(COMMA)[2]);

		String tmp_lastdate_accountlog = other_args.split(COMMA)[3];
		int date_cnt_accountlog = Integer.parseInt(other_args.split(COMMA)[4]);

		open_LoginConn();

		String merge_tab_login = "tab_login";
		drop_login_merge_table(merge_tab_login);
		create_login_merge_table(merge_tab_login, tmp_lastdate_login,
				date_cnt_login);

		String merge_tab_accountlog = "tab_accountlog_addr";
		drop_login_merge_table(merge_tab_accountlog);
		create_login_merge_table(merge_tab_accountlog, tmp_lastdate_accountlog,
				date_cnt_accountlog);

		String sql_user_login_count = "select AA.iuin , count(1) from `test`.`merge_tab_accountlog_addr` AA join `test`.`merge_tab_login` BB on AA.iuin=BB.iUin and AA.media_id=par_mediaid group by AA.iuin;";
		for (int i = 0; i < ad_ids.length; i++) {
			ad_id = ad_ids[i];

			sqlstr = sql_user_login_count.replaceAll("par_mediaid",
					String.valueOf(ad_id));

			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			Integer v1;
			Integer v2;
			while (rs.next()) {
				v1 = rs.getInt(1);
				v2 = rs.getInt(2);
				if (v2 == null)
					v2 = 0;
				resultosw.write(ad_id + TAB + v1 + TAB + v2 + NEWLINE);
			}
			resultosw.flush();
			System.gc();
			System.runFinalization();
		}

		drop_login_merge_table(merge_tab_login);
		drop_login_merge_table(merge_tab_accountlog);

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_user_login_time_bytimerange() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		cal_register_dates();

		String tmp_lastdate_login = other_args.split(COMMA)[1];
		int date_cnt_login = Integer.parseInt(other_args.split(COMMA)[2]);

		String tmp_lastdate_accountlog = other_args.split(COMMA)[3];
		int date_cnt_accountlog = Integer.parseInt(other_args.split(COMMA)[4]);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String merge_tab_townlogin = "tab_town_login";
		drop_merge_table(merge_tab_townlogin);
		create_merge_table(merge_tab_townlogin, tmp_lastdate_login,
				date_cnt_login);

		String merge_tab_accountlog = "tab_accountlog_addr";
		drop_merge_table(merge_tab_accountlog);
		create_merge_table(merge_tab_accountlog, tmp_lastdate_accountlog,
				date_cnt_accountlog);

		String sql_user_login_time = "select AA.iuin , max(BB.online_time)  from `test`.`merge_tab_accountlog_addr` AA join `test`.`merge_tab_town_login` BB on AA.iuin=BB.id and AA.media_id=par_mediaid group by AA.iUin;";
		for (int i = 0; i < ad_ids.length; i++) {
			ad_id = ad_ids[i];

			sqlstr = sql_user_login_time.replaceAll("par_mediaid",
					String.valueOf(ad_id));

			for (int l = 0; l < conns.length; l++) {
				logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
				printLogStr(logstr);
				conn = conns[l];
				try {
					stmt = conn.createStatement();
					rs = stmt.executeQuery(sqlstr);
					Integer v1;
					Integer v2;
					while (rs.next()) {
						v1 = rs.getInt(1);
						v2 = rs.getInt(2);
						if (v2 == null)
							v2 = 0;
						resultosw.write(ad_id + TAB + v1 + TAB + v2 + NEWLINE);
					}
					resultosw.flush();

				} catch (SQLException e) {
					printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
					continue;
				}
			}

			System.gc();
		}

		drop_merge_table(merge_tab_townlogin);
		drop_merge_table(merge_tab_accountlog);

		close_LoginConn();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_user_score_bytimerange() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		cal_register_dates();

		String tmp_lastdate_login = other_args.split(COMMA)[1];
		int date_cnt_login = Integer.parseInt(other_args.split(COMMA)[2]);

		String tmp_lastdate_accountlog = other_args.split(COMMA)[3];
		int date_cnt_accountlog = Integer.parseInt(other_args.split(COMMA)[4]);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String merge_tab_townlogin = "tab_town_login";
		drop_merge_table(merge_tab_townlogin);
		create_merge_table(merge_tab_townlogin, tmp_lastdate_login,
				date_cnt_login);

		String merge_tab_accountlog = "tab_accountlog_addr";
		drop_merge_table(merge_tab_accountlog);
		create_merge_table(merge_tab_accountlog, tmp_lastdate_accountlog,
				date_cnt_accountlog);

		String sql_user_score = "select AA.iuin , max(BB.score)  from `test`.`merge_tab_accountlog_addr` AA join `test`.`merge_tab_town_login` BB on AA.iuin=BB.id and AA.media_id=par_mediaid group by AA.iUin;";
		for (int i = 0; i < ad_ids.length; i++) {
			ad_id = ad_ids[i];

			sqlstr = sql_user_score.replaceAll("par_mediaid",
					String.valueOf(ad_id));

			for (int l = 0; l < conns.length; l++) {
				logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
				printLogStr(logstr);
				conn = conns[l];
				try {
					stmt = conn.createStatement();
					rs = stmt.executeQuery(sqlstr);
					Integer v1;
					Integer v2;
					while (rs.next()) {
						v1 = rs.getInt(1);
						v2 = rs.getInt(2);
						if (v2 == null)
							v2 = 0;
						resultosw.write(ad_id + TAB + v1 + TAB + v2 + NEWLINE);
					}
					resultosw.flush();

				} catch (SQLException e) {
					printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
					continue;
				}
			}

			System.gc();
		}

		drop_merge_table(merge_tab_townlogin);
		drop_merge_table(merge_tab_accountlog);

		close_LoginConn();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_user_score_time() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		cal_register_dates();

		String tmp_lastdate_login = other_args.split(COMMA)[1];
		int date_cnt_login = Integer.parseInt(other_args.split(COMMA)[2]);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String merge_tab_townlogin = "tab_town_login";
		drop_merge_table(merge_tab_townlogin);
		create_merge_table(merge_tab_townlogin, tmp_lastdate_login,
				date_cnt_login);

		// String cond = other_args.split(COMMA)[3];
		String cond = "maxscore>1500 and maxtime>=3600";

		for (int j = 0; j < register_dates.length; j++) {
			register_date = register_dates[j];
			logstr = register_date;
			printLogStr(logstr);
			action_date = register_date;
			start_date = action_date;
			start_year = start_date.substring(0, 4);
			start_month = start_date.substring(5, 7);
			start_day = start_date.substring(8, 10);

			stop_date = addDay(start_date, 1);
			start_time = ZERO_HOURMINSEC;
			stop_time = ZERO_HOURMINSEC;

			start_datetime = start_date + SPACE + start_time;
			stop_datetime = stop_date + SPACE + stop_time;

			// String cond = " maxscore=0 ";
			sqlstr = sql_user_score_time
					.replaceAll("par_maxscore_maxtime", cond)
					.replaceAll("par_starttime", start_datetime)
					.replaceAll("par_stoptime", stop_datetime);

			for (int l = 0; l < conns.length; l++) {
				logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
				printLogStr(logstr);
				conn = conns[l];

				try {
					stmt = conn.createStatement();
					rs = stmt.executeQuery(sqlstr);
					Integer v1;
					while (rs.next()) {
						v1 = rs.getInt(1);
						resultosw.write(step + TAB + action_date + TAB + cond
								+ TAB + v1 + NEWLINE);
					}
					resultosw.flush();

				} catch (SQLException e) {
					printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
					continue;
				}
			}
		}
		drop_merge_table(merge_tab_townlogin);

		close_LoginConn();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_user_login_time() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		cal_register_dates();

		ips = all_ips;
		open_Conns();

		String sql_user_login_time = "select BB.id , group_concat(BB.online_time order by BB.online_time asc)  from ( select A.iuin from `tab_accountlog_addr_par_register_year-par_register_month`.`par_register_day` A where A.media_id=par_mediaid ) AA join `tab_town_login_par_year-par_month`.`par_day` BB on AA.iuin=BB.id group by BB.id;";
		for (int i = 0; i < ad_ids.length; i++) {
			ad_id = ad_ids[i];
			// register_dates = register_dates_dates[i];
			for (int j = 0; j < register_dates.length; j++) {
				register_date = register_dates[j];

				logstr = ad_id + COMMA + register_date;
				printLogStr(logstr);

				register_year = register_date.substring(0, 4);
				register_month = register_date.substring(5, 7);
				register_day = register_date.substring(8, 10);
				action_date = register_date;
				start_year = action_date.substring(0, 4);
				start_month = action_date.substring(5, 7);
				start_day = action_date.substring(8, 10);

				sqlstr = sql_user_login_time
						.replaceAll("par_register_year", register_year)
						.replaceAll("par_register_month", register_month)
						.replaceAll("par_register_day", register_day)
						.replaceAll("par_year", start_year)
						.replaceAll("par_month", start_month)
						.replaceAll("par_day", start_day)
						.replaceAll("par_mediaid", String.valueOf(ad_id));

				for (int l = 0; l < conns.length; l++) {
					logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
					printLogStr(logstr);
					conn = conns[l];
					try {
						stmt = conn.createStatement();
						rs = stmt.executeQuery(sqlstr);

						while (rs.next()) {
							Integer v1 = rs.getInt(1);
							String v2 = rs.getString(2);
							if (v2 == null)
								v2 = "0";
							resultosw.write(ad_id + TAB + register_date + TAB
									+ v1 + TAB + v2 + NEWLINE);
						}
						resultosw.flush();
					} catch (SQLException e) {
						printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
						continue;
					}
				}
			}
		}

		close_Conns();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_guanka_byuid() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String tmp_lastdate = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		String tab_map_guanka = "tab_map_guanka";
		drop_merge_table(tab_map_guanka);
		create_merge_table(tab_map_guanka, tmp_lastdate, date_cnt);

		sqlstr = "select A.iuin,B.iMapId ,B.iWin ,B.iBossNum ,B.iAcBoss ,B.iacBossNum ,B.iMapNum ,B.iRecordTime from test.tab_logined_uid A join "
				+ merge_tab_pre + "tab_map_guanka B on A.iuin = B.iUin ;";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				while (rs.next()) {
					int v1 = rs.getInt(1);
					String v2 = rs.getString(2);
					String v3 = rs.getString(3);
					String v4 = rs.getString(4);
					String v5 = rs.getString(5);
					String v6 = rs.getString(6);
					String v7 = rs.getString(7);
					String v8 = rs.getString(8);
					resultosw.write(v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB
							+ v5 + TAB + v6 + TAB + v7 + TAB + v8 + NEWLINE);
				}

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		drop_merge_table(tab_map_guanka);

		close_Conns();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_race_byuid() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String tmp_lastdate = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		String tab_map_race = "tab_map_race";
		drop_merge_table(tab_map_race);
		create_merge_table(tab_map_race, tmp_lastdate, date_cnt);

		sqlstr = "select A.iuin,B.iMapID ,B.iIsWin ,B.iTime ,B.iRecordTime  from test.tab_logined_uid A join "
				+ merge_tab_pre + "tab_map_race B on A.iuin = B.iUin ;";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				while (rs.next()) {
					int v1 = rs.getInt(1);
					String v2 = rs.getString(2);
					String v3 = rs.getString(3);
					String v4 = rs.getString(4);
					String v5 = rs.getString(5);
					resultosw.write(v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB
							+ v5 + NEWLINE);

				}

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		drop_merge_table(tab_map_race);

		close_Conns();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_item_byuid() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String tmp_lastdate = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		String tab_item = "tab_item";
		drop_merge_table(tab_item);
		create_merge_table(tab_item, tmp_lastdate, date_cnt);

		// sqlstr =
		// "select A.iuin,B.iChangeType ,B.iGoodsState ,B.iReason ,B.iGoodsId ,B.iGoodsType ,B.iBeforeNum ,B.iAfterNum ,B.dtLogTime ,B.iGoodsPrice ,B.iIsNewPay  from test.tab_logined_uid A join "+merge_tab_pre+"tab_item B on A.iuin = B.iUin and B.iChangeType in (20,21);";
		// 6
		// for (int l = 0; l < conns.length; l++) {
		// logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
		// printLogStr(logstr);
		// conn = conns[l];
		// try {
		// stmt = conn.createStatement();
		// rs = stmt.executeQuery(sqlstr);
		//
		// while (rs.next()) {
		// int v1 = rs.getInt(1);
		// String v2 = rs.getString(2);
		// String v3 = rs.getString(3);
		// String v4 = rs.getString(4);
		// String v5 = rs.getString(5);
		// String v6 = rs.getString(6);
		// String v7 = rs.getString(7);
		// String v8 = rs.getString(8);
		// String v9 = rs.getString(9);
		// String v10 = rs.getString(10);
		// String v11 = rs.getString(11);
		// resultosw.write(v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB
		// + v5 + TAB + v6 + TAB + v7 + TAB + v8 + TAB + v9
		// + TAB + v10 + TAB + v11 + NEWLINE);
		// }
		//
		// } catch (SQLException e) {
		// printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
		// continue;
		// }
		// }
		//
		// drop_tmp_merge_table(tab_item);

		close_Conns();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_money_byuid() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String tmp_lastdate = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		String tab_m3g_money = "tab_m3g_money";
		drop_merge_table(tab_m3g_money);
		create_merge_table(tab_m3g_money, tmp_lastdate, date_cnt);

		sqlstr = "select A.iuin,B.iPayType ,B.iFromUin ,B.iToUin ,B.iParam ,B.iGoodsId ,B.iGoodsType ,B.iGoodNum ,B.dtLogTime  from test.tab_logined_uid A join "
				+ merge_tab_pre
				+ "tab_m3g_money B on A.iuin = B.iUin and B.iPayType=4;";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				while (rs.next()) {
					int v1 = rs.getInt(1);
					String v2 = rs.getString(2);
					String v3 = rs.getString(3);
					String v4 = rs.getString(4);
					String v5 = rs.getString(5);
					String v6 = rs.getString(6);
					String v7 = rs.getString(7);
					String v8 = rs.getString(8);
					String v9 = rs.getString(9);
					resultosw.write(v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB
							+ v5 + TAB + v6 + TAB + v7 + TAB + v8 + TAB + v9
							+ NEWLINE);
				}

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		drop_merge_table(tab_m3g_money);

		close_Conns();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_xxx() throws Exception {
		// 打开文件（日志文件，临时文件，结果文件）
		// 变量初始化赋值
		// 打开数据库连接（allip，localip，loginip）
		// 创建临时表
		// 执行sql语句，这里可能有临时结果集，需要有多步
		// 关闭文件
		// 关闭连接
	}

	public static void init_datetime() throws ParseException {
		register_year = register_date.substring(0, 4);
		register_month = register_date.substring(5, 7);
		register_day = register_date.substring(8, 10);

		start_date = action_date;
		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);

		stop_date = addDay(start_date, 1);
		start_time = ZERO_HOURMINSEC;
		stop_time = ZERO_HOURMINSEC;

		start_datetime = start_date + SPACE + start_time;
		stop_datetime = stop_date + SPACE + stop_time;
	}

	public static void init_hourdatetime() throws ParseException {
		register_year = register_date.substring(0, 4);
		register_month = register_date.substring(5, 7);
		register_day = register_date.substring(8, 10);

		start_date = action_date;
		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);
		start_time = action_hour + ZERO_MINSEC;

		start_datetime = start_date + SPACE + start_time;
		stop_datetime = addHour(start_datetime, 1);
		stop_datetime = stop_date + SPACE + stop_time;

		stop_date = stop_datetime.substring(0, 10);
		stop_time = stop_datetime.substring(11, 19);
	}

	public static void main(String[] args) throws Exception {

		runOut(args);
	}

	public static void runOut(String[] args) throws Exception {
		Options opts = new Options();
		opts.addOption("h", "help", false, "");
		opts.addOption("w", "workDir", true, "");
		opts.addOption("a", "action_day", true, "");
		opts.addOption("s", "step", true, "");
		opts.addOption("i", "ad_ids", true, "");
		opts.addOption("o", "other", true, "");
		BasicParser parser = new BasicParser();
		CommandLine cl = null;

		cl = parser.parse(opts, args);
		if (cl.getOptions().length > 0) {
			if (cl.hasOption('h')) {
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("Options", opts);
			} else {
				workDir = cl.getOptionValue("w");
				if (!(workDir.endsWith("/") || workDir.endsWith("\\"))) {
					workDir = workDir + "/";
				}
				action_date = cl.getOptionValue("a");
				step = cl.getOptionValue("s");

				if (cl.hasOption('i')) {
					tmpstr = cl.getOptionValue("i");
					String[] tmp = tmpstr.split(COMMA);
					int idlen = tmp.length;
					if (idlen > 0) {
						ad_ids = new int[idlen];
						for (int i = 0; i < idlen; i++) {
							ad_ids[i] = Integer.valueOf(tmp[i]);
						}
					}
				}

				if (cl.hasOption('o')) {
					other_args = cl.getOptionValue("o");
				}
				runIn();
			}
		} else {
			System.out
					.println("-w D:\\temp\\stats\\ -a  2013-07-20 -s get_login_hours -l 500 -d 5 -i 23200,310,27580");
		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void runIn() throws Exception {
		if (action_date == null || EMPTY.equals(action_date)) {
			action_date = dateformat.format(new Date());
			action_date = addDay(action_date, -1);
		}

		workDir = workDir + action_date + "/";
		initDir(workDir);
		tmpDir = workDir + "tmp/";
		initDir(tmpDir);

		Class c = DianHun_Temp.class;
		Method m = c.getMethod(step);
		m.invoke(c);

	}

	public DianHun_Temp() {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void get_userlevel_byuid() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		String tmp_lastdate = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		open_LoginConn();
		ips = all_ips;
		open_Conns();

		String tmp_tabname = tmp_tab_pre + "m3gcn_userlevel_byuid";
		drop_login_tmp_table(tmp_tabname);
		create_login_tmp_table(tmp_tabname);

		String merge_tab_townlogin = "tab_town_login";
		drop_merge_table(merge_tab_townlogin);
		create_merge_table(merge_tab_townlogin, tmp_lastdate, date_cnt);

		String sql1 = "select AA.iuin ,max(BB.score)  from test.tab_mass_uid AA join "
				+ merge_tab_pre
				+ "tab_town_login BB on AA.iuin=BB.id group by AA.iuin; ";
		sqlstr = sql1;
		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				logstr = "conn=" + ips[l] + " start to load to login db ";
				printLogStr(logstr);

				String insertsql = "replace into " + tmp_tabname
						+ "(iuin,result_value) values (?,?) ;";
				PreparedStatement insertstmt = loginConn
						.prepareStatement(insertsql);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);

					insertstmt.setInt(1, v1);
					insertstmt.setInt(2, v2);
					insertstmt.addBatch();
				}
				insertstmt.executeBatch();

				logstr = "conn=" + ips[l] + " stop to load to login db ";
				printLogStr(logstr);

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		// drop_login_tmp_table(tmp_tablename_userlevel);
		drop_merge_table(merge_tab_townlogin);

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_userpay_byuid() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		String tmp_lastdate = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		open_LoginConn();
		ips = all_ips;
		open_Conns();

		String tmp_tabname = tmp_tab_pre + "m3gcn_money_byuid";
		drop_login_tmp_table(tmp_tabname);
		create_login_tmp_table(tmp_tabname);

		String merge_tabname = "tab_m3g_money";
		drop_merge_table(merge_tabname);
		create_merge_table(merge_tabname, tmp_lastdate, date_cnt);

		String sql1 = "select distinct AA.iuin from test.tab_logined_uid AA join "
				+ merge_tab_pre + "tab_m3g_money BB on AA.iuin=BB.iUin ; ";
		sqlstr = sql1;
		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				logstr = "conn=" + ips[l] + " start to load to login db ";
				printLogStr(logstr);

				String insertsql = "replace into " + tmp_tabname
						+ "(iuin) values (?) ;";
				PreparedStatement insertstmt = loginConn
						.prepareStatement(insertsql);
				while (rs.next()) {
					int v1 = rs.getInt(1);

					insertstmt.setInt(1, v1);
					insertstmt.addBatch();
				}
				insertstmt.executeBatch();

				logstr = "conn=" + ips[l] + " stop to load to login db ";
				// printLogStr(logstr);

			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		drop_merge_table(merge_tabname);

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_mine() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = new String[] { "114.80.107.133:3306" };
		open_Conns();

		String tablename = "tab_mine_opr";
		int daycnt = Integer.parseInt(other_args.split(COMMA)[0]);
		create_merge_table(tablename, action_date, daycnt);

		String[] descs = new String[] { "mine_bazhan_free,各个矿洞发生的免费霸占次数",
				"mine_bazhan_charge,各个矿洞发生的收费霸占次数",
				"mine_bazhan_win,各个矿洞霸占结果为胜利的次数",
				"mine_bazhan_fail,各个矿洞霸占结果为失败的次数",
				"mine_bazhan_isoccupy,霸占时驻守方为无人驻守状态的次数",
				"mine_bazhan_notoccupy,霸占时驻守方为玩家军团的次数",
				"mine_qiangduo_free,各个矿洞发生的免费抢夺次数",
				"mine_qiangduo_charge,各个矿洞发生的收费抢夺次数",
				"mine_qiangduo_win,各个矿洞抢夺结果为胜利的次数",
				"mine_qiangduo_fail,各个矿洞抢夺结果为失败的次数",
				"mine_qiangduo_isoccupy,抢夺时驻守方为无人驻守状态的次数",
				"mine_qiangduo_notoccupy,抢夺时驻守方为玩家军团的次数", };
		for (int m = 0; m < descs.length; m++) {
			String action_name = descs[m].split(COMMA)[0];
			String action_desc = descs[m].split(COMMA)[1];
			for (int i = 0; i < daycnt; i++) {
				start_date = addDay(action_date, 0 - i);
				stop_date = addDay(start_date, 1);
				start_time = ZERO_HOURMINSEC;
				stop_time = ZERO_HOURMINSEC;

				start_datetime = start_date + SPACE + start_time;
				stop_datetime = stop_date + SPACE + stop_time;

				if (action_name.equalsIgnoreCase("mine_bazhan_free")) {
					sqlstr = sql_mine_bazhan_free.replaceAll("par_starttime",
							start_datetime).replaceAll("par_stoptime",
							stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_bazhan_charge")) {
					sqlstr = sql_mine_bazhan_charge.replaceAll("par_starttime",
							start_datetime).replaceAll("par_stoptime",
							stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_bazhan_win")) {
					sqlstr = sql_mine_bazhan_win.replaceAll("par_starttime",
							start_datetime).replaceAll("par_stoptime",
							stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_bazhan_fail")) {
					sqlstr = sql_mine_bazhan_fail.replaceAll("par_starttime",
							start_datetime).replaceAll("par_stoptime",
							stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_bazhan_isoccupy")) {
					sqlstr = sql_mine_bazhan_isoccupy.replaceAll(
							"par_starttime", start_datetime).replaceAll(
							"par_stoptime", stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_bazhan_notoccupy")) {
					sqlstr = sql_mine_bazhan_notoccupy.replaceAll(
							"par_starttime", start_datetime).replaceAll(
							"par_stoptime", stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_qiangduo_free")) {
					sqlstr = sql_mine_qiangduo_free.replaceAll("par_starttime",
							start_datetime).replaceAll("par_stoptime",
							stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_qiangduo_charge")) {
					sqlstr = sql_mine_qiangduo_charge.replaceAll(
							"par_starttime", start_datetime).replaceAll(
							"par_stoptime", stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_qiangduo_win")) {
					sqlstr = sql_mine_qiangduo_win.replaceAll("par_starttime",
							start_datetime).replaceAll("par_stoptime",
							stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_qiangduo_fail")) {
					sqlstr = sql_mine_qiangduo_fail.replaceAll("par_starttime",
							start_datetime).replaceAll("par_stoptime",
							stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_qiangduo_isoccupy")) {
					sqlstr = sql_mine_qiangduo_isoccupy.replaceAll(
							"par_starttime", start_datetime).replaceAll(
							"par_stoptime", stop_datetime);
				}
				if (action_name.equalsIgnoreCase("mine_qiangduo_notoccupy")) {
					sqlstr = sql_mine_qiangduo_notoccupy.replaceAll(
							"par_starttime", start_datetime).replaceAll(
							"par_stoptime", stop_datetime);
				}

				for (int l = 0; l < conns.length; l++) {
					logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
					printLogStr(logstr);
					conn = conns[l];
					try {
						stmt = conn.createStatement();
						rs = stmt.executeQuery(sqlstr);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							resultosw.write(start_date + TAB + action_desc
									+ TAB + v1 + TAB + v2 + NEWLINE);
						}
					} catch (SQLException e) {
						printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
						continue;
					}
				}
			}
		}

		drop_merge_table(tablename);
		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_mine_hours() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String tablename = "tab_mine_opr";
		int daycnt = Integer.parseInt(other_args.split(COMMA)[0]);
		drop_merge_table(tablename);
		create_merge_table(tablename, action_date, daycnt);

		String[] descs = new String[] { "mine_bazhan_free,各个矿洞发生的免费霸占次数",
				"mine_bazhan_charge,各个矿洞发生的收费霸占次数",
				"mine_bazhan_win,各个矿洞霸占结果为胜利的次数",
				"mine_bazhan_fail,各个矿洞霸占结果为失败的次数",
				"mine_bazhan_isoccupy,霸占时驻守方为无人驻守状态的次数",
				"mine_bazhan_notoccupy,霸占时驻守方为玩家军团的次数",
				"mine_qiangduo_free,各个矿洞发生的免费抢夺次数",
				"mine_qiangduo_charge,各个矿洞发生的收费抢夺次数",
				"mine_qiangduo_win,各个矿洞抢夺结果为胜利的次数",
				"mine_qiangduo_fail,各个矿洞抢夺结果为失败的次数",
				"mine_qiangduo_isoccupy,抢夺时驻守方为无人驻守状态的次数",
				"mine_qiangduo_notoccupy,抢夺时驻守方为玩家军团的次数", };
		for (int m = 0; m < descs.length; m++) {
			String action_name = descs[m].split(COMMA)[0];
			String action_desc = descs[m].split(COMMA)[1];
			for (int i = 0; i < daycnt; i++) {

				for (int j = 0; j < days_hours.length; j++) {
					action_hour = days_hours[j];

					start_date = addDay(action_date, 0 - i);
					start_time = action_hour + ZERO_MINSEC;

					start_datetime = start_date + SPACE + start_time;
					stop_datetime = addHour(start_datetime, 1);

					stop_date = stop_datetime.substring(0, 10);
					stop_time = stop_datetime.substring(11, 19);

					if (action_name.equalsIgnoreCase("mine_bazhan_free")) {
						sqlstr = sql_mine_bazhan_free.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_bazhan_charge")) {
						sqlstr = sql_mine_bazhan_charge.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_bazhan_win")) {
						sqlstr = sql_mine_bazhan_win.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_bazhan_fail")) {
						sqlstr = sql_mine_bazhan_fail.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_bazhan_isoccupy")) {
						sqlstr = sql_mine_bazhan_isoccupy.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_bazhan_notoccupy")) {
						sqlstr = sql_mine_bazhan_notoccupy.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_qiangduo_free")) {
						sqlstr = sql_mine_qiangduo_free.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_qiangduo_charge")) {
						sqlstr = sql_mine_qiangduo_charge.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_qiangduo_win")) {
						sqlstr = sql_mine_qiangduo_win.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_qiangduo_fail")) {
						sqlstr = sql_mine_qiangduo_fail.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_qiangduo_isoccupy")) {
						sqlstr = sql_mine_qiangduo_isoccupy.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}
					if (action_name.equalsIgnoreCase("mine_qiangduo_notoccupy")) {
						sqlstr = sql_mine_qiangduo_notoccupy.replaceAll(
								"par_starttime", start_datetime).replaceAll(
								"par_stoptime", stop_datetime);
					}

					for (int l = 0; l < conns.length; l++) {
						logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
						printLogStr(logstr);
						conn = conns[l];
						int zoneId = zoneIds[l];
						try {
							stmt = conn.createStatement();
							rs = stmt.executeQuery(sqlstr);

							while (rs.next()) {
								int v1 = rs.getInt(1);
								int v2 = rs.getInt(2);
								resultosw.write(start_datetime + TAB + zoneId
										+ TAB + action_desc + TAB + v1 + TAB
										+ v2 + NEWLINE);
							}
						} catch (SQLException e) {
							printLogStr(ips[l] + COMMA + e.getMessage()
									+ NEWLINE);
							continue;
						}
					}
					resultosw.flush();
				}
			}
		}

		drop_merge_table(tablename);
		close_Conns();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_testloaddata_bybatch() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();
		sqlstr = "select distinct iuin from `tab_login_2013-11`.`01` limit 1000;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String tab_lostuid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tab_lostuid);
		create_tmp_table(tab_lostuid);

		sqlstr = "replace into " + tab_lostuid + "(iuin) values (?) ;";
		// load user to all database
		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		final Lock lock = new ReentrantLock();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						PreparedStatement insertstmt = conn
								.prepareStatement(sqlstr);
						lock.lock();
						try {
							rs.beforeFirst();
							logstr = "conn=" + connStr + ",start to addBatch";
							printLogStr(logstr);
							while (rs.next()) {
								int v1 = rs.getInt(1);
								insertstmt.setInt(1, v1);
								insertstmt.addBatch();
							}
							logstr = "conn=" + connStr + ",stop to addBatch";
							printLogStr(logstr);
						} finally {
							lock.unlock();
						}
						logstr = "conn=" + connStr + ",start to executeBatch";
						printLogStr(logstr);
						insertstmt.executeBatch();
						insertstmt.close();
						logstr = "conn=" + connStr + ",stop to executeBatch";
						printLogStr(logstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		close_Conns();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_testloaddata_byremoteload() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();
		sqlstr = "select distinct iuin from `tab_login_2013-11`.`01` limit 10000;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultosw.write(v1 + NEWLINE);
		}
		resultosw.flush();

		String tab_lostuid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tab_lostuid);
		create_tmp_table(tab_lostuid);

		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ resultfile
				+ "' replace into table "
				+ tab_lostuid
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						logstr = "conn=" + connStr + ",start to load";
						printLogStr(logstr);
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
						logstr = "conn=" + connStr + ",stop to load";
						printLogStr(logstr);

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		close_Conns();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_testrs() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();
		sqlstr = "select distinct iuin from `tab_login_2013-11`.`01` limit 10;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			printLogStr(String.valueOf(v1));
		}

		rs.beforeFirst();
		while (rs.next()) {
			int v1 = rs.getInt(1);
			printLogStr(String.valueOf(v1));
		}
		rs.beforeFirst();
		while (rs.next()) {
			int v1 = rs.getInt(1);
			printLogStr(String.valueOf(v1));
		}
		rs.close();
		stmt.close();

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_useractions_aftercall() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String uidfile = "D:/temp/calluserid.txt";

		String tab_newuser = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tab_newuser);
		create_tmp_table(tab_newuser);
		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ uidfile
				+ "' replace into table "
				+ tab_newuser
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		drop_login_merge_table("tab_login");
		create_login_merge_table("tab_login", "2013-12-08", 22);
		drop_merge_table("tab_town_login");
		create_merge_table("tab_town_login", "2013-12-08", 80);
		// drop_merge_table("tab_item_sell");
		// create_merge_table("tab_item_sell", "2013-12-08", 22);
		// drop_merge_table("tab_map_race");
		// create_merge_table("tab_map_race", "2013-12-08", 22);
		// drop_merge_table("tab_map_guanka");
		// create_merge_table("tab_map_guanka", "2013-12-08", 22);

		// login
		current_sub_action = "login_count";
		resultfile = workDir + current_sub_action + result_suffix;
		openResultFile();

		sqlstr = "select AA.iuin , count(BB.iuin) from "
				+ tab_newuser
				+ " AA left join "
				+ merge_tab_pre
				+ "tab_login BB on AA.iuin=BB.iuin and BB.dtLogTime>='2013-11-19' group by AA.iuin; ";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			int v2 = rs.getInt(2);
			resultosw.write(v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();
		closeResultFile();

		String tmp_tabname = tmp_tab_pre + "useronedata";
		replacesql = "replace into " + tmp_tabname
				+ "(iuin,result_value) values (?,?) ;";
		// score
		drop_login_tmp_table(tmp_tabname);
		create_login_tmp_table(tmp_tabname);

		current_sub_action = "score";
		resultfile = workDir + current_sub_action + result_suffix;
		openResultFile();

		sqlstr = "select T1.iuin , T1.score-T2.score as score from "
				+ "( select AA.iuin , max(BB.score) as score from "
				+ tab_newuser
				+ " AA left join "
				+ merge_tab_pre
				+ "tab_town_login BB on AA.iuin=BB.id and BB.record_time >='2013-11-19' group by AA.iuin ) T1 "
				+ " , ( select AA.iuin , max(BB.score) as score from "
				+ tab_newuser
				+ " AA left join "
				+ merge_tab_pre
				+ "tab_town_login BB on AA.iuin=BB.id and BB.record_time <'2013-11-19' group by AA.iuin ) T2 "
				+ "where T1.iuin = T2.iuin ;";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				PreparedStatement insertstmt = loginConn
						.prepareStatement(replacesql);
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					insertstmt.setInt(1, v1);
					insertstmt.setInt(2, v2);
					insertstmt.addBatch();
				}
				insertstmt.executeBatch();
				insertstmt.close();
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		sqlstr = "select AA.iuin , max(AA.result_value) from " + tmp_tabname
				+ " AA group by AA.iuin; ";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			int v2 = rs.getInt(2);
			resultosw.write(v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();
		closeResultFile();

		// comsume
		drop_login_tmp_table(tmp_tabname);
		create_login_tmp_table(tmp_tabname);

		current_sub_action = "comsume";
		resultfile = workDir + current_sub_action + result_suffix;
		openResultFile();

		sqlstr = "select AA.iuin , sum(BB.iGoodsPrice) from "
				+ tab_newuser
				+ " AA left join "
				+ merge_tab_pre
				+ "tab_item_sell BB on AA.iuin=BB.iUin and BB.iReason in (1,1000) and BB.dtLogTime >='2013-11-19' group by AA.iuin ;";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				PreparedStatement insertstmt = loginConn
						.prepareStatement(replacesql);
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					insertstmt.setInt(1, v1);
					insertstmt.setInt(2, v2);
					insertstmt.addBatch();
				}
				insertstmt.executeBatch();
				insertstmt.close();
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}
		sqlstr = "select AA.iuin , sum(AA.result_value) from " + tmp_tabname
				+ " AA group by AA.iuin; ";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			int v2 = rs.getInt(2);
			resultosw.write(v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();
		closeResultFile();

		// guanka
		drop_login_tmp_table(tmp_tabname);
		create_login_tmp_table(tmp_tabname);

		current_sub_action = "guanka";
		resultfile = workDir + current_sub_action + result_suffix;
		openResultFile();

		sqlstr = "select AA.iuin , count(BB.iUin) from "
				+ tab_newuser
				+ " AA left join "
				+ merge_tab_pre
				+ "tab_map_guanka BB on AA.iuin=BB.iUin and BB.iRecordTime >='2013-11-19' group by AA.iuin ;";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				PreparedStatement insertstmt = loginConn
						.prepareStatement(replacesql);
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					insertstmt.setInt(1, v1);
					insertstmt.setInt(2, v2);
					insertstmt.addBatch();
				}
				insertstmt.executeBatch();
				insertstmt.close();
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}
		sqlstr = "select AA.iuin , sum(AA.result_value) from " + tmp_tabname
				+ " AA group by AA.iuin; ";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			int v2 = rs.getInt(2);
			resultosw.write(v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();
		closeResultFile();

		// race
		drop_login_tmp_table(tmp_tabname);
		create_login_tmp_table(tmp_tabname);

		current_sub_action = "race";
		resultfile = workDir + current_sub_action + result_suffix;
		openResultFile();

		sqlstr = "select AA.iuin , count(BB.iUin) from "
				+ tab_newuser
				+ " AA left join "
				+ merge_tab_pre
				+ "tab_map_race BB on AA.iuin=BB.iUin and BB.iRecordTime >='2013-11-19' group by AA.iuin ;";

		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			try {
				PreparedStatement insertstmt = loginConn
						.prepareStatement(replacesql);
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					insertstmt.setInt(1, v1);
					insertstmt.setInt(2, v2);
					insertstmt.addBatch();
				}
				insertstmt.executeBatch();
				insertstmt.close();
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}
		sqlstr = "select AA.iuin , sum(AA.result_value) from " + tmp_tabname
				+ " AA group by AA.iuin; ";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			int v2 = rs.getInt(2);
			resultosw.write(v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();
		closeResultFile();

		close_LoginConn();
		close_Conns();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	/**
	 * 计算流失用户流失前的等级
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_lostuser_leveldistribution() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String nologin_startdate = other_args.split(COMMA)[0];
		String nologin_stopdate = other_args.split(COMMA)[1];
		String lastdate = addDay(nologin_startdate, -1);
		date_cnt = Integer.parseInt(lastdate.substring(8, 10));

		// query before user
		String tmp_tabname1 = tmp_tab_pre + "userid_before";
		drop_login_tmp_table(tmp_tabname1);
		create_login_tmp_table(tmp_tabname1);

		String merge_tabname1 = "tab_login";
		drop_login_merge_table(merge_tabname1);
		create_login_merge_table(merge_tabname1, lastdate, date_cnt);

		sqlstr = "replace into " + tmp_tabname1 + " select distinct iUin from "
				+ merge_tab_pre + merge_tabname1;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query after user
		String tmp_tabname2 = tmp_tab_pre + "userid_after";
		drop_login_tmp_table(tmp_tabname2);
		create_login_tmp_table(tmp_tabname2);

		String merge_tabname2 = "tab_login";
		int after_datecount = cal_DiffDay(nologin_stopdate, nologin_startdate);
		drop_login_merge_table(merge_tabname2);
		create_login_merge_table(merge_tabname2, addDay(nologin_stopdate, -1),
				after_datecount);

		sqlstr = "replace into " + tmp_tabname2 + " select distinct iUin from "
				+ merge_tab_pre + merge_tabname2;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query lost user

		sqlstr = "select distinct A.iuin from " + tmp_tabname1
				+ " A left join " + tmp_tabname2
				+ " B on A.iuin = B.iuin where B.iuin is null ;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String loadfile = workDir + step + load_suffix;
		FileOutputStream fos = new FileOutputStream(loadfile, true);
		OutputStreamWriter loadosw = new OutputStreamWriter(fos);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			loadosw.write(v1 + NEWLINE);
		}
		loadosw.flush();
		loadosw.close();

		String tmp_tabname3 = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tmp_tabname3);
		create_tmp_table(tmp_tabname3);
		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tmp_tabname3
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// query user score from all database to login database

		String merge_tabname3 = "tab_town_login";
		drop_merge_table(merge_tabname3);
		create_merge_table(merge_tabname3, lastdate, date_cnt);

		String tmp_tabname4 = tmp_tab_pre + "useronedata";
		drop_login_tmp_table(tmp_tabname4);
		create_login_tmp_table(tmp_tabname4);
		sqlstr = "select A.iuin , max(B.score) from " + tmp_tabname3
				+ " A join " + merge_tab_pre + merge_tabname3
				+ " B on A.iuin=B.id group by A.iuin;";
		replacesql = "replace into " + tmp_tabname4
				+ "(iuin,result_value) values (?,?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							insertstmt.setInt(1, v1);
							insertstmt.setInt(2, v2);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();
						insertstmt.close();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();
		// query user level distribution
		sqlstr = "select userlevel,count(iuin) as levelcnt from ( select iuin, par_userlevel from "
				+ tmp_tabname4 + " group by iuin ) AA group by userlevel ; ";
		sqlstr = sqlstr.replaceAll("par_userlevel", sql_userlevel_common
				.replaceAll("result_value", "max(result_value)"));
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			String v1 = rs.getString(1);
			int v2 = rs.getInt(2);
			resultosw.write(nologin_startdate + TAB + v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_reguser_leveldistribution() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String ad_id = other_args.split(COMMA)[0];
		String reg_startdate = other_args.split(COMMA)[1];
		String reg_stopdate = other_args.split(COMMA)[2];
		String action_startdate = other_args.split(COMMA)[3];
		String action_stopdate = other_args.split(COMMA)[4];

		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate);
		int action_date_cnt = cal_DiffDay(action_stopdate, action_startdate);

		// query register user

		String merge_accountlogaddr = "tab_accountlog_addr";
		drop_login_merge_table(merge_accountlogaddr);
		create_login_merge_table(merge_accountlogaddr, reg_stopdate,
				reg_date_cnt);

		sqlstr = "select iuin from " + merge_tab_pre + merge_accountlogaddr
				+ " where media_id = par_media_id ";
		sqlstr = sqlstr.replaceAll("par_media_id", ad_id);

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String loadfile = workDir + step + load_suffix;
		FileOutputStream fos = new FileOutputStream(loadfile, false);
		OutputStreamWriter loadosw = new OutputStreamWriter(fos);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			loadosw.write(v1 + NEWLINE);
		}
		loadosw.flush();
		loadosw.close();

		String tmp_userid_1 = tmp_tab_pre + "userid_1" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_1);
		create_tmp_table(tmp_userid_1);
		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tmp_userid_1
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// query user score from all database to login database

		String merge_townleave = "tab_town_leave";
		drop_merge_table(merge_townleave);
		create_merge_table(merge_townleave, action_stopdate, action_date_cnt);

		String tmp_useronedata = tmp_tab_pre + "useronedata";
		drop_login_tmp_table(tmp_useronedata);
		create_login_tmp_table(tmp_useronedata);
		sqlstr = "select A.iuin , max(B.Score) from " + tmp_userid_1
				+ " A join " + merge_tab_pre + merge_townleave
				+ " B on A.iuin=B.PlayerID group by A.iuin;";
		replacesql = "replace into " + tmp_useronedata
				+ "(iuin,result_value) values (?,?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							insertstmt.setInt(1, v1);
							insertstmt.setInt(2, v2);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();
						insertstmt.close();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();
		// query user level distribution
		sqlstr = "select userlevel,count(iuin) as levelcnt from ( select iuin, par_userlevel from "
				+ tmp_useronedata + " group by iuin ) AA group by userlevel ; ";
		sqlstr = sqlstr.replaceAll("par_userlevel", sql_userlevel_common
				.replaceAll("result_value", "max(result_value)"));
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			String v1 = rs.getString(1);
			int v2 = rs.getInt(2);
			resultosw.write(ad_id + TAB + reg_startdate + TAB + reg_stopdate
					+ TAB + action_startdate + TAB + action_stopdate + TAB + v1
					+ TAB + v2 + NEWLINE);
		}
		resultosw.flush();

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_reguser_actions() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String reg_startdate = other_args.split(COMMA)[0];
		String reg_stopdate = other_args.split(COMMA)[1];
		String action_startdate = other_args.split(COMMA)[2];
		String action_stopdate = other_args.split(COMMA)[3];

		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate);
		int action_date_cnt = cal_DiffDay(action_stopdate, action_startdate);

		// query register user

		String merge_accountlogaddr = "tab_accountlog_addr";
		drop_login_merge_table(merge_accountlogaddr);
		create_login_merge_table(merge_accountlogaddr, reg_stopdate,
				reg_date_cnt);

		sqlstr = "select iuin from " + merge_tab_pre + merge_accountlogaddr;

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String loadfile = workDir + step + load_suffix;
		FileOutputStream fos = new FileOutputStream(loadfile, false);
		OutputStreamWriter loadosw = new OutputStreamWriter(fos);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			loadosw.write(v1 + NEWLINE);
		}
		loadosw.flush();
		loadosw.close();

		String tmp_userid_1 = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_1);
		create_tmp_table(tmp_userid_1);
		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tmp_userid_1
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// query user score from all database to login database

		for (int i = 0; i < action_date_cnt; i++) {
			String tmpdate = addDay(action_startdate, i);

			String merge_login = "tab_login";
			drop_login_merge_table(merge_login);
			create_login_merge_table(merge_login, tmpdate, 1);

			sqlstr = "select count(distinct T1.iuin) from " + tmp_userid_1
					+ " T1 , " + merge_tab_pre + merge_login
					+ " T2 where T1.iuin = T2.iUin";

			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				int v1 = rs.getInt(1);
				resultosw.write(tmpdate + TAB + v1 + NEWLINE);
			}
			resultosw.flush();
		}

		String merge_townlogin = "tab_town_login";
		drop_merge_table(merge_townlogin);
		create_merge_table(merge_townlogin, action_stopdate, action_date_cnt
				+ reg_date_cnt);

		String tmp_one = tmp_tab_pre + "useronedata_1";
		drop_login_tmp_table(tmp_one);
		create_login_tmp_table(tmp_one);
		sqlstr = "select T1.iuin , max(T2.score) from " + tmp_userid_1
				+ " T1 join " + merge_tab_pre + merge_townlogin
				+ " T2 on T1.iuin=T2.id group by T1.iuin;";
		replacesql = "replace into " + tmp_one
				+ "(iuin,result_value) values (?,?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							insertstmt.setInt(1, v1);
							insertstmt.setInt(2, v2);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();
						insertstmt.close();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		sqlstr = " select count( distinct iuin) from " + tmp_one
				+ " where result_value>0 ; ";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultosw.write(v1 + NEWLINE);
		}
		resultosw.flush();

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_reguser_charcreate() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		String ad_id = other_args.split(COMMA)[0];
		String reg_startdate = other_args.split(COMMA)[1];
		String reg_stopdate = other_args.split(COMMA)[2];
		String action_startdate = other_args.split(COMMA)[3];
		String action_stopdate = other_args.split(COMMA)[4];

		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate);
		int action_date_cnt = cal_DiffDay(action_stopdate, action_startdate);

		// query register user
		String tmp_userid_1 = tmp_tab_pre + "userid_1" + UNDERLINE + step;
		drop_login_tmp_table(tmp_userid_1);
		create_login_tmp_table(tmp_userid_1);

		String merge_accountlogaddr = "tab_accountlog_addr";
		drop_login_merge_table(merge_accountlogaddr);
		create_login_merge_table(merge_accountlogaddr, reg_stopdate,
				reg_date_cnt);

		sqlstr = "replace into " + tmp_userid_1 + " (iuin) select iuin from "
				+ merge_tab_pre + merge_accountlogaddr
				+ " where media_id = par_media_id ";
		sqlstr = sqlstr.replaceAll("par_media_id", ad_id);

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		String tmp_tabname = "tab_char_create";
		for (int i = 1; i < 4; i++) {
			String merge_tab_char = tmp_tabname + "_" + i;
			drop_login_merge_table(merge_tab_char);
			create_login_merge_table(merge_tab_char, action_stopdate,
					action_date_cnt);
		}

		for (int i = 6; i < 11; i++) {
			String merge_tab_char = tmp_tabname + "_" + i;
			drop_login_merge_table(merge_tab_char);
			create_login_merge_table(merge_tab_char, action_stopdate,
					action_date_cnt);
		}

		String basesql = " ( select distinct BB.iUin from " + tmp_userid_1
				+ " AA join par_tablename BB on AA.iuin=BB.iuin ) union ";

		sqlstr = "select count(distinct iUin) from (";
		for (int i = 1; i < 4; i++) {
			sqlstr += basesql.replaceAll("par_tablename", "" + merge_tab_pre
					+ "tab_char_create_" + i);
		}
		for (int i = 6; i < 11; i++) {
			sqlstr += basesql.replaceAll("par_tablename", "" + merge_tab_pre
					+ "tab_char_create_" + i);
		}
		sqlstr = sqlstr.substring(0, sqlstr.length() - 6);
		sqlstr += " ) T;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultosw.write(ad_id + TAB + reg_startdate + TAB + reg_stopdate
					+ TAB + action_startdate + TAB + action_stopdate + TAB + v1
					+ NEWLINE);
		}
		resultosw.flush();

		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_reguser_actions_1() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String reg_startdate = other_args.split(COMMA)[0];
		String reg_stopdate = other_args.split(COMMA)[1];
		String action_startdate = other_args.split(COMMA)[2];
		String action_stopdate = other_args.split(COMMA)[3];

		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate) + 1;
		int action_date_cnt = cal_DiffDay(action_stopdate, action_startdate) + 1;

		// query register user

		String merge_accountlogaddr = "tab_accountlog_addr";
		drop_login_merge_table(merge_accountlogaddr);
		create_login_merge_table(merge_accountlogaddr, reg_stopdate,
				reg_date_cnt);

		sqlstr = "select iuin from " + merge_tab_pre + merge_accountlogaddr
				+ " where media_id = 13498 " + SEMI;

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String loadfile = workDir + step + load_suffix;
		FileOutputStream fos = new FileOutputStream(loadfile, false);
		OutputStreamWriter loadosw = new OutputStreamWriter(fos);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			loadosw.write(v1 + NEWLINE);
		}
		loadosw.flush();
		loadosw.close();

		String tmp_userid_1 = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_1);
		create_tmp_table(tmp_userid_1);
		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tmp_userid_1
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// login count
		String merge_login = "tab_login";
		drop_login_merge_table(merge_login);
		create_login_merge_table(merge_login, action_stopdate, action_date_cnt);

		String tmp_userone = tmp_tab_pre + "useronedata";
		drop_login_tmp_table(tmp_userone);
		create_login_tmp_table(tmp_userone);

		sqlstr = "replace into "
				+ tmp_userone
				+ " select T1.iuin , case when T2.iUin is null then 0 else count(T2.iUin) end as login_count from "
				+ tmp_userid_1 + " T1 left join " + merge_tab_pre + merge_login
				+ " T2 on T1.iuin=T2.iUin group by T1.iuin ;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		// login time and score
		String merge_townlogin = "tab_town_leave";
		drop_merge_table(merge_townlogin);
		create_merge_table(merge_townlogin, action_stopdate, action_date_cnt);

		String tmp_two = tmp_tab_pre + "usertwodata_1";
		drop_login_tmp_table(tmp_two);
		create_login_tmp_table(tmp_two);
		sqlstr = "select T1.iuin , case when T2.PlayerID is null then 0 else max(T2.OnlineTime) end ,"
				+ " case when T2.PlayerID is null then 0 else max(T2.Score) end from "
				+ tmp_userid_1
				+ " T1 left join "
				+ merge_tab_pre
				+ merge_townlogin
				+ " T2 on T1.iuin=T2.PlayerID group by T1.iuin;";
		replacesql = "replace into " + tmp_two
				+ "(iuin,value1,value2) values (?,?,?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							int v3 = rs.getInt(3);
							insertstmt.setInt(1, v1);
							insertstmt.setInt(2, v2);
							insertstmt.setInt(3, v3);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();
						insertstmt.close();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		sqlstr = " select T1.iuin ,max(T1.ip),max(T1.dtLogtime), max(T2.result_value) , max(T3.value1) , max(T3.value2) from "
				+ merge_tab_pre
				+ merge_accountlogaddr
				+ " T1 , "
				+ tmp_userone
				+ " T2 , "
				+ tmp_two
				+ " T3 where T1.media_id = 13498 and T1.iuin=T2.iuin and T1.iuin = T3.iuin group by T1.iuin"
				+ SEMI;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			String v2 = rs.getString(2);
			String v3 = rs.getString(3);
			int v4 = rs.getInt(4);
			int v5 = rs.getInt(5);
			int v6 = rs.getInt(6);
			resultosw.write(v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB + v5
					+ TAB + v6 + TAB + NEWLINE);
		}
		resultosw.flush();

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_reguser_score() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String reg_startdate = other_args.split(COMMA)[0];
		String reg_stopdate = other_args.split(COMMA)[1];
		String action_startdate = other_args.split(COMMA)[2];
		String action_stopdate = other_args.split(COMMA)[3];
		String ad_id = other_args.split(COMMA)[4];
		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate) + 1;
		int action_date_cnt = cal_DiffDay(action_stopdate, action_startdate) + 1;

		// query register user

		String merge_accountlogaddr = "tab_accountlog_addr";
		drop_login_merge_table(merge_accountlogaddr);
		create_login_merge_table(merge_accountlogaddr, reg_stopdate,
				reg_date_cnt);

		sqlstr = "select A.iuin from test.merge_tab_accountlog_addr A where A.media_id=par_ad_id ;";
		sqlstr = sqlstr.replaceAll("par_ad_id", ad_id);
		if ("-1".equalsIgnoreCase(ad_id)) {
			sqlstr = "select A.iuin from test.merge_tab_accountlog_addr A where A.media_id!=0 ;";
		}

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String loadfile = workDir + step + load_suffix;
		FileOutputStream fos = new FileOutputStream(loadfile, false);
		OutputStreamWriter loadosw = new OutputStreamWriter(fos);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			loadosw.write(v1 + NEWLINE);
		}
		loadosw.flush();
		loadosw.close();

		String tmp_userid_1 = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_1);
		create_tmp_table(tmp_userid_1);
		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tmp_userid_1
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// query user score from all database to login database

		String merge_townlogin = "tab_town_leave";
		drop_merge_table(merge_townlogin);
		create_merge_table(merge_townlogin, action_stopdate, action_date_cnt);

		String tmp_userone = tmp_tab_pre + "useronedata_1";
		drop_login_tmp_table(tmp_userone);
		create_login_tmp_table(tmp_userone);
		sqlstr = "select T1.iuin , max(T2.Score) from " + tmp_userid_1
				+ " T1 join " + merge_tab_pre + merge_townlogin
				+ " T2 on T1.iuin=T2.PlayerID group by T1.iuin;";
		replacesql = "replace into " + tmp_userone
				+ "(iuin,result_value) values (?,?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							insertstmt.setInt(1, v1);
							insertstmt.setInt(2, v2);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();
						insertstmt.close();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		sqlstr = " select count( distinct iuin) from " + tmp_userone
				+ " where result_value>0 ; ";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultosw.write(v1 + NEWLINE);
		}
		resultosw.flush();

		sqlstr = " select iuin, par_userlevel from " + tmp_userone
				+ " where result_value>=1973500 group by iuin " + SEMI;
		sqlstr = sqlstr.replaceAll("par_userlevel", sql_userlevel_common
				.replaceAll("result_value", "max(result_value)"));
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			String v2 = rs.getString(2);
			resultosw.write(reg_startdate + TAB + v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_reguser_savecount() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String reg_startdate = other_args.split(COMMA)[0];
		String reg_stopdate = other_args.split(COMMA)[1];
		String action_startdate = other_args.split(COMMA)[2];
		String action_stopdate = other_args.split(COMMA)[3];
		String ad_id = other_args.split(COMMA)[4];
		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate) + 1;
		int action_date_cnt = cal_DiffDay(action_stopdate, action_startdate) + 1;

		// query register user

		String merge_accountlogaddr = "tab_accountlog_addr";
		drop_login_merge_table(merge_accountlogaddr);
		create_login_merge_table(merge_accountlogaddr, reg_stopdate,
				reg_date_cnt);

		String tmp_userid_1 = tmp_tab_pre + "userid_1" + UNDERLINE + step;
		drop_login_tmp_table(tmp_userid_1);
		create_login_tmp_table(tmp_userid_1);

		sqlstr = "replace into "
				+ tmp_userid_1
				+ " select A.iuin from test.merge_tab_accountlog_addr A where A.media_id=par_ad_id ;";
		sqlstr = sqlstr.replaceAll("par_ad_id", ad_id);
		if ("-1".equalsIgnoreCase(ad_id)) {
			sqlstr = "replace into "
					+ tmp_userid_1
					+ " select A.iuin from test.merge_tab_accountlog_addr A where A.media_id!=0 ;";
		}

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		for (int i = 0; i < action_date_cnt; i++) {
			String tmpdate = addDay(action_startdate, i);

			String merge_login = "tab_login";
			drop_login_merge_table(merge_login);
			create_login_merge_table(merge_login, tmpdate, 1);

			sqlstr = "select count(distinct T1.iuin) from " + tmp_userid_1
					+ " T1 , " + merge_tab_pre + merge_login
					+ " T2 where T1.iuin = T2.iUin";

			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				int v1 = rs.getInt(1);
				resultosw.write(tmpdate + TAB + v1 + NEWLINE);
			}
			resultosw.flush();
		}

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_aduser_regcnt() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		String reg_startdate = other_args.split(COMMA)[0];
		String reg_stopdate = other_args.split(COMMA)[1];
		String ad_id = other_args.split(COMMA)[2];
		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate);

		// query register user

		String merge_accountlogaddr = "tab_accountlog_addr";
		drop_login_merge_table(merge_accountlogaddr);
		create_login_merge_table(merge_accountlogaddr, reg_stopdate,
				reg_date_cnt);

		sqlstr = "select count(iuin) from " + merge_tab_pre
				+ merge_accountlogaddr + " where media_id =" + ad_id;

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultosw.write(ad_id + TAB + v1 + NEWLINE);
		}
		resultosw.flush();

		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_returnuserid_action() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String login_lastdate = other_args.split(COMMA)[0];
		int login_date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		String nologin_lastdate = other_args.split(COMMA)[2];
		int nologin_date_cnt = Integer.parseInt(other_args.split(COMMA)[3]);

		String return_lastdate = other_args.split(COMMA)[4];
		int return_date_cnt = Integer.parseInt(other_args.split(COMMA)[5]);

		// query before user
		String tmp_userid_1 = tmp_tab_pre + "userid_before";
		drop_login_tmp_table(tmp_userid_1);
		create_login_tmp_table(tmp_userid_1);

		String merge_login_1 = "tab_login";
		drop_login_merge_table(merge_login_1);
		create_login_merge_table(merge_login_1, login_lastdate, login_date_cnt);

		sqlstr = "replace into " + tmp_userid_1 + " select distinct iUin from "
				+ merge_tab_pre + merge_login_1;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query after user
		String tmp_userid_2 = tmp_tab_pre + "userid_after";
		drop_login_tmp_table(tmp_userid_2);
		create_login_tmp_table(tmp_userid_2);

		String merge_login_2 = "tab_login";

		drop_login_merge_table(merge_login_2);
		create_login_merge_table(merge_login_2, nologin_lastdate,
				nologin_date_cnt);

		sqlstr = "replace into " + tmp_userid_2 + " select distinct iUin from "
				+ merge_tab_pre + merge_login_2;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query lost user
		String tmp_userid_3 = tmp_tab_pre + "userid_lost";
		drop_login_tmp_table(tmp_userid_3);
		create_login_tmp_table(tmp_userid_3);
		sqlstr = "replace into " + tmp_userid_3
				+ " select distinct A.iuin from " + tmp_userid_1
				+ " A left join " + tmp_userid_2
				+ " B on A.iuin = B.iuin where B.iuin is null ;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.executeQuery(sqlstr);

		sqlstr = "select count(iuin) from " + tmp_userid_3;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultstr = login_lastdate + TAB + login_date_cnt + TAB + v1
					+ NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		// query return user

		String merge_login = "tab_login";
		drop_login_merge_table(merge_login);
		create_login_merge_table(merge_login, return_lastdate, return_date_cnt);

		sqlstr = "select distinct A.iuin from " + tmp_userid_3 + " A join "
				+ merge_tab_pre + merge_login + " B on A.iuin = B.iuin  ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String loadfile = workDir + step + load_suffix;
		FileOutputStream fos = new FileOutputStream(loadfile, true);
		OutputStreamWriter loadosw = new OutputStreamWriter(fos);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			loadosw.write(v1 + NEWLINE);
		}
		loadosw.flush();
		loadosw.close();

		// load return user to other db
		String tmp_tabname4 = tmp_tab_pre + "userid_return" + UNDERLINE + step;
		drop_tmp_table(tmp_tabname4);
		create_tmp_table(tmp_tabname4);
		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tmp_tabname4
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		sqlstr = "select count(iuin) from " + tmp_tabname4;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultstr = login_lastdate + TAB + login_date_cnt + TAB + v1
					+ NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		// query user score from all database to login database

		String merge_townleave = "tab_town_login";
		drop_merge_table(merge_townleave);
		create_merge_table(merge_townleave, return_lastdate, return_date_cnt);

		String tmp_tabname5 = tmp_tab_pre + "useronedata";
		drop_login_tmp_table(tmp_tabname5);
		create_login_tmp_table(tmp_tabname5);
		sqlstr = "select A.iuin , max(B.score) from " + tmp_tabname4
				+ " A join " + merge_tab_pre + merge_townleave
				+ " B on A.iuin=B.id group by A.iuin;";
		replacesql = "replace into " + tmp_tabname5
				+ "(iuin,result_value) values (?,?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							insertstmt.setInt(1, v1);
							insertstmt.setInt(2, v2);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();
						insertstmt.close();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();
		// query user level distribution
		sqlstr = "select userlevel,count(iuin) as levelcnt from ( select iuin, par_userlevel from "
				+ tmp_tabname5 + " group by iuin ) AA group by userlevel ; ";
		sqlstr = sqlstr.replaceAll("par_userlevel", sql_userlevel_common
				.replaceAll("result_value", "max(result_value)"));
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			String v1 = rs.getString(1);
			int v2 = rs.getInt(2);
			resultosw.write(v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();
		// String merge_money = "tab_m3g_money";
		// drop_merge_table(merge_money);
		// create_merge_table(merge_money, action_lastdate, action_date_cnt);
		//
		// sqlstr = "select distinct A.iuin from " + tmp_tabname4 + " A join "
		// + merge_tab_pre + merge_money + " B on A.iuin = B.iUin  ;";
		//
		// String tmp_tabname5 = tmp_tab_pre + "userid_final" + UNDERLINE +
		// step;
		// replacesql = "replace into " + tmp_tabname5 + "(iuin) values (?) ;";
		// drop_login_tmp_table(tmp_tabname5);
		// create_login_tmp_table(tmp_tabname5);
		//
		// exec = Executors.newFixedThreadPool(conns.length);
		// taskMap = new HashMap<String, Future>();
		// for (int l = 0; l < conns.length; l++) {
		//
		// final String connStr = ips[l];
		// final Connection conn = conns[l];
		//
		// Callable call = new Callable() {
		// public String call() throws Exception {
		// String ret = OK;
		// logstr = "conn=" + connStr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// try {
		// Statement stmt = conn.createStatement();
		// ResultSet rs = stmt.executeQuery(sqlstr);
		// PreparedStatement insertstmt = loginConn
		// .prepareStatement(replacesql);
		//
		// while (rs.next()) {
		// int v = rs.getInt(1);
		// insertstmt.setInt(1, v);
		// insertstmt.addBatch();
		// }
		// insertstmt.executeBatch();
		// insertstmt.close();
		//
		// } catch (SQLException e) {
		// printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
		// ret = FAILED;
		// }
		//
		// return ret;
		// }
		//
		// };
		// Future task = exec.submit(call);
		// taskMap.put(connStr, task);
		// }
		//
		// iter = taskMap.entrySet().iterator();
		// while (iter.hasNext()) {
		// Map.Entry entry = (Map.Entry) iter.next();
		// String key = (String) entry.getKey();
		// Future val = (Future) entry.getValue();
		// String ret = (String) val.get();
		// printLogStr(key + TAB + ret);
		// }
		//
		// exec.shutdown();
		//
		// sqlstr = "select count(iuin) from " + tmp_tabname5;
		// logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		// stmt = loginConn.createStatement();
		// rs = stmt.executeQuery(sqlstr);
		//
		// while (rs.next()) {
		// int v = rs.getInt(1);
		// resultosw.write(step + TAB + "all" + TAB + action_date + TAB
		// + date_cnt + TAB + v + NEWLINE);
		// }
		// resultosw.flush();
		//
		// sqlstr = "select count(distinct iuin) from " + tmp_tabname5;
		// logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		// stmt = loginConn.createStatement();
		// rs = stmt.executeQuery(sqlstr);
		//
		// while (rs.next()) {
		// int v = rs.getInt(1);
		// resultosw.write(step + TAB + "distinct" + TAB + action_date + TAB
		// + date_cnt + TAB + v + NEWLINE);
		// }
		// resultosw.flush();

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_loginpersoncount() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		String login_lastdate = other_args.split(COMMA)[0];
		int login_date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		String merge_login = "tab_login";
		drop_login_merge_table(merge_login);
		create_login_merge_table(merge_login, login_lastdate, login_date_cnt);

		sqlstr = "select  count(distinct iUin) from " + merge_tab_pre
				+ merge_login;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultstr = login_lastdate + TAB + login_date_cnt + TAB + v1
					+ NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_paycount() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String last_date = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		String merge_money = "tab_m3g_money";
		drop_login_merge_table(merge_money);
		create_login_merge_table(merge_money, last_date, date_cnt);

		String tmp_userid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_login_tmp_table(tmp_userid);
		create_login_tmp_table(tmp_userid);

		ExecutorService exec = null;
		HashMap taskMap = null;
		Iterator iter = null;

		sqlstr = "select distinct iuin from " + merge_tab_pre + merge_money;

		replacesql = "replace into " + tmp_userid + "(iuin) values (?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							insertstmt.setInt(1, v1);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();
						insertstmt.close();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		sqlstr = "select  count(iuin) from " + tmp_userid;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultstr = last_date + TAB + date_cnt + TAB + v1 + NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		sqlstr = "select  count(distinct iuin) from " + tmp_userid;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultstr = last_date + TAB + date_cnt + TAB + v1 + NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_paycount_byuid() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String last_date = other_args.split(COMMA)[0];
		int date_cnt = Integer.parseInt(other_args.split(COMMA)[1]);

		String merge_money = "tab_m3g_money";
		drop_login_merge_table(merge_money);
		create_login_merge_table(merge_money, last_date, date_cnt);

		String tmp_userid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_login_tmp_table(tmp_userid);
		create_login_tmp_table(tmp_userid);

		ExecutorService exec = null;
		HashMap taskMap = null;
		Iterator iter = null;

		String tab_return_1 = tmp_tab_pre
				+ "userid_return_get_returnuserid_action";
		sqlstr = "select distinct A.iuin from " + tab_return_1 + " A join "
				+ merge_tab_pre + merge_money + " B on A.iuin = B.iuin ";

		replacesql = "replace into " + tmp_userid + "(iuin) values (?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							insertstmt.setInt(1, v1);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();
						insertstmt.close();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		sqlstr = "select  count(iuin) from " + tmp_userid;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultstr = last_date + TAB + date_cnt + TAB + v1 + NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		sqlstr = "select  count(distinct iuin) from " + tmp_userid;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultstr = last_date + TAB + date_cnt + TAB + v1 + NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_saveuser() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		if (other_args == null) {
			return;
		}
		String lastdate = other_args.split(COMMA)[0];
		int date_cnt = new Integer(other_args.split(COMMA)[1]);
		String tmp_tabname = "tab_login";

		int saveuser_count = 0;

		// query after user
		String tab_afteruid = tmp_tab_pre + "userid_after" + UNDERLINE + step;
		drop_login_tmp_table(tab_afteruid);
		create_login_tmp_table(tab_afteruid);

		drop_login_merge_table(tmp_tabname);
		create_login_merge_table(tmp_tabname, lastdate, date_cnt);

		sqlstr = "replace into " + tab_afteruid
				+ " select distinct iUin from test.merge_tab_login ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// cal save user count
		String tab_return_1 = tmp_tab_pre
				+ "userid_return_get_returnuserid_action";

		sqlstr = "select count(distinct A1.iUin) from " + tab_afteruid
				+ " A1 join " + tab_return_1 + " A2 on A1.iUin=A2.iUin ;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			saveuser_count = rs.getInt(1);
		}

		resultosw.write(date_cnt + TAB + saveuser_count + NEWLINE);
		resultosw.flush();

		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_regnotloginuser_count() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();

		String reg_startdate = other_args.split(COMMA)[0];
		String reg_stopdate = other_args.split(COMMA)[1];
		String login_startdate = other_args.split(COMMA)[2];
		String login_stopdate = other_args.split(COMMA)[3];

		int regdate_count = cal_DiffDay(reg_stopdate, reg_startdate);
		int logindate_count = cal_DiffDay(login_stopdate, login_startdate);

		// reg
		String tmp_tab_reg = tmp_tab_pre + "userid_reg" + UNDERLINE + step;
		drop_login_tmp_table(tmp_tab_reg);
		create_login_tmp_table(tmp_tab_reg);

		String merge_tabname1 = "tab_accountlog_addr";
		drop_login_merge_table(merge_tabname1);
		create_login_merge_table(merge_tabname1, reg_stopdate, regdate_count);

		sqlstr = "replace into " + tmp_tab_reg + " select distinct iUin from "
				+ merge_tab_pre + merge_tabname1;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// login
		String tmp_tab_login = tmp_tab_pre + "userid_login" + UNDERLINE + step;
		drop_login_tmp_table(tmp_tab_login);
		create_login_tmp_table(tmp_tab_login);

		String merge_tabname2 = "tab_login";
		drop_login_merge_table(merge_tabname2);
		create_login_merge_table(merge_tabname2, login_stopdate,
				logindate_count);

		sqlstr = "replace into " + tmp_tab_login
				+ " select distinct iUin from " + merge_tab_pre
				+ merge_tabname2;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query after user
		String tab_afteruid = tmp_tab_pre + "userid_after";
		drop_login_tmp_table(tab_afteruid);
		create_login_tmp_table(tab_afteruid);

		// query user level distribution
		sqlstr = " select count(A.iuin) from " + tmp_tab_pre + tmp_tab_reg
				+ " A left join " + tmp_tab_pre + tmp_tab_login
				+ " B on A.iuin=B.iuin where B.iuin is null ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			String v1 = rs.getString(1);
			resultosw.write(reg_startdate + TAB + reg_stopdate + TAB
					+ login_startdate + TAB + login_stopdate + TAB + v1
					+ NEWLINE);
		}
		resultosw.flush();

		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	/**
	 * 计算流失用户流失前的等级
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_lostuser_leveldistribution_2() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		String nologin_startdate = other_args.split(COMMA)[0];
		String nologin_stopdate = other_args.split(COMMA)[1];
		String lastdate = addDay(nologin_startdate, -1);
		int before_date_cnt = Integer.parseInt(lastdate.substring(8, 10));
		// int before_datecount = 3;
		// query before user
		String tmp_tabname1 = tmp_tab_pre + "userid_before";
		drop_login_tmp_table(tmp_tabname1);
		create_login_tmp_table(tmp_tabname1);

		String merge_tabnam1 = "tab_login";
		drop_login_merge_table(merge_tabnam1);
		create_login_merge_table(merge_tabnam1, lastdate, before_date_cnt);

		sqlstr = "replace into " + tmp_tabname1 + " select distinct iUin from "
				+ merge_tab_pre + merge_tabnam1;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query after user
		String tmp_tabname2 = tmp_tab_pre + "userid_after";
		drop_login_tmp_table(tmp_tabname2);
		create_login_tmp_table(tmp_tabname2);

		String merge_tabname1 = "tab_login";
		int after_date_cnt = cal_DiffDay(nologin_stopdate, nologin_startdate);
		// int after_datecount = 4;
		drop_login_merge_table(merge_tabname1);
		create_login_merge_table(merge_tabname1, addDay(nologin_stopdate, -1),
				after_date_cnt);

		sqlstr = "replace into " + tmp_tabname2 + " select distinct iUin from "
				+ merge_tab_pre + merge_tabname1;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query lost user
		String tmp_tabname3 = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tmp_tabname3);
		create_tmp_table(tmp_tabname3);
		sqlstr = "select distinct A.iuin from" + tmp_tab_pre + tmp_tabname1
				+ " A left join" + tmp_tab_pre + tmp_tabname2
				+ " B on A.iuin = B.iuin where B.iuin is null ;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String loadfile = workDir + step + load_suffix;
		FileOutputStream fos = new FileOutputStream(loadfile, true);
		OutputStreamWriter loadosw = new OutputStreamWriter(fos);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			loadosw.write(v1 + NEWLINE);
		}
		loadosw.flush();
		loadosw.close();

		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tmp_tabname3
				+ " fields terminated by '\t' lines terminated by '\n' (iuin) ;";

		ExecutorService exec = null;
		HashMap taskMap = null;

		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						stmt.execute(sqlstr);
					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// query user score from all database to login database

		String merge_tabname3 = "tab_town_login";
		drop_merge_table(merge_tabname3);
		create_merge_table(merge_tabname3, lastdate, before_date_cnt);

		String tmp_tabname4 = tmp_tab_pre + "useronedata" + UNDERLINE + step;
		drop_login_tmp_table(tmp_tabname4);
		create_login_tmp_table(tmp_tabname4);
		sqlstr = "select A.iuin , max(B.score) from" + tmp_tabname3
				+ " A join " + merge_tab_pre + merge_tabname3
				+ " B on A.iuin=B.id group by A.iuin;";
		replacesql = "replace into " + tmp_tabname4
				+ "(iuin,result_value) values (?,?) ;";
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
						PreparedStatement insertstmt = loginConn
								.prepareStatement(replacesql);

						while (rs.next()) {
							int v1 = rs.getInt(1);
							int v2 = rs.getInt(2);
							insertstmt.setInt(1, v1);
							insertstmt.setInt(2, v2);
							insertstmt.addBatch();
						}
						insertstmt.executeBatch();
						insertstmt.close();

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}

					return ret;
				}

			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// register
		String merge_tabname4 = "tab_accountlog_addr";
		drop_login_merge_table(merge_tabname4);
		create_login_merge_table(merge_tabname4, lastdate, before_date_cnt);

		String type_newuser_media = "1";
		String type_newuser_nomedia = "2";
		String type_olduser = "3";
		// query user level distribution
		sqlstr = "select userlevel,count(iuin) as levelcnt from ( select A.iuin, par_userlevel from "
				+ tmp_tabname4
				+ " A join "
				+ merge_tab_pre
				+ merge_tabname4
				+ " B on A.iuin=B.iuin and B.media_id != 0 group by A.iuin ) AA group by userlevel ; ";
		sqlstr = sqlstr.replaceAll("par_userlevel", sql_userlevel_common
				.replaceAll("result_value", "max(result_value)"));
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			String v1 = rs.getString(1);
			int v2 = rs.getInt(2);
			resultosw.write(nologin_startdate + TAB + type_newuser_media + TAB
					+ v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();

		sqlstr = "select userlevel,count(iuin) as levelcnt from ( select A.iuin, par_userlevel from "
				+ tmp_tabname4
				+ " A join "
				+ merge_tab_pre
				+ merge_tabname4
				+ " B on A.iuin=B.iuin and B.media_id = 0 group by A.iuin ) AA group by userlevel ; ";
		sqlstr = sqlstr.replaceAll("par_userlevel", sql_userlevel_common
				.replaceAll("result_value", "max(result_value)"));
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			String v1 = rs.getString(1);
			int v2 = rs.getInt(2);
			resultosw.write(nologin_startdate + TAB + type_newuser_nomedia
					+ TAB + v1 + TAB + v2 + NEWLINE);
		}
		resultosw.flush();

		sqlstr = "select userlevel,count(iuin) as levelcnt from ( select A.iuin, par_userlevel from "
				+ tmp_tabname4
				+ " A left join "
				+ merge_tab_pre
				+ merge_tabname4
				+ " B on A.iuin=B.iuin where B.iuin is null group by A.iuin ) AA group by userlevel ; ";
		sqlstr = sqlstr.replaceAll("par_userlevel", sql_userlevel_common
				.replaceAll("result_value", "max(result_value)"));
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		while (rs.next()) {
			String v1 = rs.getString(1);
			int v2 = rs.getInt(2);
			resultosw.write(nologin_startdate + TAB + type_olduser + TAB + v1
					+ TAB + v2 + NEWLINE);
		}
		resultosw.flush();

		drop_merge_table(merge_tabname3);

		close_Conns();
		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_soulcard_insertcount() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();

		start_date = action_date;
		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);

		sqlstr = sql_card_insertcount.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
		ExecutorService exec = null;
		HashMap taskMap = null;
		Iterator iter = null;
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];
			final int zoneId = zoneIds[l];
			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);

						while (rs.next()) {
							String v1 = rs.getString(1);
							int v2 = rs.getInt(2);
							resultosw.write(action_date + TAB + zoneId + TAB
									+ v1 + TAB + v2 + NEWLINE);
						}

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_soulcard_takeoffcount() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();

		start_date = action_date;
		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);

		sqlstr = sql_card_takeoffcount.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);

		ExecutorService exec = null;
		HashMap taskMap = null;
		Iterator iter = null;
		exec = Executors.newFixedThreadPool(conns.length);
		taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];
			final int zoneId = zoneIds[l];
			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);
					try {
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);

						while (rs.next()) {
							String v1 = rs.getString(1);
							int v2 = rs.getInt(2);
							resultosw.write(action_date + TAB + zoneId + TAB
									+ v1 + TAB + v2 + NEWLINE);
						}

					} catch (SQLException e) {
						printLogStr(connStr + COMMA + e.getMessage() + NEWLINE);
						ret = FAILED;
					}
					return ret;
				}
			};
			Future task = exec.submit(call);
			taskMap.put(connStr, task);
		}

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_soulcard() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		String par_goodstype = other_args.split(SEMI)[0];

		ips = all_ips;
		open_Conns();

		start_date = action_date;
		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);

		sqlstr = sql_soulcard.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day)
				.replaceAll("par_goodstype", par_goodstype);
		for (int l = 0; l < conns.length; l++) {
			logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
			printLogStr(logstr);
			conn = conns[l];
			int zoneId = zoneIds[l];
			try {
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				while (rs.next()) {
					String v1 = rs.getString(1);
					int v2 = rs.getInt(2);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

}
