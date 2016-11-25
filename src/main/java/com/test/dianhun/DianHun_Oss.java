package com.test.dianhun;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
public class DianHun_Oss extends Tools implements DianHunSql {

	public static void get_xxx() throws Exception {
		// 打开文件（日志文件，临时文件，结果文件）
		// 变量初始化赋值
		// 打开数据库连接（allip，localip，loginip）
		// 创建临时表
		// 执行sql语句，这里可能有临时结果集，需要有多步
		// 关闭文件
		// 关闭连接
	}

	public static void main(String[] args) throws Exception {
		runOut(args);
	}

	public static void get_guanka_prizecount() throws Exception {
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

		sqlstr = sql_guanka_prizecount.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					int v3 = rs.getInt(3);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void cal_register_dates() throws Exception {
		register_day_cnt = Integer.parseInt(other_args.split(COMMA)[0]);
		register_dates = new String[register_day_cnt];
		for (int n = 0; n < register_day_cnt; n++) {
			String tmp_date = addDay(action_date, 0 - n);
			register_dates[n] = tmp_date;
		}
	}

	public static void get_allpricelevel() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		action_table = "tab_item";

		ips = all_ips;
		open_Conns();

		logstr = step;
		printLogStr(logstr);

		start_date = action_date;
		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);

		stop_date = addDay(start_date, 1);
		start_time = ZERO_HOURMINSEC;
		stop_time = ZERO_HOURMINSEC;

		start_datetime = start_date + SPACE + start_time;
		stop_datetime = stop_date + SPACE + stop_time;

		sqlstr = sql_all_price_level.replaceAll("par_year", start_year)
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
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		resultosw.flush();

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_newpayer() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_LoginConn();
		action_table = "tab_first_pay";

		logstr = action_date;
		printLogStr(logstr);

		start_time = ZERO_HOURMINSEC;
		stop_time = ZERO_HOURMINSEC;

		start_date = action_date;
		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);
		start_datetime = start_date + SPACE + start_time;
		stop_date = addDay(action_date, 1);
		stop_datetime = stop_date + SPACE + stop_time;

		resultosw.write(action_date + TAB);
		sqlstr = " select count(distinct iUin) as newpayer_cnt from ( \r\n";
		for (int i0 = 0; i0 < zoneIds.length; i0++) {
			int zoneId = zoneIds[i0];
			String table = action_table + UNDERLINE + zoneId;
			String s = sql_newpayer.replaceAll("par_year", start_year)
					.replaceAll("par_month", start_month)
					.replaceAll("par_day", start_day)
					.replaceAll("par_starttime", start_datetime)
					.replaceAll("par_stoptime", stop_datetime)
					.replaceAll("par_table", table);
			if (i0 == zoneIds.length - 1) {
				sqlstr += s + "\r\n) CC ; ";
			} else {
				sqlstr += s + " union \r\n";
			}
		}

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		if (rs.next()) {
			int v = rs.getInt(1);
			resultosw.write(String.valueOf(v) + NEWLINE);
		}
		resultosw.flush();

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_validuser() throws Exception {

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
		open_ResultConn();

		String ad_id = other_args.split(COMMA)[0];
		String stats_name = other_args.split(COMMA)[1];
		String reg_startdate = action_date;
		String reg_stopdate = action_date;
		String stats_startdate = action_date;
		String stats_stopdate = action_date;

		// get ad_id
		sqlstr = "select ad_id from tab_m3gcn_ad.m3gcn_ad_id where media_id in (157,182);";
		logstr = "conn=" + resulthostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = resultConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String par_ad_id_str = EMPTY;
		while (rs.next()) {
			par_ad_id_str += rs.getInt(1) + COMMA;
		}
		par_ad_id_str += "0";

		// query user id
		int reg_date_cnt = cal_DiffDay(reg_stopdate, reg_startdate) + 1;
		String merge_tab_account = "tab_accountlog_addr";
		drop_login_merge_table(merge_tab_account);
		create_login_merge_table(merge_tab_account, reg_stopdate, reg_date_cnt);

		if ("-1".equalsIgnoreCase(ad_id)) {
			sqlstr = "select distinct A.iuin from " + merge_tab_pre
					+ merge_tab_account
					+ " A where A.media_id not in (par_ad_id) ;";
		}
		if ("0".equalsIgnoreCase(ad_id)) {
			sqlstr = "select distinct A.iuin from " + merge_tab_pre
					+ merge_tab_account
					+ " A where A.media_id in (par_ad_id) ;";
		}
		sqlstr = sqlstr.replaceAll("par_ad_id", par_ad_id_str);
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

		String tmp_userid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tmp_userid);
		create_tmp_table(tmp_userid);
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

		String tmp_tabname1 = tmp_tab_pre + "useronedata" + UNDERLINE + step;
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
		String merge_tab_townleave = "tab_town_leave";
		drop_merge_table(merge_tab_townleave);
		create_merge_table(merge_tab_townleave, stats_stopdate, date_cnt);

		sqlstr = "select B1.iuin , case when B2.PlayerID is null then 0 else max(B2.OnlineTime) end as login_time , case when B2.PlayerID is null then 0 else max(B2.Score) end as score from "
				+ tmp_userid
				+ " B1 left join "
				+ merge_tab_pre
				+ merge_tab_townleave
				+ " B2 on B1.iuin=B2.PlayerID group by B1.iuin ;";

		String tmp_tabname2 = tmp_tab_pre + "usertwodata" + UNDERLINE + step;
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

		String[] login_counts = new String[] { "( T.login_count =0 )",
				"( T.login_count =1 )", "( T.login_count =2 )",
				"( T.login_count >=3 )", };
		String[] login_times = new String[] {
				"( T.login_time>=0 and T.login_time<300 )",
				"( T.login_time>=300 and T.login_time<3600 )",
				"( T.login_time>=3600 and T.login_time<86400 )",
				"( T.login_time>=86400 )", };
		String[] scores = new String[] { "( T.score=0 )",
				"( T.score>0 and T.score<=100 )",
				"( T.score>100 and T.score<=1500 )", "( T.score>1500 )" };

		String basesql = "select count(*) from ( select A.iuin , A.result_value as login_count , sum(B.value1) as login_time, max(B.value2) as score from "
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
		close_ResultConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_lostnewuser() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		cal_register_dates();
		open_LoginConn();

		for (int m3 = 1; m3 < register_dates.length; m3++) {

			register_date = register_dates[m3];

			logstr = step + COMMA + action_date + COMMA + register_date;
			printLogStr(logstr);

			init_datetime();

			String basesql = "(select distinct A.iuin from `tab_accountlog_addr_par_register_year-par_register_month`.`par_register_day` A , `tab_char_create_par_zoneId_par_register_year-par_register_month`.`par_register_day` B where A.iuin = B.iUin )";
			sqlstr = "select count(distinct iuin) from ( ";
			for (int i = 0; i < zoneIds.length; i++) {
				String zoneIdStr = String.valueOf(zoneIds[i]);
				if (i == zoneIds.length - 1) {
					sqlstr = sqlstr
							+ basesql
									.replaceAll("par_register_year",
											register_year)
									.replaceAll("par_register_month",
											register_month)
									.replaceAll("par_register_day",
											register_day)
									.replaceAll("par_zoneId", zoneIdStr)
							+ " ) T ;";
				} else {
					sqlstr = sqlstr
							+ basesql
									.replaceAll("par_register_year",
											register_year)
									.replaceAll("par_register_month",
											register_month)
									.replaceAll("par_register_day",
											register_day)
									.replaceAll("par_zoneId", zoneIdStr)
							+ " union ";
				}
			}

			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			int regcharcrtCnt = 0;
			while (rs.next()) {
				regcharcrtCnt = rs.getInt(1);
			}

			sqlstr = "select count(distinct AA.iUin) from `tab_login_par_year-par_month`.`par_day` AA , ( ";
			for (int i = 0; i < zoneIds.length; i++) {
				String zoneIdStr = String.valueOf(zoneIds[i]);
				if (i == zoneIds.length - 1) {
					sqlstr = sqlstr
							+ basesql
									.replaceAll("par_register_year",
											register_year)
									.replaceAll("par_register_month",
											register_month)
									.replaceAll("par_register_day",
											register_day)
									.replaceAll("par_zoneId", zoneIdStr)
							+ " ) T  where AA.iUin = T.iuin ;";
				} else {
					sqlstr = sqlstr
							+ basesql
									.replaceAll("par_register_year",
											register_year)
									.replaceAll("par_register_month",
											register_month)
									.replaceAll("par_register_day",
											register_day)
									.replaceAll("par_zoneId", zoneIdStr)
							+ " union ";
				}
			}
			sqlstr = sqlstr.replaceAll("par_year", start_year)
					.replaceAll("par_month", start_month)
					.replaceAll("par_day", start_day);

			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = loginConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			int saveCnt = 0;
			while (rs.next()) {
				saveCnt = rs.getInt(1);
			}

			int diffDay = cal_DiffDay(action_date, register_date);
			resultosw.write(action_date + TAB + register_date + TAB + diffDay
					+ TAB + regcharcrtCnt + TAB + saveCnt + NEWLINE);

		}
		resultosw.flush();

		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_guanka_avgtime() throws Exception {
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

		sqlstr = sql_guanka_avgtime.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					int v3 = rs.getInt(3);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	/*
	 * case when max(iFightScoreFun)>-1 and max(iFightScoreFun)<=3000 then 0
	 * when max(iFightScoreFun)>3000 and max(iFightScoreFun)<=5000 then 1 when
	 * max(iFightScoreFun)>5000 and max(iFightScoreFun)<=6000 then 2 when
	 * max(iFightScoreFun)>6000 and max(iFightScoreFun)<=7000 then 3 when
	 * max(iFightScoreFun)>7000 and max(iFightScoreFun)<=8000 then 4 when
	 * max(iFightScoreFun)>8000 and max(iFightScoreFun)<=10000 then 5 when
	 * max(iFightScoreFun)>10000 and max(iFightScoreFun)<=12500 then 6 when
	 * max(iFightScoreFun)>12500 and max(iFightScoreFun)<=16000 then 7 when
	 * max(iFightScoreFun)>16000 and max(iFightScoreFun)<=20000 then 8 else 9
	 * end
	 */
	public static void get_fight_score() throws Exception {
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

		sqlstr = sql_fight_score.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_avguserlevel() throws Exception {
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

		sqlstr = sql_avguserlevel.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					float v2 = rs.getFloat(2);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_corptask() throws Exception {
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

		String sql = "select sum(case when eTaskType=3 then 1 else 0 end) as normal_count, sum(case when eTaskType=7 then 1 else 0 end) as build_count,sum(case when eTaskType=12 then 1 else 0 end) as xuanshang_count,sum(case when eTaskType=12 and dwTaskID=1093677105 then 1 else 0 end) as xuanshang_race_count from `tab_corp_task_par_year-par_month`.`par_day` where eTaskType in (3,7,12) ;";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					int v3 = rs.getInt(3);
					int v4 = rs.getInt(4);
					int v5 = v3 - v4;
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v4 + TAB + v5 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_guozhan_winlose() throws Exception {
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

		String sql = "select iMapID , count( distinct (case when iIsWin=1 then RaceID end) ) as win, count( distinct (case when iIsWin=0 then RaceID end) ) as lose from `tab_map_race_par_year-par_month`.`par_day` where iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84') group by iMapID;";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					int v3 = rs.getInt(3);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_guozhan_winlose_bymapnation() throws Exception {
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

		String sql = "select t11.iMapID , t12.nation , count( distinct (case when t11.iIsWin=1 then t11.RaceID end) ) as win, count( distinct (case when t11.iIsWin=0 then t11.RaceID end) ) as lose "
				+ " from `tab_map_race_par_year-par_month`.`par_day` t11  join "
				+ " ( select t2.playerid , max(t2.nation) nation from `tab_map_race_par_year-par_month`.`par_day` t1 join `tab_town_leave_par_year-par_month`.`par_day` t2 "
				+ "on (t1.iuin=t2.playerid and t1.iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84') ) group by t2.playerid ) t12 on (t11.iuin=t12.playerid and t11.iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84') ) group by t11.iMapID ,t12.nation ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					int v3 = rs.getInt(3);
					int v4 = rs.getInt(4);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + TAB + v4 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_corpbaseinfo() throws Exception {
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

		String sql = "select ( select count( distinct CorpID ) from `tab_corp_misc_par_year-par_month`.`par_day` where Type=1 ) , (select count( distinct CorpID ) from `tab_corp_misc_par_year-par_month`.`par_day` where Type=2 ) ,(select count( distinct PlayerID ) from `tab_corp_misc_par_year-par_month`.`par_day` where Type=7 ) ,(select count( distinct PlayerID ) from `tab_corp_misc_par_year-par_month`.`par_day` where Type=8 ) ,(select count( distinct PlayerID ) from `tab_corp_misc_par_year-par_month`.`par_day` where Type=9 ) ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					int v3 = rs.getInt(3);
					int v4 = rs.getInt(4);
					int v5 = rs.getInt(5);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + TAB + v4 + TAB + v5 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_guozhan_avgcosttime() throws Exception {
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

		String sql = "select avg(iTime) from `tab_map_race_par_year-par_month`.`par_day` where iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84') ;";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					resultosw.write(action_date + TAB + zoneId + TAB + v1
							+ NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_guozhan_participantcount() throws Exception {
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

		String sql = "select T2.eNation,count(distinct T1.iUin) from `tab_map_race_par_year-par_month`.`par_day` T1 join `tab_town_login_par_year-par_month`.`par_day` T2 on T1.iUin=T2.id and T1.iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84') group by T2.eNation ;";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_guozhan_participanttype_bak() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		start_date = action_date;
		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);

		ExecutorService exec = null;
		HashMap taskMap = null;
		Iterator iter = null;

		// 当天参与国战的用户
		String tmp_userid_1 = tmp_tab_pre + "userid_1" + UNDERLINE + step;
		replacesql = "replace into " + tmp_userid_1 + "(iuin) values (?) ;";

		drop_login_tmp_table(tmp_userid_1);
		create_login_tmp_table(tmp_userid_1);

		String sql = "select distinct iUin from `tab_map_race_par_year-par_month`.`par_day` where iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84') ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);

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
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
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

		// 当天参与竞技的用户

		String tmp_userid_2 = tmp_tab_pre + "userid_2" + UNDERLINE + step;
		replacesql = "replace into " + tmp_userid_2 + "(iuin) values (?) ;";

		drop_login_tmp_table(tmp_userid_2);
		create_login_tmp_table(tmp_userid_2);

		sql = "select distinct iUin from `tab_map_race_par_year-par_month`.`par_day` where iMapID not in ('DT11','DT71','DT72','DT73','DT83','DT84') ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);

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
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
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

		// 当天参与关卡的用户

		String tmp_userid_3 = tmp_tab_pre + "userid_3" + UNDERLINE + step;
		replacesql = "replace into " + tmp_userid_3 + "(iuin) values (?) ;";

		drop_login_tmp_table(tmp_userid_3);
		create_login_tmp_table(tmp_userid_3);

		sql = "select distinct iUin from `tab_map_guanka_par_year-par_month`.`par_day` ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);

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
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
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

		// 当天成就助手的用户

		String tmp_userid_4 = tmp_tab_pre + "userid_4" + UNDERLINE + step;
		replacesql = "replace into " + tmp_userid_4 + "(iuin) values (?) ;";

		drop_login_tmp_table(tmp_userid_4);
		create_login_tmp_table(tmp_userid_4);

		sql = "select distinct char_id from `tab_misc_par_year-par_month`.`par_day` where type=5 and sub_type=1200 ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);

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
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery(sqlstr);
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

		// 国籍

		String merge_tab_townleave = "tab_town_leave";
		drop_merge_table(merge_tab_townleave);
		create_merge_table(merge_tab_townleave, action_date, 1);

		String tmp_userone = tmp_tab_pre + "useronedata" + UNDERLINE + step;
		sqlstr = "select PlayerID ,max(Nation) from " + merge_tab_pre
				+ merge_tab_townleave + " group by PlayerID " + SEMI;
		drop_login_tmp_table(tmp_userone);
		create_login_tmp_table(tmp_userone);

		replacesql = "replace into " + tmp_userone
				+ "(iuin,result_value) values (?,?)" + SEMI;

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

		// 计算各种类型的人数
		sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
				+ tmp_userid_1 + " T1 join " + tmp_userone
				+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		int[] all_cnt = new int[4];
		while (rs.next()) {
			if (rs.getInt(1) == 0)
				all_cnt[0] = rs.getInt(2);
			if (rs.getInt(1) == 1)
				all_cnt[1] = rs.getInt(2);
			if (rs.getInt(1) == 2)
				all_cnt[2] = rs.getInt(2);
			if (rs.getInt(1) == 3)
				all_cnt[3] = rs.getInt(2);
		}

		String tmp_userid_5 = EMPTY;
		String tmp_userid_6 = EMPTY;
		String tmp_userid_final = EMPTY;
		//
		tmp_userid_5 = tmp_tab_pre + "userid_5" + UNDERLINE + step;
		sqlstr = "replace into " + tmp_userid_5
				+ "(iuin) select distinct T1.iuin from " + tmp_userid_1
				+ " T1 join " + tmp_userid_2 + " T2 on T1.iuin=T2.iuin" + SEMI;
		drop_login_tmp_table(tmp_userid_5);
		create_login_tmp_table(tmp_userid_5);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		tmp_userid_final = tmp_tab_pre + "userid_final" + UNDERLINE + step;
		sqlstr = "replace into " + tmp_userid_final
				+ "(iuin) select distinct T1.iuin from " + tmp_userid_5
				+ " T1 left join " + tmp_userid_3
				+ " T2 on T1.iuin = T2.iuin where T2.iuin is null " + SEMI;
		drop_login_tmp_table(tmp_userid_final);
		create_login_tmp_table(tmp_userid_final);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
				+ tmp_userid_final + " T1 join " + tmp_userone
				+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		int[] just_race = new int[4];
		while (rs.next()) {
			if (rs.getInt(1) == 0)
				just_race[0] = rs.getInt(2);
			if (rs.getInt(1) == 1)
				just_race[1] = rs.getInt(2);
			if (rs.getInt(1) == 2)
				just_race[2] = rs.getInt(2);
			if (rs.getInt(1) == 3)
				just_race[3] = rs.getInt(2);
		}
		//
		tmp_userid_5 = tmp_tab_pre + "userid_5" + UNDERLINE + step;
		sqlstr = "replace into " + tmp_userid_5
				+ "(iuin) select distinct T1.iuin from " + tmp_userid_1
				+ " T1 join " + tmp_userid_3 + " T2 on T1.iuin=T2.iuin " + SEMI;
		drop_login_tmp_table(tmp_userid_5);
		create_login_tmp_table(tmp_userid_5);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		tmp_userid_final = tmp_tab_pre + "userid_final" + UNDERLINE + step;
		sqlstr = "replace into " + tmp_userid_final
				+ "(iuin) select distinct T1.iuin from " + tmp_userid_5
				+ " T1 left join " + tmp_userid_2
				+ " T2 on T1.iuin = T2.iuin where T2.iuin is null " + SEMI;
		drop_login_tmp_table(tmp_userid_final);
		create_login_tmp_table(tmp_userid_final);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
				+ tmp_userid_final + " T1 join " + tmp_userone
				+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		int[] just_guanka = new int[4];
		while (rs.next()) {
			if (rs.getInt(1) == 0)
				just_guanka[0] = rs.getInt(2);
			if (rs.getInt(1) == 1)
				just_guanka[1] = rs.getInt(2);
			if (rs.getInt(1) == 2)
				just_guanka[2] = rs.getInt(2);
			if (rs.getInt(1) == 3)
				just_guanka[3] = rs.getInt(2);
		}
		// 竞技和关卡
		tmp_userid_5 = tmp_tab_pre + "userid_5" + UNDERLINE + step;
		sqlstr = "replace into " + tmp_userid_5
				+ "(iuin) select distinct T1.iuin from " + tmp_userid_1
				+ " T1 join " + tmp_userid_2 + " T2 on T1.iuin=T2.iuin " + SEMI;
		drop_login_tmp_table(tmp_userid_5);
		create_login_tmp_table(tmp_userid_5);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		tmp_userid_final = tmp_tab_pre + "userid_final" + UNDERLINE + step;
		sqlstr = "replace into " + tmp_userid_final
				+ "(iuin) select T1.iuin from " + tmp_userid_5 + " T1 join "
				+ tmp_userid_3 + " T2 on T1.iuin = T2.iuin " + SEMI;
		drop_login_tmp_table(tmp_userid_final);
		create_login_tmp_table(tmp_userid_final);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
				+ tmp_userid_final + " T1 join " + tmp_userone
				+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		int[] raceandguanka = new int[4];
		while (rs.next()) {
			if (rs.getInt(1) == 0)
				raceandguanka[0] = rs.getInt(2);
			if (rs.getInt(1) == 1)
				raceandguanka[1] = rs.getInt(2);
			if (rs.getInt(1) == 2)
				raceandguanka[2] = rs.getInt(2);
			if (rs.getInt(1) == 3)
				raceandguanka[3] = rs.getInt(2);
		}
		// 成就助手

		tmp_userid_5 = tmp_tab_pre + "userid_5" + UNDERLINE + step;
		sqlstr = "replace into " + tmp_userid_5
				+ "(iuin) select distinct T1.iuin from " + tmp_userid_1
				+ " T1 join " + tmp_userid_4 + " T2 on T1.iuin=T2.iuin " + SEMI;
		drop_login_tmp_table(tmp_userid_5);
		create_login_tmp_table(tmp_userid_5);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		tmp_userid_6 = tmp_tab_pre + "userid_6" + UNDERLINE + step;
		sqlstr = "replace into " + tmp_userid_6
				+ "(iuin) select distinct T1.iuin from " + tmp_userid_2
				+ " T1 union select distinct T2.iuin from " + tmp_userid_3
				+ " T2 " + SEMI;
		drop_login_tmp_table(tmp_userid_6);
		create_login_tmp_table(tmp_userid_6);
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		tmp_userid_final = tmp_tab_pre + "userid_final" + UNDERLINE + step;
		drop_login_tmp_table(tmp_userid_final);
		create_login_tmp_table(tmp_userid_final);
		sqlstr = "replace into " + tmp_userid_final
				+ "(iuin) select distinct T1.iuin from " + tmp_userid_5
				+ " T1 join " + tmp_userid_6
				+ " T2 on T1.iuin=T2.iuin where T2.iuin is null " + SEMI;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
				+ tmp_userid_final + " T1 join " + tmp_userone
				+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		int[] just_achievement = new int[4];
		while (rs.next()) {
			if (rs.getInt(1) == 0)
				just_achievement[0] = rs.getInt(2);
			if (rs.getInt(1) == 1)
				just_achievement[1] = rs.getInt(2);
			if (rs.getInt(1) == 2)
				just_achievement[2] = rs.getInt(2);
			if (rs.getInt(1) == 3)
				just_achievement[3] = rs.getInt(2);
		}
		// other

		int[] other = new int[4];
		other[0] = all_cnt[0] - just_race[0] - just_guanka[0]
				- raceandguanka[0] - just_achievement[0];
		other[1] = all_cnt[1] - just_race[1] - just_guanka[1]
				- raceandguanka[1] - just_achievement[1];
		other[2] = all_cnt[2] - just_race[2] - just_guanka[2]
				- raceandguanka[2] - just_achievement[2];
		other[3] = all_cnt[3] - just_race[3] - just_guanka[3]
				- raceandguanka[3] - just_achievement[3];

		resultosw.write(action_date + TAB + all_cnt[0] + TAB + all_cnt[1] + TAB
				+ all_cnt[2] + TAB + all_cnt[3] + TAB + just_race[0] + TAB
				+ just_race[1] + TAB + just_race[2] + TAB + just_race[3] + TAB
				+ just_guanka[0] + TAB + just_guanka[1] + TAB + just_guanka[2]
				+ TAB + just_guanka[3] + TAB + raceandguanka[0] + TAB
				+ raceandguanka[1] + TAB + raceandguanka[2] + TAB
				+ raceandguanka[3] + TAB + just_achievement[0] + TAB
				+ just_achievement[1] + TAB + just_achievement[2] + TAB
				+ just_achievement[3] + TAB + other[0] + TAB + other[1] + TAB
				+ other[2] + TAB + other[3] + NEWLINE);

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_guozhan_participanttype() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		start_date = action_date;
		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);

		ExecutorService exec = null;
		HashMap taskMap = null;
		Iterator iter = null;

		// 当天参与国战的用户
		String tmp_userid_guozhan = tmp_tab_pre + "userid_1" + UNDERLINE + step;

		drop_tmp_table(tmp_userid_guozhan);
		create_tmp_table(tmp_userid_guozhan);

		String sql = "replace into "
				+ tmp_userid_guozhan
				+ "(iuin) select distinct iUin from `tab_map_race_par_year-par_month`.`par_day` where iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84') ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);

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

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// 当天参与竞技的用户

		String tmp_userid_race = tmp_tab_pre + "userid_2" + UNDERLINE + step;

		drop_tmp_table(tmp_userid_race);
		create_tmp_table(tmp_userid_race);

		sql = "replace into "
				+ tmp_userid_race
				+ "(iuin) select distinct iUin from `tab_map_race_par_year-par_month`.`par_day` where iMapID not in ('DT11','DT71','DT72','DT73','DT83','DT84') ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);

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

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// 当天参与关卡的用户

		String tmp_userid_guanka = tmp_tab_pre + "userid_3" + UNDERLINE + step;

		drop_tmp_table(tmp_userid_guanka);
		create_tmp_table(tmp_userid_guanka);

		sql = "replace into "
				+ tmp_userid_guanka
				+ "(iuin) select distinct iUin from `tab_map_guanka_par_year-par_month`.`par_day` ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);

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

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// 当天成就助手的用户

		String tmp_userid_achieve = tmp_tab_pre + "userid_4" + UNDERLINE + step;

		drop_tmp_table(tmp_userid_achieve);
		create_tmp_table(tmp_userid_achieve);

		sql = "replace into "
				+ tmp_userid_achieve
				+ "(iuin) select distinct char_id from `tab_misc_par_year-par_month`.`par_day` where type=5 and sub_type=1200 ";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);

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

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// 国籍

		String merge_tab_townleave = "tab_town_leave";
		drop_merge_table(merge_tab_townleave);
		create_merge_table(merge_tab_townleave, action_date, 1);

		String tmp_userone_nation = tmp_tab_pre + "useronedata" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userone_nation);
		create_tmp_table(tmp_userone_nation);

		sqlstr = "replace into " + tmp_userone_nation
				+ "(iuin,result_value) select PlayerID ,max(Nation) from "
				+ merge_tab_pre + merge_tab_townleave + " group by PlayerID "
				+ SEMI;

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

		iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		exec.shutdown();

		// 计算各种类型的人数
		//
		String tmp_userid_res_race1 = tmp_tab_pre + "userid_res_race1"
				+ UNDERLINE + step;
		String tmp_userid_res_race2 = tmp_tab_pre + "userid_res_race2"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_res_race1);
		create_tmp_table(tmp_userid_res_race1);
		drop_tmp_table(tmp_userid_res_race2);
		create_tmp_table(tmp_userid_res_race2);

		String tmp_userid_res_guanka1 = tmp_tab_pre + "userid_res_guanka1"
				+ UNDERLINE + step;
		String tmp_userid_res_guanka2 = tmp_tab_pre + "userid_res_guanka2"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_res_guanka1);
		create_tmp_table(tmp_userid_res_guanka1);
		drop_tmp_table(tmp_userid_res_guanka2);
		create_tmp_table(tmp_userid_res_guanka2);

		String tmp_userid_res_raceguanka1 = tmp_tab_pre
				+ "userid_res_raceguanka1" + UNDERLINE + step;
		String tmp_userid_res_raceguanka2 = tmp_tab_pre
				+ "userid_res_raceguanka2" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_res_raceguanka1);
		create_tmp_table(tmp_userid_res_raceguanka1);
		drop_tmp_table(tmp_userid_res_raceguanka2);
		create_tmp_table(tmp_userid_res_raceguanka2);

		String tmp_userid_res_achieve1 = tmp_tab_pre + "userid_res_achieve1"
				+ UNDERLINE + step;
		String tmp_userid_res_achieve2 = tmp_tab_pre + "userid_res_achieve2"
				+ UNDERLINE + step;
		String tmp_userid_res_achieve3 = tmp_tab_pre + "userid_res_achieve3"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_res_achieve1);
		create_tmp_table(tmp_userid_res_achieve1);
		drop_tmp_table(tmp_userid_res_achieve2);
		create_tmp_table(tmp_userid_res_achieve2);
		drop_tmp_table(tmp_userid_res_achieve3);
		create_tmp_table(tmp_userid_res_achieve3);
		for (int l = 0; l < conns.length; l++) {

			String connStr = ips[l];
			Connection conn = conns[l];
			int zoneId = zoneIds[l];

			// all
			sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
					+ tmp_userid_guozhan + " T1 join " + tmp_userone_nation
					+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;

			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			int[] all_cnt = new int[4];
			while (rs.next()) {
				if (rs.getInt(1) == 0)
					all_cnt[0] = rs.getInt(2);
				if (rs.getInt(1) == 1)
					all_cnt[1] = rs.getInt(2);
				if (rs.getInt(1) == 2)
					all_cnt[2] = rs.getInt(2);
				if (rs.getInt(1) == 3)
					all_cnt[3] = rs.getInt(2);
			}

			// race
			sqlstr = "replace into " + tmp_userid_res_race1
					+ "(iuin) select distinct T1.iuin from "
					+ tmp_userid_guozhan + " T1 join " + tmp_userid_race
					+ " T2 on T1.iuin=T2.iuin" + SEMI;

			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			stmt.execute(sqlstr);

			sqlstr = "replace into " + tmp_userid_res_race2
					+ "(iuin) select distinct T1.iuin from "
					+ tmp_userid_res_race1 + " T1 left join "
					+ tmp_userid_guanka
					+ " T2 on T1.iuin = T2.iuin where T2.iuin is null " + SEMI;

			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			stmt.execute(sqlstr);

			sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
					+ tmp_userid_res_race2 + " T1 join " + tmp_userone_nation
					+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;

			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			int[] just_race = new int[4];
			while (rs.next()) {
				if (rs.getInt(1) == 0)
					just_race[0] = rs.getInt(2);
				if (rs.getInt(1) == 1)
					just_race[1] = rs.getInt(2);
				if (rs.getInt(1) == 2)
					just_race[2] = rs.getInt(2);
				if (rs.getInt(1) == 3)
					just_race[3] = rs.getInt(2);
			}
			// guanka
			sqlstr = "replace into " + tmp_userid_res_guanka1
					+ "(iuin) select distinct T1.iuin from "
					+ tmp_userid_guozhan + " T1 join " + tmp_userid_guanka
					+ " T2 on T1.iuin=T2.iuin " + SEMI;

			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			stmt.execute(sqlstr);

			sqlstr = "replace into " + tmp_userid_res_guanka2
					+ "(iuin) select distinct T1.iuin from "
					+ tmp_userid_res_guanka1 + " T1 left join "
					+ tmp_userid_race
					+ " T2 on T1.iuin = T2.iuin where T2.iuin is null " + SEMI;

			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			stmt.execute(sqlstr);

			sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
					+ tmp_userid_res_guanka2 + " T1 join " + tmp_userone_nation
					+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;
			logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			int[] just_guanka = new int[4];
			while (rs.next()) {
				if (rs.getInt(1) == 0)
					just_guanka[0] = rs.getInt(2);
				if (rs.getInt(1) == 1)
					just_guanka[1] = rs.getInt(2);
				if (rs.getInt(1) == 2)
					just_guanka[2] = rs.getInt(2);
				if (rs.getInt(1) == 3)
					just_guanka[3] = rs.getInt(2);
			}
			// 竞技和关卡
			sqlstr = "replace into " + tmp_userid_res_raceguanka1
					+ "(iuin) select distinct T1.iuin from "
					+ tmp_userid_guozhan + " T1 join " + tmp_userid_race
					+ " T2 on T1.iuin=T2.iuin " + SEMI;

			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			stmt.execute(sqlstr);

			sqlstr = "replace into " + tmp_userid_res_raceguanka2
					+ "(iuin) select T1.iuin from "
					+ tmp_userid_res_raceguanka1 + " T1 join "
					+ tmp_userid_guanka + " T2 on T1.iuin = T2.iuin " + SEMI;

			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			stmt.execute(sqlstr);

			sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
					+ tmp_userid_res_raceguanka2 + " T1 join "
					+ tmp_userone_nation
					+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;

			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			int[] raceguanka = new int[4];
			while (rs.next()) {
				if (rs.getInt(1) == 0)
					raceguanka[0] = rs.getInt(2);
				if (rs.getInt(1) == 1)
					raceguanka[1] = rs.getInt(2);
				if (rs.getInt(1) == 2)
					raceguanka[2] = rs.getInt(2);
				if (rs.getInt(1) == 3)
					raceguanka[3] = rs.getInt(2);
			}
			// achieve

			sqlstr = "replace into " + tmp_userid_res_achieve1
					+ "(iuin) select distinct T1.iuin from "
					+ tmp_userid_guozhan + " T1 join " + tmp_userid_achieve
					+ " T2 on T1.iuin=T2.iuin " + SEMI;
			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			stmt.execute(sqlstr);

			sqlstr = "replace into " + tmp_userid_res_achieve2
					+ "(iuin) select distinct T1.iuin from " + tmp_userid_race
					+ " T1 union select distinct T2.iuin from "
					+ tmp_userid_guanka + " T2 " + SEMI;
			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			stmt.execute(sqlstr);

			sqlstr = "replace into " + tmp_userid_res_achieve3
					+ "(iuin) select distinct T1.iuin from "
					+ tmp_userid_res_achieve1 + " T1 join "
					+ tmp_userid_res_achieve2
					+ " T2 on T1.iuin=T2.iuin where T2.iuin is null " + SEMI;
			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			stmt.execute(sqlstr);

			sqlstr = "select T2.result_value , count(distinct T1.iuin) from "
					+ tmp_userid_res_achieve3 + " T1 join "
					+ tmp_userone_nation
					+ " T2 on T1.iuin=T2.iuin group by T2.result_value " + SEMI;
			logstr = "conn=" + connStr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			int[] just_achieve = new int[4];
			while (rs.next()) {
				if (rs.getInt(1) == 0)
					just_achieve[0] = rs.getInt(2);
				if (rs.getInt(1) == 1)
					just_achieve[1] = rs.getInt(2);
				if (rs.getInt(1) == 2)
					just_achieve[2] = rs.getInt(2);
				if (rs.getInt(1) == 3)
					just_achieve[3] = rs.getInt(2);
			}
			// other

			int[] other = new int[4];
			other[0] = all_cnt[0] - just_race[0] - just_guanka[0]
					- raceguanka[0] - just_achieve[0];
			other[1] = all_cnt[1] - just_race[1] - just_guanka[1]
					- raceguanka[1] - just_achieve[1];
			other[2] = all_cnt[2] - just_race[2] - just_guanka[2]
					- raceguanka[2] - just_achieve[2];
			other[3] = all_cnt[3] - just_race[3] - just_guanka[3]
					- raceguanka[3] - just_achieve[3];

			resultosw.write(action_date + TAB + zoneId + TAB + +all_cnt[0]
					+ TAB + all_cnt[1] + TAB + all_cnt[2] + TAB + all_cnt[3]
					+ TAB + just_race[0] + TAB + just_race[1] + TAB
					+ just_race[2] + TAB + just_race[3] + TAB + just_guanka[0]
					+ TAB + just_guanka[1] + TAB + just_guanka[2] + TAB
					+ just_guanka[3] + TAB + raceguanka[0] + TAB
					+ raceguanka[1] + TAB + raceguanka[2] + TAB + raceguanka[3]
					+ TAB + just_achieve[0] + TAB + just_achieve[1] + TAB
					+ just_achieve[2] + TAB + just_achieve[3] + TAB + other[0]
					+ TAB + other[1] + TAB + other[2] + TAB + other[3]
					+ NEWLINE);
		}
		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_honor_info() throws Exception {
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

		// String sql =
		// "select iGoodsType,count(distinct iuin) as personcnt,count(iuin) as buycount,sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end) as itemcount,"
		// +
		// " case when iGoodsType='AJ83' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 200 when iGoodsType='AS99' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 750 when iGoodsType='AQ31' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 1500 when iGoodsType='AQ30' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 2500 when iGoodsType='AI29' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 7000 when iGoodsType='AU79' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 10000 when iGoodsType='AQ33' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 15000 when iGoodsType='AQ32' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 25000 when iGoodsType='A348' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 7000 when iGoodsType='A349' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 7000 when iGoodsType='A350' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 7000 when iGoodsType='A351' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 7000 when iGoodsType='A352' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 7000 when iGoodsType='A353' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 7000 when iGoodsType='J004' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 12000 when iGoodsType='A354' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 60000 when iGoodsType='AI58' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 4000 when iGoodsType='AI62' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 12000 when iGoodsType='AF44' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 50000 when iGoodsType='AF45' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 50000 when iGoodsType='AF46' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 50000 when iGoodsType='AG40' then sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 80000 else sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end  ) * 80000 end as itemvalue "
		// +
		// " from `tab_item_par_year-par_month`.`par_day` where  iReason=17 and iChangeType  in (0,11,12) group by iGoodsType ;";
		//

		String sql = "select iGoodsType,count(distinct iuin) as personcnt,count(iuin) as buycount,sum(case when iBeforeNum=iAfterNum then iAfterNum else (iAfterNum - iBeforeNum) end) as itemcount "
				+ " from `tab_item_par_year-par_month`.`par_day` where  iReason=17 and iChangeType  in (0,11,12) group by iGoodsType ;";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					int v3 = rs.getInt(3);
					int v4 = rs.getInt(4);
					resultstr = action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + TAB + v4 + NEWLINE;
					resultosw.write(resultstr);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_guozhan_playcount() throws Exception {
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

		String sql = "select count(distinct RaceID) from `tab_map_race_par_year-par_month`.`par_day` where iMapID in ('DT11','DT71','DT72','DT73','DT83','DT84') ;";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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

					resultstr = action_date + TAB + zoneId + TAB + v1 + NEWLINE;
					resultosw.write(resultstr);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_userlevel_distribution() throws Exception {
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
		// guanka
		sqlstr = sql_guanka_userlevel_distribution
				.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day)
				.replaceAll("par_userlevel", sql_userlevel_common_2);
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
					String v2 = rs.getString(2);
					int v3 = rs.getInt(3);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}
		// race
		sqlstr = sql_race_userlevel_distribution
				.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day)
				.replaceAll("par_userlevel", sql_userlevel_common_2);
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
					String v2 = rs.getString(2);
					int v3 = rs.getInt(3);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}
		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void get_multithd_template() throws Exception {
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
		// guanka
		sqlstr = sql_guanka_userlevel_distribution
				.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day)
				.replaceAll("par_userlevel", sql_userlevel_common_2);

		ExecutorService exec = Executors.newFixedThreadPool(conns.length);
		final Lock lock = new ReentrantLock();

		HashMap taskMap = new HashMap<String, Future>();
		for (int l = 0; l < conns.length; l++) {

			final String connStr = ips[l];
			final Connection conn = conns[l];
			// final int zoneId = zoneIds[l];

			Callable call = new Callable() {
				public String call() throws Exception {
					String ret = OK;
					logstr = "conn=" + connStr + ",sql=" + sqlstr;
					printLogStr(logstr);

					try {
						Statement statement = conn.createStatement();
						ResultSet resultset = statement.executeQuery(sqlstr);
						lock.lock();
						try {
							while (resultset.next()) {
								int v1 = resultset.getInt(1);
								resultosw.write(v1 + NEWLINE);

							}
							resultosw.flush();
						} finally {
							lock.unlock();
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

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();

	}

	public static void get_guanka_deathnum() throws Exception {
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

		String sql_guanka_deathnum = "select iMapId , iDeathNums , count(iUin) from `tab_map_guanka_par_year-par_month`.`par_day` group by iMapId , iDeathNums;";
		sqlstr = sql_guanka_deathnum.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					int v3 = rs.getInt(3);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	// 关卡神兵值
	public static void get_equipscore() throws Exception {
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

		String sql = "select iMapId , case  when EquipScore>=0 and EquipScore<=6500 then 0  when EquipScore>=6501 and EquipScore<=12000 then 1  when EquipScore>=12001 and EquipScore<=15000 then 2  when EquipScore>=15001 and EquipScore<=16000 then 3  when EquipScore>=16001 and EquipScore<=18000 then 4  when EquipScore>=18001 and EquipScore<=20000 then 5  when EquipScore>=20001 and EquipScore<=23000 then 6  when EquipScore>=23001 and EquipScore<=99999 then 7  else 8 end as EquipScoreLevel , count(iUin), count(distinct iUin) from `tab_map_guanka_par_year-par_month`.`par_day` group by iMapId , EquipScoreLevel;";
		sqlstr = sql.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					int v3 = rs.getInt(3);
					int v4 = rs.getInt(4);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + TAB + v4 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_heroselectpercent() throws Exception {
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

		sqlstr = sql_heroselectpercent.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					long t = Long.parseLong(rs.getString(2));
					String v2 = long2str(t);
					long v3 = rs.getLong(3);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_kickedaction() throws Exception {
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

		sqlstr = sql_heroselectpercent.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					long t = Long.parseLong(rs.getString(2));
					String v2 = long2str(t);
					long v3 = rs.getLong(3);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_heroendstat() throws Exception {
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

		sqlstr = sql_heroendstat.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
					long t = Long.parseLong(rs.getString(2));
					String v2 = long2str(t);
					int v3 = rs.getInt(3);
					int v4 = rs.getInt(4);
					int v5 = rs.getInt(5);
					int v6 = rs.getInt(6);
					float v7 = rs.getFloat(7);
					float v8 = rs.getFloat(8);
					float v9 = rs.getFloat(9);
					int v10 = rs.getInt(10);
					int v11 = rs.getInt(11);
					int v12 = rs.getInt(12);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + TAB + v4 + TAB + v5 + TAB + v6
							+ TAB + v7 + TAB + v8 + TAB + v9 + TAB + v10 + TAB
							+ v11 + TAB + v12 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

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
		int datecount = 0;
		datecount = new Integer(other_args.split(COMMA)[0]);

		String tmp_tabname = "tab_login";

		int before_usercount = 0;
		int saveuser_count = 0;

		// cal before login user count

		drop_login_merge_table(tmp_tabname);
		create_login_merge_table(tmp_tabname,
				addDay(action_date, 0 - datecount), datecount);

		// query before user

		String tab_beforeuid = tmp_tab_pre + "userid_before";
		drop_login_tmp_table(tab_beforeuid);
		create_login_tmp_table(tab_beforeuid);

		drop_login_merge_table(tmp_tabname);
		create_login_merge_table(tmp_tabname,
				addDay(action_date, 0 - datecount), datecount);

		sqlstr = "replace into " + tab_beforeuid
				+ " select distinct iUin from test.merge_tab_login ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query after user
		String tab_afteruid = tmp_tab_pre + "userid_after";
		drop_login_tmp_table(tab_afteruid);
		create_login_tmp_table(tab_afteruid);

		drop_login_merge_table(tmp_tabname);
		create_login_merge_table(tmp_tabname, action_date, datecount);

		sqlstr = "replace into " + tab_afteruid
				+ " select distinct iUin from test.merge_tab_login ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// cal save user count
		sqlstr = " select count(*) from " + tab_beforeuid + " ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			before_usercount = rs.getInt(1);
		}

		sqlstr = "select count(distinct A1.iUin) from " + tab_afteruid
				+ " A1 , " + tab_beforeuid + " A2 where A1.iUin=A2.iUin ;";

		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			saveuser_count = rs.getInt(1);
		}

		resultosw.write(action_date + TAB + datecount + TAB + before_usercount
				+ TAB + saveuser_count + NEWLINE);
		resultosw.flush();

		drop_login_tmp_table(tab_beforeuid);
		drop_login_tmp_table(tab_afteruid);
		drop_login_merge_table(tmp_tabname);

		close_LoginConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void get_lostuserlevel() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();
		open_LoginConn();

		if (other_args == null) {
			return;
		}
		int datecount = 0;
		datecount = new Integer(other_args.split(COMMA)[0]);

		String merge_tabname1 = "tab_login";

		// query before user

		String tmp_tabname1 = tmp_tab_pre + "userid_before";
		drop_login_tmp_table(tmp_tabname1);
		create_login_tmp_table(tmp_tabname1);

		drop_login_merge_table(merge_tabname1);
		create_login_merge_table(merge_tabname1,
				addDay(action_date, 0 - datecount), datecount);

		sqlstr = "replace into " + tmp_tabname1
				+ " select distinct iUin from test.merge_tab_login ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// query after user
		String tmp_tabname2 = tmp_tab_pre + "userid_after";
		drop_login_tmp_table(tmp_tabname2);
		create_login_tmp_table(tmp_tabname2);

		String merge_tabname2 = "tab_login";
		drop_login_merge_table(merge_tabname2);
		create_login_merge_table(merge_tabname2, action_date, datecount);

		sqlstr = "replace into " + tmp_tabname2
				+ " select distinct iUin from test.merge_tab_login ;";
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		stmt.execute(sqlstr);

		// cal lost user
		sqlstr = "select A1.iUin from " + tmp_tabname1 + " A1 left join "
				+ tmp_tabname2
				+ " A2 on A1.iUin=A2.iUin where A2.iUin is null ;";

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

		String merge_tab_townlogin = "tab_town_login";
		drop_merge_table(merge_tab_townlogin);
		create_merge_table(merge_tab_townlogin,
				addDay(action_date, 0 - datecount), datecount);

		String tmp_tabname4 = tmp_tab_pre + "useronedata";
		drop_login_tmp_table(tmp_tabname4);
		create_login_tmp_table(tmp_tabname4);
		sqlstr = "select A.iuin , max(B.score) from " + tmp_tabname3
				+ " A join " + merge_tab_pre + merge_tab_townlogin
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

		int userlevelcnt = 0;
		while (rs.next()) {
			String v1 = rs.getString(1);
			int v2 = rs.getInt(2);
			resultosw.write(action_date + TAB + datecount + TAB + v1 + TAB + v2
					+ NEWLINE);
			userlevelcnt = userlevelcnt + v2;
		}
		resultosw.flush();

		sqlstr = "select count(*) from " + tmp_tabname3;
		logstr = "conn=" + loginhostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = loginConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		int lostusertotalcnt = 0;
		if (rs.next()) {
			lostusertotalcnt = rs.getInt(1);
		}
		int unkowncnt = lostusertotalcnt - userlevelcnt;
		resultosw.write(action_date + TAB + datecount + TAB + UNKOWN + TAB
				+ unkowncnt + NEWLINE);

		resultosw.flush();

		close_Conns();
		close_LoginConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_wish_count() throws Exception {
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

		String before_date = addDay(action_date, -1);
		String before_year = before_date.substring(0, 4);
		String before_month = before_date.substring(5, 7);
		String before_day = before_date.substring(8, 10);

		sqlstr = sql_wish_count.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day)
				.replaceAll("par_beforeyear", before_year)
				.replaceAll("par_beforemonth", before_month)
				.replaceAll("par_beforeday", before_day);

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
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_mapbuyitemcount() throws Exception {
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

		String before_date = addDay(action_date, -1);
		String before_year = before_date.substring(0, 4);
		String before_month = before_date.substring(5, 7);
		String before_day = before_date.substring(8, 10);

		sqlstr = sql_mapbuyitemcount.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day)
				.replaceAll("par_beforeyear", before_year)
				.replaceAll("par_beforemonth", before_month)
				.replaceAll("par_beforeday", before_day);
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
					String v2 = rs.getString(2);
					long v3 = rs.getLong(3);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void get_match_count() throws Exception {
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

		sqlstr = sql_match_count.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day);
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
			resultosw.flush();
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void runIn() throws Exception {

		if (action_date == null || EMPTY.equals(action_date)) {
			action_date = dateformat.format(new Date());
			action_date = addDay(action_date, -1);
		}

		workDir = workDir + action_date + "/";
		initDir(workDir);
		tmpDir = workDir + "tmp/";
		initDir(tmpDir);

		Class c = DianHun_Oss.class;
		Method m = c.getMethod(step);
		m.invoke(c);

	}

	public DianHun_Oss() {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void runOut(String[] args) throws Exception {
		Options opts = new Options();
		opts.addOption("h", "help", false, EMPTY);
		opts.addOption("w", "workDir", true, EMPTY);
		opts.addOption("a", "action_date", true, EMPTY);
		opts.addOption("s", "steps", true, EMPTY);
		opts.addOption("o", "other", true, EMPTY);
		BasicParser parser = new BasicParser();
		CommandLine cl = null;

		cl = parser.parse(opts, args);
		if (cl.getOptions().length > 0) {
			if (cl.hasOption('h')) {
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("Options", opts);
			} else {
				if (cl.hasOption('w')) {
					workDir = cl.getOptionValue("w");
					if (!(workDir.endsWith("/") || workDir.endsWith("\\"))) {
						workDir = workDir + "/";
					}
				}
				if (cl.hasOption('a')) {
					action_date = cl.getOptionValue("a");
				}
				if (cl.hasOption('s')) {
					step = cl.getOptionValue("s");
				}
				if (cl.hasOption('o')) {
					other_args = cl.getOptionValue("o");
				}

				runIn();
			}
		} else {
			HelpFormatter hf = new HelpFormatter();
			hf.printHelp("Options", opts);
		}

	}

	public static void get_mine() throws Exception {

		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = all_ips;
		open_Conns();

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
		start_date = action_date;
		stop_date = addDay(start_date, 1);
		start_time = ZERO_HOURMINSEC;
		stop_time = ZERO_HOURMINSEC;

		start_year = start_date.substring(0, 4);
		start_month = start_date.substring(5, 7);
		start_day = start_date.substring(8, 10);

		start_datetime = start_date + SPACE + start_time;
		stop_datetime = stop_date + SPACE + stop_time;

		for (int m = 0; m < descs.length; m++) {
			String action_name = descs[m].split(COMMA)[0];
			// String action_desc = descs[m].split(COMMA)[1];

			if (action_name.equalsIgnoreCase("mine_bazhan_free")) {
				sqlstr = sqloss_mine_bazhan_free.replaceAll("par_starttime",
						start_datetime).replaceAll("par_stoptime",
						stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_bazhan_charge")) {
				sqlstr = sqloss_mine_bazhan_charge.replaceAll("par_starttime",
						start_datetime).replaceAll("par_stoptime",
						stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_bazhan_win")) {
				sqlstr = sqloss_mine_bazhan_win.replaceAll("par_starttime",
						start_datetime).replaceAll("par_stoptime",
						stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_bazhan_fail")) {
				sqlstr = sqloss_mine_bazhan_fail.replaceAll("par_starttime",
						start_datetime).replaceAll("par_stoptime",
						stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_bazhan_isoccupy")) {
				sqlstr = sqloss_mine_bazhan_isoccupy.replaceAll(
						"par_starttime", start_datetime).replaceAll(
						"par_stoptime", stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_bazhan_notoccupy")) {
				sqlstr = sqloss_mine_bazhan_notoccupy.replaceAll(
						"par_starttime", start_datetime).replaceAll(
						"par_stoptime", stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_qiangduo_free")) {
				sqlstr = sqloss_mine_qiangduo_free.replaceAll("par_starttime",
						start_datetime).replaceAll("par_stoptime",
						stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_qiangduo_charge")) {
				sqlstr = sqloss_mine_qiangduo_charge.replaceAll(
						"par_starttime", start_datetime).replaceAll(
						"par_stoptime", stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_qiangduo_win")) {
				sqlstr = sqloss_mine_qiangduo_win.replaceAll("par_starttime",
						start_datetime).replaceAll("par_stoptime",
						stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_qiangduo_fail")) {
				sqlstr = sqloss_mine_qiangduo_fail.replaceAll("par_starttime",
						start_datetime).replaceAll("par_stoptime",
						stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_qiangduo_isoccupy")) {
				sqlstr = sqloss_mine_qiangduo_isoccupy.replaceAll(
						"par_starttime", start_datetime).replaceAll(
						"par_stoptime", stop_datetime);
			}
			if (action_name.equalsIgnoreCase("mine_qiangduo_notoccupy")) {
				sqlstr = sqloss_mine_qiangduo_notoccupy.replaceAll(
						"par_starttime", start_datetime).replaceAll(
						"par_stoptime", stop_datetime);
			}
			sqlstr = sqlstr.replaceAll("par_year", start_year)
					.replaceAll("par_month", start_month)
					.replaceAll("par_day", start_day);
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
						resultosw.write(start_date + TAB + action_name + TAB
								+ v1 + TAB + v2 + NEWLINE);
					}
				} catch (SQLException e) {
					printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
					continue;
				}
			}
		}

		close_Conns();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void get_multithd() throws Exception {
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
		// guanka
		sqlstr = sql_guanka_userlevel_distribution
				.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day)
				.replaceAll("par_userlevel", sql_userlevel_common_2);

		ExecutorService exec = Executors.newFixedThreadPool(conns.length);
		final Lock lock = new ReentrantLock();

		HashMap taskMap = new HashMap<String, Future>();
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
						lock.lock();
						try {
							while (rs.next()) {
								String v1 = rs.getString(1);
								String v2 = rs.getString(2);
								int v3 = rs.getInt(3);
								resultosw.write(action_date + TAB + zoneId
										+ TAB + "guanka" + TAB + v1 + TAB + v2
										+ TAB + v3 + NEWLINE);
							}
							resultosw.flush();
						} finally {
							lock.unlock();
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

		Iterator iter = taskMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			Future val = (Future) entry.getValue();
			String ret = (String) val.get();
			printLogStr(key + TAB + ret);
		}

		// race
		sqlstr = sql_race_userlevel_distribution
				.replaceAll("par_year", start_year)
				.replaceAll("par_month", start_month)
				.replaceAll("par_day", start_day)
				.replaceAll("par_userlevel", sql_userlevel_common_2);

		taskMap.clear();
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
						lock.lock();
						try {
							while (rs.next()) {
								String v1 = rs.getString(1);
								String v2 = rs.getString(2);
								int v3 = rs.getInt(3);
								resultosw.write(action_date + TAB + zoneId
										+ TAB + "race" + TAB + v1 + TAB + v2
										+ TAB + v3 + NEWLINE);
							}
							resultosw.flush();
						} finally {
							lock.unlock();
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
}
