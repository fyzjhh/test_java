package com.test.dianhun;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class Hadoop_Temp extends Hadoop {

	public static void main(String[] args) throws Exception {
		args = "-w /data/tmp/j_resultdir/ -a 2014-06-12 -s read_conf "
				.split(" +");
		runOut(args);
		System.out.println("====success====");
	}

	public Hadoop_Temp() {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void runOut(String[] args) throws Exception {
		Options opts = new Options();
		opts.addOption("h", "help", false, EMPTY);
		opts.addOption("w", "workDir", true, EMPTY);
		opts.addOption("a", "action_date", true, EMPTY);
		opts.addOption("s", "step", true, EMPTY);
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

	/*
	 * 因需要分析此次lol“无限火力”模式对我们活跃用户的影响，需调取以下数据：
	 * 调查对象：4月4日-7日、4月11日-14日、5月16日-19日、5月23日-26日的活跃用户（共16天）
	 * 调查需求：地图（关卡地图不分明细、竞技地图分明细）、地图次数（次数为一周次数，例如5月23日的即为5月17日-23日各地图总次数）
	 */
	public static void get_userinfo4() throws Exception {

		before_exec();

		String tmp_action_date = other_args.split(COMMA)[0];

		String login_start_date = tmp_action_date;
		String login_stop_date = tmp_action_date;

		String action_start_date = addDay(tmp_action_date, -7);
		String action_stop_date = tmp_action_date;

		String final_login_stop_date = addDay(login_stop_date, 1);
		String final_action_stop_date = addDay(action_stop_date, 1);

		String login_par_partition = cal_partitionstr(login_start_date,
				login_stop_date);
		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		// query userid

		String tmp_userid_1 = tmp_tab_pre + "userid_1" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_1);
		create_tmp_table(tmp_userid_1);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_1
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login_par_partition)
				.replaceAll("par_startdate", login_start_date)
				.replaceAll("par_stopdate", final_login_stop_date);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query 竞技

		String tmp_useronedata_race = tmp_tab_pre + "usertwostr1_race"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_race);
		create_tmp_table(tmp_useronedata_race);

		basesql = " select t1.iuin,t2.imapid from "
				+ tmp_userid_1
				+ " t1 join par_dbname.tab_map_race t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_useronedata_race
				+ " select t.iuin, count(t.iuin) ,t.imapid from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin , t.imapid";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query 关卡

		String tmp_useronedata_guanka = tmp_tab_pre + "useronedata_guanka"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_guanka);
		create_tmp_table(tmp_useronedata_guanka);

		basesql = " select t1.iuin from "
				+ tmp_userid_1
				+ " t1 join par_dbname.tab_map_guanka t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_useronedata_guanka
				+ " select t.iuin , count(t.iuin) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// final

		try {
			sqlstr = "select t1.iuin , t2.value2 , t2.value1 , t3.value1 from "
					+ tmp_userid_1 + " t1 left outer join "
					+ tmp_useronedata_race
					+ " t2 on (t1.iuin=t2.iuin) left outer join "
					+ tmp_useronedata_guanka + " t3 on (t1.iuin=t3.iuin) ";

			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				String v2 = rs.getString(2);
				String v3 = rs.getString(3);
				String v4 = rs.getString(4);
				resultstr = tmp_action_date + TAB + v1 + TAB + v2 + TAB + v3
						+ TAB + v4 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_talent() throws Exception {

		before_exec();

		String action_start_date = other_args.split(COMMA)[0];
		String action_stop_date = other_args.split(COMMA)[1];

		String final_action_stop_date = addDay(action_stop_date, 1);

		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		// query score
		String tmp_userone_score = tmp_tab_pre + "useronedata_score"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userone_score);
		create_tmp_table(tmp_userone_score);

		basesql = " select playerid,max(score) v from par_dbname.tab_town_leave where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' group by playerid ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_userone_score
				+ " select t.playerid , max(t.v) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.playerid ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query talent

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			String tmp_useronedata_talent = tmp_tab_pre + "usertwodata_talent"
					+ UNDERLINE + step;
			drop_tmp_table(tmp_useronedata_talent);
			create_tmp_table(tmp_useronedata_talent);

			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_useronedata_talent
					+ " select char_id,max(param1),max(param2) from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=10 and sub_type=2002 and param1>0 group by char_id ";
			sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);

			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// final

			try {
				sqlstr = "select t1.iuin ,  par_userlevel, t1.value1 , t1.value2 from "
						+ tmp_useronedata_talent
						+ " t1 left outer join "
						+ tmp_userone_score + " t2 on (t1.iuin=t2.iuin) ";
				basesql = sql_userlevel_common.replaceAll("result_value",
						"t2.value1");
				sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					String v1 = rs.getString(1);
					String v2 = rs.getString(2);
					String v3 = rs.getString(3);
					String v4 = rs.getString(4);
					resultstr = zoneId + TAB + v1 + TAB + v2 + TAB + v3 + TAB
							+ v4 + NEWLINE;
					resultosw.write(resultstr);
				}
				resultosw.flush();

			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	/*
	 * 5月31日0:00—6月2日23:59期间，进行三国志大战地图游戏的玩家（大区、ID，该数据可重复）
	 */
	public static void get_userid_bycondition() throws Exception {

		before_exec();

		String action_start_date = other_args.split(COMMA)[0];
		String action_stop_date = other_args.split(COMMA)[1];
		String final_action_stop_date = addDay(action_stop_date, 1);

		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		try {
			for (int i = 0; i < zoneDbStrs.length; i++) {
				String zoneStr = zoneDbStrs[i];
				int zoneId = zoneIds[i];

				// final
				sqlstr = " select iuin from par_dbname.tab_map_race where par_datetime in (par_partition) and iRecordTime>='par_startdate' and iRecordTime<'par_stopdate' and iMapID='DT57' and iTime>=1200 ";
				sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", action_par_partition)
						.replaceAll("par_startdate", action_start_date)
						.replaceAll("par_stopdate", final_action_stop_date);

				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					resultstr = zoneId + TAB + v1 + NEWLINE;
					resultosw.write(resultstr);
				}
				resultosw.flush();
			}
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_userinfo_lifecycle() throws Exception {

		before_exec();

		String reg_start_date = other_args.split(COMMA)[0];
		String reg_stop_date = other_args.split(COMMA)[1];

		String action_start_date = other_args.split(COMMA)[2];
		String action_stop_date = other_args.split(COMMA)[3];

		String final_reg_stop_date = addDay(reg_stop_date, 1);
		String final_action_stop_date = addDay(action_stop_date, 1);

		String reg_par_partition = cal_partitionstr(reg_start_date,
				reg_stop_date);
		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		// query userid

		String tmp_userid_reg_all = tmp_tab_pre + "userid_reg_all" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_reg_all);
		create_tmp_table(tmp_userid_reg_all);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_all
				+ " select t2.iuin from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on "
				+ "(t1.product_id=1297303375 and t1.ad_id=t2.media_id and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' ) ";
		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_start_date)
				.replaceAll("par_stopdate", final_reg_stop_date);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		String type_race = "race";
		String type_guanka = "guanka";
		String type_item_sell = "item_sell";
		// query 竞技

		basesql = " select t1.iuin,t2.imapid , par_userlevel from "
				+ tmp_userid_reg_all
				+ " t1 join par_dbname.tab_map_race t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) ";

		sqlstr = " select t.iuin,t.userlevel,t.imapid , count(*) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql
					.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date)
					.replaceAll(
							"par_userlevel",
							sql_userlevel_common.replaceAll("result_value",
									"t2.PlayerScore"));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin,t.userlevel,t.imapid ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = hiveConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			String v2 = rs.getString(2);
			String v3 = rs.getString(3);
			int v4 = rs.getInt(4);
			resultstr = type_race + TAB + v1 + TAB + v2 + TAB + v3 + TAB + v4
					+ NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		// query 关卡

		basesql = " select t1.iuin ,t2.imapid , par_userlevel from "
				+ tmp_userid_reg_all
				+ " t1 join par_dbname.tab_map_guanka t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) ";

		sqlstr = " select t.iuin,t.userlevel,t.imapid , count(*) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql
					.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date)
					.replaceAll(
							"par_userlevel",
							sql_userlevel_common.replaceAll("result_value",
									"t2.PlayerScore"));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin,t.userlevel,t.imapid ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = hiveConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			String v2 = rs.getString(2);
			String v3 = rs.getString(3);
			int v4 = rs.getInt(4);
			resultstr = type_guanka + TAB + v1 + TAB + v2 + TAB + v3 + TAB + v4
					+ NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		// query 购买物品

		basesql = " select t1.iuin ,t2.igoodstype , par_userlevel from "
				+ tmp_userid_reg_all
				+ " t1 join par_dbname.tab_item_sell t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' and t2.Ireason not in (1003) ) ";

		sqlstr = " select t.iuin,t.userlevel,t.igoodstype , count(*) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql
					.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date)
					.replaceAll(
							"par_userlevel",
							sql_userlevel_common.replaceAll("result_value",
									"t2.PlayerScore"));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin,t.userlevel,t.igoodstype ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		stmt = hiveConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			String v2 = rs.getString(2);
			String v3 = rs.getString(3);
			int v4 = rs.getInt(4);
			resultstr = type_item_sell + TAB + v1 + TAB + v2 + TAB + v3 + TAB
					+ v4 + NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		after_exec();

	}

	/*
	 * 为评估封工作室账号对活跃用户的影响（5月19-23日封停一批），需按上次格式再要几天的数据
	 * 5月10日，5月17日，5月24日，5月25日的活跃用户列表，列表如下： 日期 用户id 注册时间 注册ip 注册地址 等级 竞技次数 关卡次数
	 * 最后登录ip
	 */

	public static void get_userinfo1() throws Exception {

		before_exec();

		String tmp_action_date = other_args.split(COMMA)[0];

		String reg_start_date = addDay(tmp_action_date, -60);
		String reg_stop_date = tmp_action_date;

		String login_start_date = tmp_action_date;
		String login_stop_date = tmp_action_date;

		String action_start_date = addDay(tmp_action_date, -7);
		String action_stop_date = tmp_action_date;

		String final_reg_stop_date = addDay(reg_stop_date, 1);
		String final_login_stop_date = addDay(login_stop_date, 1);
		String final_action_stop_date = addDay(action_stop_date, 1);

		String reg_par_partition = cal_partitionstr(reg_start_date,
				reg_stop_date);
		String login_par_partition = cal_partitionstr(login_start_date,
				login_stop_date);
		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function ip2str as 'com.udf.IpGetter'");
		stmt.execute("create temporary function rank as 'com.udf.Rank'");

		// query userid

		String tmp_userid_ip = tmp_tab_pre + "useronestr1_ip" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_ip);
		create_tmp_table(tmp_userid_ip);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_ip
				+ " select iuin ,vClientIp from ( select iuin ,rank(iuin) r ,dtLogTime,vClientIp from ( select iuin ,dtLogTime,vClientIp from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' order by iuin ,dtLogTime desc ) t1 ) t where r = 0 ";
		sqlstr = sqlstr.replaceAll("par_partition", login_par_partition)
				.replaceAll("par_startdate", login_start_date)
				.replaceAll("par_stopdate", final_login_stop_date);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query score
		String tmp_userone_score = tmp_tab_pre + "useronedata_score"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userone_score);
		create_tmp_table(tmp_userone_score);

		basesql = " select t1.iuin,max(t2.score) v from "
				+ tmp_userid_ip
				+ " t1 join par_dbname.tab_town_leave t2 on ( t1.iuin = t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate' ) group by t1.iuin  ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_userone_score
				+ " select t.iuin , max(t.v) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", login_par_partition)
					.replaceAll("par_startdate", login_start_date)
					.replaceAll("par_stopdate", final_login_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query 竞技

		String tmp_useronedata_race = tmp_tab_pre + "useronedata_race"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_race);
		create_tmp_table(tmp_useronedata_race);

		basesql = " select t1.iuin from "
				+ tmp_userid_ip
				+ " t1 join par_dbname.tab_map_race t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_useronedata_race
				+ " select t.iuin , count(t.iuin) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query 关卡

		String tmp_useronedata_guanka = tmp_tab_pre + "useronedata_guanka"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_guanka);
		create_tmp_table(tmp_useronedata_guanka);

		basesql = " select t1.iuin from "
				+ tmp_userid_ip
				+ " t1 join par_dbname.tab_map_guanka t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_useronedata_guanka
				+ " select t.iuin , count(t.iuin) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// final

		try {
			sqlstr = "select t1.iuin ,par_userlevel,t3.value1,t4.value1,t1.value1 from "
					+ tmp_userid_ip
					+ " t1 left outer join "
					+ tmp_userone_score
					+ " t2 on (t1.iuin=t2.iuin) left outer join "
					+ tmp_useronedata_race
					+ " t3 on (t1.iuin=t3.iuin) left outer join "
					+ tmp_useronedata_guanka + " t4 on (t1.iuin=t4.iuin) ";
			basesql = sql_userlevel_common.replaceAll("result_value",
					"t2.value1");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);

			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				int v1 = rs.getInt(1);
				String v2 = rs.getString(2);
				int v3 = rs.getInt(3);
				int v4 = rs.getInt(4);
				String v5 = rs.getString(5);
				resultstr = tmp_action_date + TAB + v1 + TAB + v2 + TAB + v3
						+ TAB + v4 + TAB + v5 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

			//
			// sqlstr =
			// "select t1.iuin , t5.dtlogtime,t5.ip,ip2str(t5.ip),par_userlevel,t3.value1,t4.value1,t1.value1 from "
			// + tmp_userid_ip
			// + " t1 left outer join "
			// + tmp_userone_score
			// + " t2 on (t1.iuin=t2.iuin) left outer join "
			// + tmp_useronedata_race
			// + " t3 on (t1.iuin=t3.iuin) left outer join "
			// + tmp_useronedata_guanka
			// +
			// " t4 on (t1.iuin=t4.iuin) left outer join caochuan_2.accountlog t5 on (t1.iuin=t5.iuin and t5.par_datetime in (par_partition) and t5.dtlogtime>='par_startdate' and t5.dtlogtime<'par_stopdate' ) ";
			// basesql = sql_userlelve_common.replaceAll("result_value",
			// "t2.value1");
			// sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
			// sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
			// .replaceAll("par_startdate", reg_start_date)
			// .replaceAll("par_stopdate", final_reg_stop_date);
			// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			// printLogStr(logstr);
			//
			// stmt = hiveConn.createStatement();
			// rs = stmt.executeQuery(sqlstr);
			// while (rs.next()) {
			// int v1 = rs.getInt(1);
			// String v2 = rs.getString(2);
			// String v3 = rs.getString(3);
			// String v4 = rs.getString(4);
			// String v5 = rs.getString(5);
			// int v6 = rs.getInt(6);
			// int v7 = rs.getInt(7);
			// String v8 = rs.getString(8);
			// resultstr = tmp_action_date + TAB + v1 + TAB + v2 + TAB + v3
			// + TAB + v4 + TAB + v5 + TAB + v6 + TAB + v7 + TAB + v8
			// + NEWLINE;
			// resultosw.write(resultstr);
			// }
			// resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_userinfo2() throws Exception {

		before_exec();

		String tmp_action_date = other_args.split(COMMA)[0];

		String reg_start_date = addDay(tmp_action_date, -60);
		String reg_stop_date = tmp_action_date;

		String login_start_date = tmp_action_date;
		String login_stop_date = tmp_action_date;

		String action_start_date = addDay(tmp_action_date, -7);
		String action_stop_date = tmp_action_date;

		String final_reg_stop_date = addDay(reg_stop_date, 1);
		String final_login_stop_date = addDay(login_stop_date, 1);
		String final_action_stop_date = addDay(action_stop_date, 1);

		String reg_par_partition = cal_partitionstr(reg_start_date,
				reg_stop_date);
		String login_par_partition = cal_partitionstr(login_start_date,
				login_stop_date);
		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function ip2str as 'com.udf.IpGetter'");
		stmt.execute("create temporary function rank as 'com.udf.Rank'");

		// query userid

		String tmp_userid_ip = tmp_tab_pre + "useronestr1_ip" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_ip);
		create_tmp_table(tmp_userid_ip);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_ip
				+ " select iuin ,vClientIp from ( select iuin ,rank(iuin) r ,dtLogTime,vClientIp from ( select iuin ,dtLogTime,vClientIp from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' order by iuin ,dtLogTime desc ) t1 ) t where r = 0 ";
		sqlstr = sqlstr.replaceAll("par_partition", login_par_partition)
				.replaceAll("par_startdate", login_start_date)
				.replaceAll("par_stopdate", final_login_stop_date);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query 竞技

		String tmp_useronedata_race = tmp_tab_pre + "useronedata_race"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_race);
		create_tmp_table(tmp_useronedata_race);

		basesql = " select t1.iuin , t2.iMapID from "
				+ tmp_userid_ip
				+ " t1 join par_dbname.tab_map_race t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) ";

		sqlstr = " select t11.iuin ,t12.map_name, count(t11.iuin) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s
						+ " ) t11 join gamemeta.m3gcn_map_info t12 on (t11.iMapID=t12.imapid) group by t11.iuin,t12.map_name ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		// final

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				int v1 = rs.getInt(1);
				String v2 = rs.getString(2);
				int v3 = rs.getInt(3);
				resultstr = tmp_action_date + TAB + v1 + TAB + v2 + TAB + v3
						+ NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	/*
	 * 
	 * 2014年3月1日以来活跃玩家（登陆过梦三）VIP玩家分布数据，即
	 * 
	 * 1、VIP0级的一共有多少位玩家 2、VIP1级的一共有多少位玩家 . . 7、VIP6级的一共有多少位玩家
	 */

	public static void get_activeuser_viplevel() throws Exception {

		before_exec();

		String action_start_date = other_args.split(COMMA)[0];
		String action_stop_date = other_args.split(COMMA)[1];

		String final_action_stop_date = addDay(action_stop_date, 1);

		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		String tmp_strtwo_1 = tmp_tab_pre + "strtwo" + UNDERLINE + step;
		drop_tmp_table(tmp_strtwo_1);
		create_tmp_table(tmp_strtwo_1);

		basesql = " select playerid,viplevel from par_dbname.tab_town_leave where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_strtwo_1
				+ " select t.playerid , max(t.viplevel) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.playerid ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// final
		try {
			sqlstr = "select value2 , count(value1) from " + tmp_strtwo_1
					+ " group by value2 ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				int v2 = rs.getInt(2);
				resultstr = v1 + TAB + v2 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_lostuserkickedcount() throws Exception {

		before_exec();

		String login1_startdate = other_args.split(COMMA)[0];
		String login1_stopdate = other_args.split(COMMA)[1];
		String login2_startdate = other_args.split(COMMA)[2];
		String login2_stopdate = other_args.split(COMMA)[3];

		String final_login1_stopdate = addDay(login1_stopdate, 1);
		String final_login2_stopdate = addDay(login2_stopdate, 1);

		String login1_par_partition = cal_partitionstr(login1_startdate,
				login1_stopdate);
		String login2_par_partition = cal_partitionstr(login2_startdate,
				login2_stopdate);

		// query login1 id

		String tmp_userid_login1 = tmp_tab_pre + "userid_login1" + UNDERLINE
				+ step;
		// drop_tmp_table(tmp_userid_login1);
		// create_tmp_table(tmp_userid_login1);
		//
		// sqlstr = "insert OVERWRITE TABLE "
		// + tmp_userid_login1
		// +
		// " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		// sqlstr = sqlstr.replaceAll("par_partition", login1_par_partition)
		// .replaceAll("par_startdate", login1_startdate)
		// .replaceAll("par_stopdate", final_login1_stopdate);
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// query login2 id

		String tmp_userid_login2 = tmp_tab_pre + "userid_login2" + UNDERLINE
				+ step;
		// drop_tmp_table(tmp_userid_login2);
		// create_tmp_table(tmp_userid_login2);
		//
		// sqlstr = "insert OVERWRITE TABLE "
		// + tmp_userid_login2
		// +
		// " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		// sqlstr = sqlstr.replaceAll("par_partition", login2_par_partition)
		// .replaceAll("par_startdate", login2_startdate)
		// .replaceAll("par_stopdate", final_login2_stopdate);
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// query lost user id
		String tmp_userid_lostuser = tmp_tab_pre + "userid_lostuser"
				+ UNDERLINE + step;
		// drop_tmp_table(tmp_userid_lostuser);
		// create_tmp_table(tmp_userid_lostuser);
		//
		// sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lostuser
		// + " select t1.iuin from " + tmp_userid_login1
		// + " t1 left outer join " + tmp_userid_login2
		// + " t2 on t1.iuin=t2.iuin where t2.iuin is null ";
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// query 开局次数
		String tmp_usertwo_play = tmp_tab_pre + "usertwostr1_play" + UNDERLINE
				+ step;
		// drop_tmp_table(tmp_usertwo_play);
		// create_tmp_table(tmp_usertwo_play);
		//
		// sqlstr = "insert OVERWRITE TABLE " + tmp_usertwo_play
		// + " select t.iuin , sum(v), t.imapid from ( ";
		//
		// // race
		// basesql =
		// " select t1.iuin ,count(distinct t2.raceid) v, t2.imapid imapid from "
		// + tmp_userid_lostuser
		// +
		// " t1 join par_dbname.tab_map_race t2 on (t1.iuin=t2.iuin and t2.par_datetime in (par_partition) and t2.irecordtime>='par_startdate' and t2.irecordtime<'par_stopdate' ) group by t1.iuin , t2.imapid";
		// for (int i = 0; i < zoneDbStrs.length; i++) {
		// String zoneStr = zoneDbStrs[i];
		// String s = basesql.replaceAll("par_dbname", zoneStr)
		// .replaceAll("par_partition", login1_par_partition)
		// .replaceAll("par_startdate", login1_startdate)
		// .replaceAll("par_stopdate", final_login1_stopdate);
		// if (i == zoneDbStrs.length - 1) {
		// sqlstr += s + " union all ";
		// } else {
		// sqlstr += s + " union all ";
		// }
		// }
		// // guanka
		// basesql =
		// " select t1.iuin , count(distinct t2.raceid) v, t2.imapid imapid from "
		// + tmp_userid_lostuser
		// +
		// " t1 join par_dbname.tab_map_guanka t2 on (t1.iuin=t2.iuin and t2.par_datetime in (par_partition) and t2.irecordtime>='par_startdate' and t2.irecordtime<'par_stopdate' ) group by t1.iuin , t2.imapid";
		// for (int i = 0; i < zoneDbStrs.length; i++) {
		// String zoneStr = zoneDbStrs[i];
		// String s = basesql.replaceAll("par_dbname", zoneStr)
		// .replaceAll("par_partition", login1_par_partition)
		// .replaceAll("par_startdate", login1_startdate)
		// .replaceAll("par_stopdate", final_login1_stopdate);
		// if (i == zoneDbStrs.length - 1) {
		// sqlstr += s + " ) t group by t.iuin,t.imapid ";
		// } else {
		// sqlstr += s + " union all ";
		// }
		// }
		//
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// 按照用户和地图计算 被踢的次数
		String tmp_usertwo = tmp_tab_pre + "usertwostr1" + UNDERLINE + step;
		// drop_tmp_table(tmp_usertwo);
		// create_tmp_table(tmp_usertwo);
		//
		// basesql = " select t1.iuin , t2.mapid , 1 as kicked_count  from "
		// + tmp_userid_lostuser
		// +
		// " t1 join par_dbname.tab_kicked_player t2 on (t1.iuin=t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate' ) ";
		// sqlstr = "insert OVERWRITE TABLE " + tmp_usertwo
		// + " select iuin , sum(kicked_count) , mapid from ( ";
		// for (int i = 0; i < zoneDbStrs.length; i++) {
		// String zoneStr = zoneDbStrs[i];
		// String s = basesql.replaceAll("par_dbname", zoneStr)
		// .replaceAll("par_partition", login1_par_partition)
		// .replaceAll("par_startdate", login1_startdate)
		// .replaceAll("par_stopdate", final_login1_stopdate);
		// if (i == zoneDbStrs.length - 1) {
		// sqlstr += s + " ) t group by iuin ,mapid ";
		// } else {
		// sqlstr += s + " union all ";
		// }
		// }
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);

		execSql();
		// 计算用户的等级
		String tmp_userone_level = tmp_tab_pre + "useronedata_level"
				+ UNDERLINE + step;
		// drop_tmp_table(tmp_userone_level);
		// create_tmp_table(tmp_userone_level);
		//
		// basesql = " select t1.iuin , t2.scorelevel from "
		// + tmp_userid_lostuser
		// +
		// " t1 join par_dbname.tab_kicked_player t2 on (t1.iuin=t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate' ) ";
		// sqlstr = "insert OVERWRITE TABLE " + tmp_userone_level
		// + " select iuin , max(scorelevel) from ( ";
		// for (int i = 0; i < zoneDbStrs.length; i++) {
		// String zoneStr = zoneDbStrs[i];
		// String s = basesql.replaceAll("par_dbname", zoneStr)
		// .replaceAll("par_partition", login1_par_partition)
		// .replaceAll("par_startdate", login1_startdate)
		// .replaceAll("par_stopdate", final_login1_stopdate);
		// if (i == zoneDbStrs.length - 1) {
		// sqlstr += s + " ) t group by iuin ";
		// } else {
		// sqlstr += s + " union all ";
		// }
		// }
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);

		execSql();
		// final
		try {
			// 开局
			sqlstr = "select t2.value1 , t3.map_name , sum(t1.value1) from "
					+ tmp_usertwo_play
					+ " t1 join "
					+ tmp_userone_level
					+ " t2 on (t1.iuin=t2.iuin) join gamemeta.m3gcn_map_info t3 on (t1.value2=t3.imapid)"
					+ " group by t2.value1 , t3.map_name ";

			basesql = sql_userlevel_common.replaceAll("result_value",
					"t2.value1");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);

			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				int v1 = rs.getInt(1);
				String v2 = rs.getString(2);
				int v3 = rs.getInt(3);
				resultstr = v1 + TAB + v2 + TAB + v3 + TAB + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

			// kicked
			// sqlstr =
			// "select t2.value1 , t3.map_name , t1.value1 ,count(distinct t1.iuin),sum(t1.value1) from "
			// + tmp_usertwo
			// + " t1 join "
			// + tmp_userone_level
			// +
			// " t2 on (t1.iuin=t2.iuin) join gamemeta.m3gcn_map_info t3 on (t1.value2=t3.imapid)"
			// + " group by t2.value1 , t3.map_name , t1.value1 ";
			//
			// basesql = sql_userlelve_common.replaceAll("result_value",
			// "t2.value1");
			// sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
			//
			// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			// printLogStr(logstr);
			// stmt = hiveConn.createStatement();
			// rs = stmt.executeQuery(sqlstr);
			// while (rs.next()) {
			// int v1 = rs.getInt(1);
			// String v2 = rs.getString(2);
			// int v3 = rs.getInt(3);
			// int v4 = rs.getInt(4);
			// int v5 = rs.getInt(5);
			// resultstr = v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB + v5
			// + NEWLINE;
			// resultosw.write(resultstr);
			// }
			// resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	/*
	 * 麻烦帮忙查下以下三个时间段内，梦三登录用户数分别是多少人。谢谢~ 4月29日7:00—8:00 5月6日8:00—10:00
	 * 5月8日9:00—12:00
	 */

	public static void get_loginusercount() throws Exception {

		before_exec();

		String login_startdatetime = other_args.split(COMMA)[0].replaceAll(
				UNDERLINE, SPACE);
		String login_stopdatetime = other_args.split(COMMA)[1].replaceAll(
				UNDERLINE, SPACE);

		String final_login_stopdatetime = addHour(login_stopdatetime, 1);

		String login_startdate = login_startdatetime.substring(0, 10);
		String login_stopdate = login_stopdatetime.substring(0, 10);
		String login_par_partition = cal_partitionstr(login_startdate,
				login_stopdate);

		// query login1 id

		sqlstr = " select count(distinct iuin) from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login_par_partition)
				.replaceAll("par_startdate", login_startdatetime)
				.replaceAll("par_stopdate", final_login_stopdatetime);

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = hiveConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultstr = login_startdatetime + TAB + final_login_stopdatetime
					+ TAB + v1 + NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		after_exec();

	}

	/*
	 * 6月5日 10:00-14:00 梦三登录数（杭州地区ip） 14:00-18:45 梦三全区全服登录数
	 */
	public static void get_loginusercount_addr() throws Exception {

		before_exec();

		String login_startdatetime = other_args.split(COMMA)[0].replaceAll(
				UNDERLINE, SPACE);
		String login_stopdatetime = other_args.split(COMMA)[1].replaceAll(
				UNDERLINE, SPACE);

		String final_login_stopdatetime = addHour(login_stopdatetime, 1);

		String login_startdate = login_startdatetime.substring(0, 10);
		String login_stopdate = login_stopdatetime.substring(0, 10);
		String login_par_partition = cal_partitionstr(login_startdate,
				login_stopdate);

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function ip2str as 'com.udf.IpGetter'");

		// query login1 id

		sqlstr = "select count( distinct iuin ) from ( select iuin,ip2str(vclientip) addr from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ) t where t.addr like '%杭州%' ";
		sqlstr = sqlstr.replaceAll("par_partition", login_par_partition)
				.replaceAll("par_startdate", login_startdatetime)
				.replaceAll("par_stopdate", final_login_stopdatetime);

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = hiveConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			resultstr = login_startdatetime + TAB + final_login_stopdatetime
					+ TAB + v1 + NEWLINE;
			resultosw.write(resultstr);
		}
		resultosw.flush();

		after_exec();

	}

	public static void get_lostuseritemcount() throws Exception {

		before_exec();

		String reg_startdate = other_args.split(COMMA)[0];
		String reg_stopdate = other_args.split(COMMA)[1];
		String login1_startdate = reg_startdate;
		String login1_stopdate = reg_stopdate;
		String login2_startdate = other_args.split(COMMA)[2];
		String login2_stopdate = other_args.split(COMMA)[3];

		String final_reg_stopdate = addDay(reg_stopdate, 1);
		String final_login1_stopdate = addDay(login1_stopdate, 1);
		String final_login2_stopdate = addDay(login2_stopdate, 1);

		String reg_par_partition = cal_partitionstr(reg_startdate, reg_stopdate);
		String login1_par_partition = cal_partitionstr(login1_startdate,
				login1_stopdate);
		String login2_par_partition = cal_partitionstr(login2_startdate,
				login2_stopdate);

		// query all reg id
		String tmp_userid_reg = tmp_tab_pre + "userid_1_all" + UNDERLINE + step;
		// drop_tmp_table(tmp_userid_reg);
		// create_tmp_table(tmp_userid_reg);
		//
		// sqlstr = "insert OVERWRITE TABLE "
		// + tmp_userid_reg
		// +
		// " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		// sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
		// .replaceAll("par_startdate", reg_startdate)
		// .replaceAll("par_stopdate", final_reg_stopdate);
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// query login1 id

		String tmp_userid_login1 = tmp_tab_pre + "userid_login1" + UNDERLINE
				+ step;
		// drop_tmp_table(tmp_userid_login1);
		// create_tmp_table(tmp_userid_login1);
		//
		// sqlstr = "insert OVERWRITE TABLE "
		// + tmp_userid_login1
		// +
		// " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		// sqlstr = sqlstr.replaceAll("par_partition", login1_par_partition)
		// .replaceAll("par_startdate", login1_startdate)
		// .replaceAll("par_stopdate", final_login1_stopdate);
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// query login2 id

		String tmp_userid_login2 = tmp_tab_pre + "userid_login2" + UNDERLINE
				+ step;
		// drop_tmp_table(tmp_userid_login2);
		// create_tmp_table(tmp_userid_login2);
		//
		// sqlstr = "insert OVERWRITE TABLE "
		// + tmp_userid_login2
		// +
		// " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		// sqlstr = sqlstr.replaceAll("par_partition", login2_par_partition)
		// .replaceAll("par_startdate", login2_startdate)
		// .replaceAll("par_stopdate", final_login2_stopdate);
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// query lost user id
		String tmp_userid_lostuser = tmp_tab_pre + "userid_lostuser"
				+ UNDERLINE + step;
		// drop_tmp_table(tmp_userid_lostuser);
		// create_tmp_table(tmp_userid_lostuser);
		//
		// sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lostuser
		// + "  select t11.iuin from  ( select t1.iuin from "
		// + tmp_userid_reg + " t1  join " + tmp_userid_login1
		// + " t2 on t1.iuin=t2.iuin ) t11 left outer join "
		// + tmp_userid_login2
		// + " t12 on t11.iuin=t12.iuin where t12.iuin is null ";
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// query item
		String tmp_usertwo_1 = tmp_tab_pre + "usertwostr1_1" + UNDERLINE + step;
		// drop_tmp_table(tmp_usertwo_1);
		// create_tmp_table(tmp_usertwo_1);
		//
		// basesql =
		// " select t1.iuin , t2.igoodstype , sum(case when t2.ichangetype in ( 12,11,17,33,34,38 ) then (case when t2.iBeforeNum=t2.iAfterNum then t2.iAfterNum else abs(t2.iAfterNum - t2.iBeforeNum) end) else 0 end) - sum(case when t2.ichangetype in ( 20,21,22,2,4,7,8,16 ) then (case when t2.iBeforeNum=t2.iAfterNum then t2.iAfterNum else abs(t2.iAfterNum - t2.iBeforeNum) end) else 0 end) as v from "
		// + tmp_userid_lostuser
		// +
		// " t1 join par_dbname.tab_item t2 on (t1.iuin=t2.iuin and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' and t2.ichangetype in ( 12,11,20,21,22,2,4,7,8 ) ) group by t1.iuin ,t2.igoodstype ";
		// sqlstr = "insert OVERWRITE TABLE " + tmp_usertwo_1
		// + " select t.iuin , sum(t.v) , t.igoodstype from ( ";
		// for (int i = 0; i < zoneDbStrs.length; i++) {
		// String zoneStr = zoneDbStrs[i];
		// String s = basesql.replaceAll("par_dbname", zoneStr)
		// .replaceAll("par_partition", login1_par_partition)
		// .replaceAll("par_startdate", login1_startdate)
		// .replaceAll("par_stopdate", final_login1_stopdate);
		// if (i == zoneDbStrs.length - 1) {
		// sqlstr += s + " ) t group by t.iuin , t.igoodstype ";
		// } else {
		// sqlstr += s + " union all ";
		// }
		// }
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// query score

		String tmp_userone_2 = tmp_tab_pre + "useronedata_2" + UNDERLINE + step;
		// drop_tmp_table(tmp_userone_2);
		// create_tmp_table(tmp_userone_2);
		//
		// basesql = " select t1.iuin,max(t2.score) v from "
		// + tmp_userid_lostuser
		// +
		// " t1 join par_dbname.tab_town_leave t2 on ( t1.iuin = t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate' ) group by t1.iuin ";
		//
		// sqlstr = "insert OVERWRITE TABLE " + tmp_userone_2
		// + " select t.iuin , max(t.v) from ( ";
		// for (int i = 0; i < zoneDbStrs.length; i++) {
		// String zoneStr = zoneDbStrs[i];
		// String s = basesql.replaceAll("par_dbname", zoneStr)
		// .replaceAll("par_partition", login1_par_partition)
		// .replaceAll("par_startdate", login1_startdate)
		// .replaceAll("par_stopdate", final_login1_stopdate);
		// if (i == zoneDbStrs.length - 1) {
		// sqlstr += s + " ) t group by t.iuin ";
		// } else {
		// sqlstr += s + " union all ";
		// }
		// }
		// sqlstr = sqlstr.replaceAll("par_partition", par_partition);
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();
		// final

		try {
			sqlstr = "select userlevel , value2, sum(value1) from ( select t1.iuin, par_userlevel,t1.value1,t1.value2 from "
					+ tmp_usertwo_1
					+ " t1 join "
					+ tmp_userone_2
					+ " t2 on (t1.iuin=t2.iuin) ) t group by userlevel,value2 ";
			sqlstr = "select userlevel , igoodsname, sum(value1) from ( select t1.iuin, par_userlevel,t1.value1,t3.igoodsname from "
					+ tmp_usertwo_1
					+ " t1 join "
					+ tmp_userone_2
					+ " t2 on (t1.iuin=t2.iuin) join gamemeta.m3gcn_item_name t3 on (t1.value2=t3.igoodstype and t3.igoodstype in ('AR32', 'AM84', 'AX73', 'AX70', 'AX71', 'AX72', 'AC39', 'AC58', 'AC33', 'AX75', 'AX77', 'AX74', 'AX76', 'AC01', 'AC80', 'AC06', 'AC08', 'AC14', 'AC27', 'AC28', 'AC41', 'AC45', 'AC82', 'AC97', 'AC70', 'AN12', 'AM82', 'AR32', 'AM84', 'AX73', 'AX70', 'AX71', 'AX72', 'AC39', 'A140', 'A141', 'A142', 'A143', 'A144', 'A145', 'A146', 'A147', 'A148', 'A149', 'A150', 'A151' ) )"
					+ " ) t group by userlevel,igoodsname ";

			basesql = sql_userlevel_common.replaceAll("result_value",
					"t2.value1");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				String v2 = rs.getString(2);
				int v3 = rs.getInt(3);
				resultstr = v1 + TAB + v2 + TAB + v3 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

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

		Class c = Hadoop_Temp.class;
		Method m = c.getMethod(step);
		m.invoke(c);
	}

	public static void get_useraction() throws Exception {

		before_exec();

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String iuinfile = other_args.split(COMMA)[2];
		par_partition = cal_partitionstr(start_date, stop_date);

		// load data
		String tmp_userid_1 = tmp_tab_pre + "userid_1" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_1);
		create_tmp_table(tmp_userid_1);

		sqlstr = "LOAD DATA LOCAL INPATH '" + iuinfile
				+ "' OVERWRITE INTO TABLE " + tmp_userid_1;
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query
		String tmp_useronedata_1 = tmp_tab_pre + "useronedata_1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_useronedata_1);
		create_tmp_table(tmp_useronedata_1);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_useronedata_1
				+ " select t1.iuin,count(t2.iUin) from "
				+ tmp_userid_1
				+ " t1 left outer join caochuan_2.tab_login t2 on t1.iuin = t2.iUin and t2.par_datetime in (par_partition) group by t1.iuin ";
		sqlstr = sqlstr.replaceAll("par_partition", par_partition);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query
		String tmp_usertwodata_1 = tmp_tab_pre + "usertwodata_1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_usertwodata_1);
		create_tmp_table(tmp_usertwodata_1);

		basesql = " select t1.iuin,max(t2.online_time),max(t2.score) from "
				+ tmp_userid_1
				+ " t1 left outer join par_dbname.tab_town_login t2 on t1.iuin = t2.id and t2.par_datetime in (par_partition) group by t1.iuin ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_usertwodata_1
				+ " select * from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		sqlstr = sqlstr.replaceAll("par_partition", par_partition);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query
		String tmp_useronedata_2 = tmp_tab_pre + "useronedata_2" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_useronedata_2);
		create_tmp_table(tmp_useronedata_2);

		basesql = "select t1.iuin,count(t2.iuin) from "
				+ tmp_userid_1
				+ " t1 left outer join par_dbname.tab_item_sell t2 on t1.iuin = t2.iuin and t2.iReason != 1003 and t2.par_datetime in (par_partition) group by t1.iuin ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_useronedata_2
				+ " select * from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		sqlstr = sqlstr.replaceAll("par_partition", par_partition);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// final
		sqlstr = "select t1.iuin,max(t1.value1),max(t2.value1),par_userlevel,max(t3.value1) from "
				+ tmp_useronedata_1
				+ " t1 join "
				+ tmp_usertwodata_1
				+ " t2 on (t1.iuin=t2.iuin) join "
				+ tmp_useronedata_2
				+ " t3 on (t1.iuin=t3.iuin) group by t1.iuin ";
		basesql = sql_userlevel_common.replaceAll("result_value",
				"max(t2.value2)");
		sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				Integer v1 = rs.getInt(1);
				Integer v2 = rs.getInt(2);
				Integer v3 = rs.getInt(3);
				String v4 = rs.getString(4);
				Integer v5 = rs.getInt(5);
				resultosw.write(v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB + v5
						+ NEWLINE);
			}
			resultosw.flush();
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_usercharcreate() throws Exception {

		before_exec();

		String ad_id = other_args.split(COMMA)[0];
		start_date = other_args.split(COMMA)[1];
		stop_date = other_args.split(COMMA)[2];
		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		// load data
		String tmp_userid_1 = tmp_tab_pre + "userid_1" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_1);
		create_tmp_table(tmp_userid_1);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_1
				+ " select iuin from caochuan_2.accountLog where par_datetime in (par_partition) and media_id=par_media_id and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", par_partition)
				.replaceAll("par_media_id", ad_id)
				.replaceAll("par_startdate", start_date)
				.replaceAll("par_stopdate", final_stop_date);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query

		sqlstr = "select count(distinct iuin) from (";
		String sql = " select distinct T1.iuin from "
				+ tmp_userid_1
				+ " T1 join par_tablename T2 on T1.iuin=T2.iuin and T2.par_datetime in (par_partition) union all ";
		for (int i = 1; i < 4; i++) {
			sqlstr += sql.replaceAll("par_tablename",
					"caochuan_2.tab_char_create_" + i);
		}
		for (int i = 6; i < 11; i++) {
			sqlstr += sql.replaceAll("par_tablename",
					"caochuan_2.tab_char_create_" + i);
		}
		sqlstr = sqlstr.substring(0, sqlstr.length() - 10);
		sqlstr += " ) T";
		sqlstr = sqlstr.replaceAll("par_partition", par_partition);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				Integer v1 = rs.getInt(1);
				resultosw.write(v1 + NEWLINE);
			}
			resultosw.flush();
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_usercharcreate1() throws Exception {

		before_exec();

		// String ad_id = other_args.split(COMMA)[0];
		// start_date = other_args.split(COMMA)[1];
		// stop_date = other_args.split(COMMA)[2];
		// String final_stop_date = addDay(stop_date, 1);
		// par_partition = cal_partitionstr(start_date, stop_date);

		// load data
		String tmp_userid_1 = tmp_tab_pre + "userid_1" + UNDERLINE + step;

		// query
		basesql = " select t1.iuin,t2.icreatetime from "
				+ tmp_userid_1
				+ " t1 join par_dbname.tab_char_create t2 on ( t1.iuin = t2.iuin )";

		try {

			for (int i = 0; i < zoneDbStrs.length; i++) {
				String zoneStr = zoneDbStrs[i];
				int zoneId = zoneIds[i];
				sqlstr = basesql.replaceAll("par_dbname", zoneStr);

				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				while (rs.next()) {
					Integer v1 = rs.getInt(1);
					String v2 = rs.getString(2);

					resultosw.write(zoneId + TAB + v1 + TAB + v2 + NEWLINE);
				}
				resultosw.flush();
			}

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_xxx() throws Exception {

		before_exec();

		logstr = step + COMMA + action_date;
		printLogStr(logstr);

		String lostdays = other_args.split(COMMA)[0];

		stop_date = action_date;
		start_date = addDay(stop_date, -180);
		start_time = ZERO_HOURMINSEC;
		stop_time = ZERO_HOURMINSEC;

		start_datetime = start_date + SPACE + start_time;
		stop_datetime = stop_date + SPACE + stop_time;

		par_partition = cal_partitionstr(start_date, stop_date);

		sqlstr = hdsql_usergrouplostcount;
		sqlstr = sqlstr
				.replaceAll("par_userlevel", hdsql_userlevel_common1)
				.replaceAll("par_lostusergrouplevel",
						hdsql_usergrouplostcount_common)
				.replaceAll("par_partition", par_partition)
				.replaceAll("par_startime", start_datetime)
				.replaceAll("par_stoptime", stop_datetime)
				.replaceAll("par_lostdays", lostdays);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		// try {
		//
		// stmt = hiveConn.createStatement();
		// stmt.execute("add jar lib/udf-1.0.jar");
		// stmt.execute("create temporary function rank as 'com.udf.Rank'");
		// rs = stmt.executeQuery(sqlstr);
		//
		// while (rs.next()) {
		// String v1 = rs.getString(1);
		// Integer v2 = rs.getInt(2);
		//
		// resultosw.write(v1 + TAB + v2 + NEWLINE);
		// }
		// resultosw.flush();
		// } catch (SQLException e) {
		// printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		// }

		after_exec();

	}

	public static void get_tryplaycount() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = " select imapid,herotypeid,count(iuin) from par_dbname.tab_map_race where par_datetime in (par_partition) and trialshero=1 and herotypeid in ('UM01', 'UM09', 'UM10', 'UM15', 'UM27', 'UM29', 'UM34', 'UM41', 'UM47', 'UM50', 'UM59', 'UM64', 'UM67') and irecordtime>='par_startdate' and irecordtime<'par_stopdate' group by imapid,herotypeid";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					String v1 = rs.getString(1);
					String v2 = rs.getString(2);
					Integer v3 = rs.getInt(3);

					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		basesql = " select imapid,herotypeid,count(iuin) from par_dbname.tab_map_guanka where par_datetime in (par_partition) and trialshero=1 and  herotypeid in (1431121969,1431121977,1431122224,1431122229,1431122487,1431122489,1431122740,1431122993,1431122999,1431123248,1431123257,1431123508,1431123511) and irecordtime>='par_startdate' and irecordtime<'par_stopdate' group by imapid,herotypeid";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					String v1 = rs.getString(1);
					long t = Long.parseLong(rs.getString(2));
					String v2 = long2str(t);
					Integer v3 = rs.getInt(3);

					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_userinfobyip() throws Exception {

		before_exec();

		String stats_name = other_args.split(COMMA)[0];
		String ipstr = other_args.split(COMMA)[1].replaceAll("\\+", " ");
		Map<String, String> ip_date_map = new HashMap<String, String>();

		String[] ip_arr = ipstr.split(UNDERLINE);

		for (int i = 0; i < ip_arr.length; i++) {
			String[] tmp_arr = ip_arr[i].split("\\|");
			if (tmp_arr.length == 6) {
				ip_date_map.put(tmp_arr[0], ip_arr[i].substring(ip_arr[i]
						.indexOf(VERTICAL_LINE) + 1));
			}
		}
		String par_cond = EMPTY;
		start_timestamp = spacedatetimeformat.format(new Date());

		// query userid
		par_cond = EMPTY;
		Iterator<Entry<String, String>> iter = ip_date_map.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = (Map.Entry<String, String>) iter
					.next();
			String key = entry.getKey();
			String val = entry.getValue();
			String[] tmp_arr = val.split("\\|");
			String par_partition = cal_partitionstr(tmp_arr[0], tmp_arr[1]);
			par_cond += "( ip='par_ip' and par_datetime in (par_partition) and dtlogtime>='par_start_datetime' and dtlogtime<'par_stop_datetime') or "
					.replace("par_ip", key)
					.replace("par_partition", par_partition)
					.replace("par_start_datetime", tmp_arr[0])
					.replace("par_stop_datetime", tmp_arr[1]);
		}
		par_cond = par_cond.substring(0, par_cond.length() - 3);

		String tmp_usertwostr2 = tmp_tab_pre + "usertwostr2" + UNDERLINE + step;
		drop_tmp_table(tmp_usertwostr2);
		create_tmp_table(tmp_usertwostr2);

		sqlstr = "INSERT OVERWRITE TABLE " + tmp_usertwostr2
				+ " select iuin,dtlogtime,ip from caochuan_2.accountlog where "
				+ "  ( par_cond ) order by ip";

		sqlstr = sqlstr.replaceAll("par_cond", par_cond);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		sqlstr = "select t1.iuin, t2.zid , max(t1.value1) dtlogtime , max(t1.value2) ip , max(t2.user_level_e) from  "
				+ tmp_usertwostr2
				+ " t1 left outer join dhcdm.dhcdm_user_game_info t2 "
				+ "  on (t1.iuin = t2.iuin and t2.game_type='m3gcn' and t2.par_dt>='20140801')"
				+ " group by t1.iuin , t2.zid order by ip , t1.iuin ";

		sqlstr = sqlstr.replaceAll("par_cond", par_cond);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			String v1 = rs.getString(1);
			String v2 = rs.getString(2);
			String v3 = rs.getString(3);
			String v4 = rs.getString(4);
			String v5 = rs.getString(5);
			resultosw.write(v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB + v5
					+ NEWLINE);
		}
		resultosw.flush();
		after_exec();
	}

	public static void get_hero_card() throws Exception {

		before_exec();

		logstr = step + COMMA + action_date;
		printLogStr(logstr);

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			sqlstr = "select v , count(id) from "
					+ " ( select id,max(herocardnum) v from par_dbname.tab_town_login where par_datetime in (par_partition) and record_time>='par_startdate' and record_time<'par_stopdate' group by id )"
					+ " t group by v order by v";

			sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					Integer v1 = rs.getInt(1);
					Integer v2 = rs.getInt(2);
					resultosw.write(zoneId + TAB + v1 + TAB + v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_honorvalue() throws Exception {

		before_exec();

		logstr = step + COMMA + action_date;
		printLogStr(logstr);

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);
		basesql = " select sum(param1) from par_dbname.tab_misc where par_datetime in (par_partition) and type=9 and sub_type=1602 and create_time>='par_startdate' and create_time<'par_stopdate' ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					Integer v1 = rs.getInt(1);
					resultosw.write(zoneId + TAB + v1 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_user_fihgt_score() throws Exception {

		before_exec();

		String start_date = other_args.split(COMMA)[0];
		String stop_date = other_args.split(COMMA)[1];
		String final_stop_date = addDay(stop_date, 1);
		String par_partition = cal_partitionstr(start_date, stop_date);

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function rank as 'com.udf.Rank'");

		String iuinfilepath = "/data/tmp/jhh/zhandouli/";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			String tmp_userid_1 = tmp_tab_pre + "userid_1" + UNDERLINE + step;

			drop_tmp_table(tmp_userid_1);
			create_tmp_table(tmp_userid_1);

			String iuinfile = iuinfilepath + zoneId + ".txt";
			sqlstr = "LOAD DATA LOCAL INPATH '" + iuinfile
					+ "' OVERWRITE INTO TABLE " + tmp_userid_1;
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// query 1001 ，1002,1003的记录
			String tmp_userfourstr1_1 = tmp_tab_pre + "userfourstr1_1"
					+ UNDERLINE + step;

			drop_tmp_table(tmp_userfourstr1_1);
			create_tmp_table(tmp_userfourstr1_1);

			basesql = "insert OVERWRITE TABLE "
					+ tmp_userfourstr1_1
					+ " select t2.char_id , t2.sub_type,t2.param2 , t2.param3,t2.create_time from  "
					+ tmp_userid_1
					+ " t1 join par_dbname.tab_misc t2 on ( t1.iuin = t2.char_id and t2.par_datetime in (par_partition) and t2.type=4 and t2.sub_type in (1001,1002,1003) and t2.create_time>='par_start_date' and t2.create_time<'par_stop_date' ) ";
			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_start_date", start_date)
					.replaceAll("par_stop_date", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// 找到最大的 1003 之前的1001 ， 1002 的记录
			String tmp_userfourstr1_2 = tmp_tab_pre + "userfourstr1_2"
					+ UNDERLINE + step;

			drop_tmp_table(tmp_userfourstr1_2);
			create_tmp_table(tmp_userfourstr1_2);
			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_userfourstr1_2
					+ " select t12.iuin , t12.value1 , t12.value2, t12.value3, t12.value4 from ( select iuin , max(value4) value4 from "
					+ tmp_userfourstr1_1
					+ " where value1=1003 group by iuin ) t11 join "
					+ tmp_userfourstr1_1
					+ " t12 on (t11.iuin = t12.iuin and t12.value1 in (1001,1002) ) where t11.value4>t12.value4  ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// 找到最近时间
			String tmp_useronestr1_1 = tmp_tab_pre + "useronestr1_1"
					+ UNDERLINE + step;

			drop_tmp_table(tmp_useronestr1_1);
			create_tmp_table(tmp_useronestr1_1);
			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_useronestr1_1
					+ " select iuin , value4 from ( select iuin , rank(iuin) r ,value4 from  ( select iuin, value4 from "
					+ tmp_userfourstr1_2
					+ " order by iuin , value4 desc ) t11 ) t12 where r = 0  ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// final
			sqlstr = "select t1.iuin , t1.value1,t1.value2,t1.value3,t2.value1 from "
					+ tmp_userfourstr1_2
					+ " t1 join "
					+ tmp_useronestr1_1
					+ " t2 on (t1.iuin=t2.iuin and t1.value4=t2.value1)";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				String v2 = rs.getString(2);
				String v3 = rs.getString(3);
				String v4 = rs.getString(4);
				String v5 = rs.getString(5);
				resultstr = zoneId + TAB + v1 + TAB + v2 + TAB + v3 + TAB + v4
						+ TAB + v5 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

		}

		after_exec();

	}

	public static void get_soldiercostcount() throws Exception {

		before_exec();

		logstr = step + COMMA + action_date;
		printLogStr(logstr);

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);
		basesql = "select v_date,sum(param3) from ( select substr(create_time,1,10) v_date ,param3 from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=1 and ( (sub_type>=1 and sub_type<=3) or (sub_type>=5 and sub_type<=8) or (sub_type=10) or (sub_type>=12 and sub_type<=14) or (sub_type=15 and param2=0) or (sub_type>=16 and sub_type<=19) ) ) t group by v_date order by v_date ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					String v1 = rs.getString(1);
					Long v2 = rs.getLong(2);
					resultosw.write(zoneId + TAB + v1 + TAB + v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_heroplaystats() throws Exception {

		before_exec();

		logstr = step + COMMA + action_date;
		printLogStr(logstr);

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = " select herotypeid , imapid , v_date , count(iuin) from (select iuin, imapid,substr(irecordtime,1,10) as v_date,herotypeid from par_dbname.tab_map_guanka where par_datetime in (par_partition) and irecordtime>='par_startdate' and irecordtime<'par_stopdate' and herotypeid in (1429222965 ,1430534192 ,1431253044 ,1431910450 ,1429221687 ,1430402866 ,1430729529 ,1430861108 ,1430992182 ,1431189559 ,1431516724 ,1431647539 ,1431974712 ) ) t group by herotypeid , imapid , v_date ";

		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function long2str as 'com.udf.Converter_long2str'");
		stmt.execute("create temporary function str2long as 'com.udf.Converter_str2long'");
		basesql = "select heroname,map_name,v_date,count(iuin) from ( select t2.heroname , t3.map_name , substr(t1.irecordtime,1,10) as v_date , t1.iuin from par_dbname.tab_map_guanka t1 join gamemeta.m3gcn_hero_info t2 on ( long2str(t1.herotypeid) = t2.heroid and t2.heroname in ('周泰','张辽') and t1.par_datetime in (par_partition) and t1.irecordtime>='par_startdate' and t1.irecordtime<'par_stopdate' ) join gamemeta.m3gcn_map_info t3 on(t1.imapid=t3.imapid) ) t group by heroname , map_name , v_date ";
		basesql = "select t1.v1 , t2.heroname , t3.map_name , t1.v4  ,count(t1.v5)  from test.tab_tmp1 t1 join gamemeta.m3gcn_hero_info t2 on ( t1.v2 = t2.heroid ) join gamemeta.m3gcn_map_info t3 on (t1.v3=t3.imapid)  group by t1.v1 , t2.heroname , t3.map_name , t1.v4  ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					String v1 = rs.getString(1);
					String v2 = rs.getString(2);
					String v3 = rs.getString(3);
					Integer v4 = rs.getInt(4);
					resultosw.write(zoneId + TAB + v1 + TAB + v2 + TAB + v3
							+ TAB + v4 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_guozhan_honorvalue() throws Exception {

		before_exec();

		logstr = step + COMMA + action_date;
		printLogStr(logstr);

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			// query user id
			String tmp_userid = tmp_tab_pre + "userid" + UNDERLINE + step;
			drop_tmp_table(tmp_userid);
			create_tmp_table(tmp_userid);

			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_userid
					+ " select distinct iuin from par_dbname.tab_map_race where par_datetime in (par_partition) and irecordtime>='par_startdate' and irecordtime<'par_stopdate' and imapid in ('DT11','DT71','DT72','DT73','DT83','DT84')";
			sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// query score
			String tmp_userone_equipscore = tmp_tab_pre
					+ "useronedata_equipscore" + UNDERLINE + step;
			drop_tmp_table(tmp_userone_equipscore);
			create_tmp_table(tmp_userone_equipscore);

			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_userone_equipscore
					+ " select t1.iuin,max(t2.equipscore) v from "
					+ tmp_userid
					+ " t1 left outer join par_dbname.tab_town_leave t2 on ( t1.iuin = t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate') group by t1.iuin ";
			sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// guozhan
			String tmp_userthreestr3_race = tmp_tab_pre + "userthreestr3"
					+ UNDERLINE + step;
			drop_tmp_table(tmp_userthreestr3_race);
			create_tmp_table(tmp_userthreestr3_race);

			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_userthreestr3_race
					+ " select iuin , v_date , imapid , count(1) from ( select t1.iuin , substr(t2.irecordtime,1,10) v_date , t2.imapid from  "
					+ tmp_userid
					+ " t1 join par_dbname.tab_map_race t2 on (t1.iuin=t2.iuin and t2.par_datetime in (par_partition) and t2.irecordtime>='par_startdate' and t2.irecordtime<'par_stopdate' and t2.imapid in ('DT11','DT71','DT72','DT73','DT83','DT84') ) "
					+ " ) t group by iuin , v_date , imapid  ";
			sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// rongyaozhi
			String tmp_usertwostr1_rongyao = tmp_tab_pre + "usertwostr1"
					+ UNDERLINE + step;
			drop_tmp_table(tmp_usertwostr1_rongyao);
			create_tmp_table(tmp_usertwostr1_rongyao);
			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_usertwostr1_rongyao
					+ " select iuin  , sum(param1) , v_date from ( select t1.iuin , substr(t2.create_time,1,10) v_date , t2.param1 from  "
					+ tmp_userid
					+ " t1 join par_dbname.tab_misc t2 on (t1.iuin=t2.char_id and t2.par_datetime in (par_partition) and t2.create_time>='par_startdate' and t2.create_time<'par_stopdate' and t2.type=9 and t2.sub_type in (1601,1602,1603,1605,1606,1607)  ) "
					+ " ) t group by iuin , v_date   ";

			sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			//
			sqlstr = "select t11.iuin , t11.value1, t12.v_date , t12.v_imapid , t12.v_mapcount ,t12.v_honorvalue from "
					+ tmp_userone_equipscore
					+ " t11 left outer join  "
					+ " ( select t1.iuin, t1.value1 v_date,t1.value2 v_imapid,t1.value3 v_mapcount, t2.value1 v_honorvalue from "
					+ tmp_userthreestr3_race
					+ " t1 left outer join "
					+ tmp_usertwostr1_rongyao
					+ " t2  on (t1.iuin=t2.iuin and t1.value1=t2.value2)  ) t12 on (t11.iuin=t12.iuin) order by t12.v_date ";

			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					Integer v1 = rs.getInt(1);
					String v2 = rs.getString(2);
					String v3 = rs.getString(3);
					String v4 = rs.getString(4);
					String v5 = rs.getString(5);
					String v6 = rs.getString(6);
					resultosw.write(zoneId + TAB + v3 + TAB + v1 + TAB + v2
							+ TAB + v4 + TAB + v5 + TAB + v6 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	/*
	 * 该数据取的方式为： 运维调取活跃数据（账号及ID）→美乐数据筛选无身份证账号及调取这些账号充值→运维调取这批账号大区，竞技次数及关卡次数
	 * ------
	 * --------------------------------------------------------------------
	 * ---------------------------------------------------------
	 * 步骤一、为提高工作效率，最终决定运维方面先调取近1个月（2014年4月7日-5月7日）内的活跃用户，
	 * 这批活跃用户必须满足有竞技或关卡次数，并且等级在1-10级（包括10级）
	 */
	public static void get_userinfo3() throws Exception {

		before_exec();

		String action_start_date = other_args.split(COMMA)[0];
		String action_stop_date = other_args.split(COMMA)[1];
		String final_action_stop_date = addDay(action_stop_date, 1);

		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		// query userid

		String tmp_userid = tmp_tab_pre + "userid" + UNDERLINE + step;
		drop_tmp_table(tmp_userid);
		create_tmp_table(tmp_userid);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", action_par_partition)
				.replaceAll("par_startdate", action_start_date)
				.replaceAll("par_stopdate", final_action_stop_date);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query score
		String tmp_userone_score = tmp_tab_pre + "useronedata_score"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userone_score);
		create_tmp_table(tmp_userone_score);

		basesql = " select t1.iuin,t2.score v from "
				+ tmp_userid
				+ " t1 join par_dbname.tab_town_leave t2 on ( t1.iuin = t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate')  ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_userone_score
				+ " select t.iuin , max(t.v) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query userid

		String tmp_userid_level10 = tmp_tab_pre + "userid_level10" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_level10);
		create_tmp_table(tmp_userid_level10);

		sqlstr = "insert OVERWRITE TABLE " + tmp_userid_level10
				+ " select t1.iuin from " + tmp_userid + " t1 join "
				+ tmp_userone_score
				+ " t2 on t1.iuin=t2.iuin and t2.value1<=155499 ";
		sqlstr = sqlstr.replaceAll("par_partition", action_par_partition)
				.replaceAll("par_startdate", action_start_date)
				.replaceAll("par_stopdate", final_action_stop_date);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		try {
			for (int i = 0; i < zoneDbStrs.length; i++) {
				String zoneStr = zoneDbStrs[i];
				int zoneId = zoneIds[i];
				// query 竞技

				String tmp_useronedata_race = tmp_tab_pre + "useronedata_race"
						+ UNDERLINE + step;
				drop_tmp_table(tmp_useronedata_race);
				create_tmp_table(tmp_useronedata_race);

				sqlstr = "insert OVERWRITE TABLE "
						+ tmp_useronedata_race
						+ " select t1.iuin, count(t2.iuin) v from "
						+ tmp_userid_level10
						+ " t1 join par_dbname.tab_map_race t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) group by t1.iuin ";

				sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", action_par_partition)
						.replaceAll("par_startdate", action_start_date)
						.replaceAll("par_stopdate", final_action_stop_date);

				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				execSql();

				// query 关卡

				String tmp_useronedata_guanka = tmp_tab_pre
						+ "useronedata_guanka" + UNDERLINE + step;
				drop_tmp_table(tmp_useronedata_guanka);
				create_tmp_table(tmp_useronedata_guanka);

				sqlstr = "insert OVERWRITE TABLE "
						+ tmp_useronedata_guanka
						+ " select t1.iuin, count(t2.iuin) v from "
						+ tmp_userid_level10
						+ " t1 join par_dbname.tab_map_guanka t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) group by t1.iuin ";
				sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", action_par_partition)
						.replaceAll("par_startdate", action_start_date)
						.replaceAll("par_stopdate", final_action_stop_date);

				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				execSql();

				// final
				sqlstr = "select t1.iuin, t3.value1,t4.value1 from "
						+ tmp_userid_level10 + " t1 left outer join "
						+ tmp_useronedata_race
						+ " t3 on (t1.iuin=t3.iuin) left outer join "
						+ tmp_useronedata_guanka + " t4 on (t1.iuin=t4.iuin) ";

				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					int v3 = rs.getInt(3);
					resultstr = zoneId + TAB + v1 + TAB + v2 + TAB + v3
							+ NEWLINE;
					resultosw.write(resultstr);
				}
				resultosw.flush();
			}
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_userinfo_app() throws Exception {

		before_exec();

		String action_start_date = other_args.split(COMMA)[0];
		String action_stop_date = other_args.split(COMMA)[1];
		String iuinfile = other_args.split(COMMA)[2];

		String final_action_stop_date = addDay(action_stop_date, 1);

		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		// query userid

		String tmp_userid = tmp_tab_pre + "userid" + UNDERLINE + step;
		// drop_tmp_table(tmp_userid);
		// create_tmp_table(tmp_userid);
		//
		// sqlstr = "LOAD DATA LOCAL INPATH '" + iuinfile
		// + "' OVERWRITE INTO TABLE " + tmp_userid;
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// ip

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function rank as 'com.udf.Rank'");

		String tmp_userid_ip = tmp_tab_pre + "useronestr1_ip" + UNDERLINE
				+ step;
		// drop_tmp_table(tmp_userid_ip);
		// create_tmp_table(tmp_userid_ip);
		//
		// sqlstr = "insert OVERWRITE TABLE "
		// + tmp_userid_ip
		// +
		// " select iuin ,vClientIp from ( select iuin ,rank(iuin) r ,dtLogTime,vClientIp from ("
		// + " select t1.iuin ,t2.dtLogTime,t2.vClientIp from "
		// + tmp_userid
		// +
		// " t1 join caochuan_2.tab_login t2 on (t1.iuin=t2.iuin and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' ) order by t1.iuin ,t2.dtLogTime desc "
		// + " ) t11 ) t where r = 0 ";
		// sqlstr = sqlstr.replaceAll("par_partition", action_par_partition)
		// .replaceAll("par_startdate", action_start_date)
		// .replaceAll("par_stopdate", final_action_stop_date);
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		// query online_time and score
		String tmp_usertwostr1 = tmp_tab_pre + "usertwostr1" + UNDERLINE + step;
		// drop_tmp_table(tmp_usertwostr1);
		// create_tmp_table(tmp_usertwostr1);
		//
		// basesql = " select t1.iuin,t2.onlinetime v1 ,t2.score v2 from "
		// + tmp_userid
		// +
		// " t1 join par_dbname.tab_town_leave t2 on ( t1.iuin = t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate' ) ";
		//
		// sqlstr = "insert OVERWRITE TABLE " + tmp_usertwostr1
		// + " select iuin, max(v1) , par_userlevel  from ( ";
		// for (int i = 0; i < zoneDbStrs.length; i++) {
		// String zoneStr = zoneDbStrs[i];
		// String s = basesql.replaceAll("par_dbname", zoneStr)
		// .replaceAll("par_partition", action_par_partition)
		// .replaceAll("par_startdate", action_start_date)
		// .replaceAll("par_stopdate", final_action_stop_date);
		// if (i == zoneDbStrs.length - 1) {
		// sqlstr += s + " ) t group by iuin ";
		// } else {
		// sqlstr += s + " union all ";
		// }
		// }
		// basesql = sql_userlelve_common.replaceAll("result_value", "max(v2)");
		// sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
		//
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);
		//
		// execSql();

		try {
			for (int i = 0; i < zoneDbStrs.length; i++) {
				String zoneStr = zoneDbStrs[i];
				int zoneId = zoneIds[i];
				// query 竞技

				String tmp_useronedata_race = tmp_tab_pre + "useronedata_race"
						+ UNDERLINE + step;
				drop_tmp_table(tmp_useronedata_race);
				create_tmp_table(tmp_useronedata_race);

				sqlstr = "insert OVERWRITE TABLE "
						+ tmp_useronedata_race
						+ " select t1.iuin, count(t2.iuin) v from "
						+ tmp_userid
						+ " t1 join par_dbname.tab_map_race t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) group by t1.iuin ";

				sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", action_par_partition)
						.replaceAll("par_startdate", action_start_date)
						.replaceAll("par_stopdate", final_action_stop_date);

				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				execSql();

				// query 关卡

				String tmp_useronedata_guanka = tmp_tab_pre
						+ "useronedata_guanka" + UNDERLINE + step;
				drop_tmp_table(tmp_useronedata_guanka);
				create_tmp_table(tmp_useronedata_guanka);

				sqlstr = "insert OVERWRITE TABLE "
						+ tmp_useronedata_guanka
						+ " select t1.iuin, count(t2.iuin) v from "
						+ tmp_userid
						+ " t1 join par_dbname.tab_map_guanka t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) group by t1.iuin ";
				sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", action_par_partition)
						.replaceAll("par_startdate", action_start_date)
						.replaceAll("par_stopdate", final_action_stop_date);

				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				execSql();

				// 充值

				String tmp_useronedata_pay = tmp_tab_pre + "useronedata_pay"
						+ UNDERLINE + step;
				drop_tmp_table(tmp_useronedata_pay);
				create_tmp_table(tmp_useronedata_pay);

				sqlstr = "insert OVERWRITE TABLE "
						+ tmp_useronedata_pay
						+ " select t1.iuin,count(t2.iuin) from "
						+ tmp_userid
						+ " t1 join par_dbname.tab_m3g_money t2 on "
						+ " (t1.iuin=t2.iuin and t2.par_datetime in (par_partition) and t2.dtLogTime>='par_startdate' and t2.dtLogTime<'par_stopdate' and t2.iPayType in (4) ) group by t1.iuin ";

				sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", action_par_partition)
						.replaceAll("par_startdate", action_start_date)
						.replaceAll("par_stopdate", final_action_stop_date);

				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				execSql();

				// final
				sqlstr = "select t1.iuin, t2.value1,t3.value1,t3.value2,t4.value1,t5.value1 ,t6.value1 from "
						+ tmp_userid
						+ " t1 left outer join "
						+ tmp_userid_ip
						+ " t2  on (t1.iuin=t2.iuin) left outer join "
						+ tmp_usertwostr1
						+ " t3  on (t1.iuin=t3.iuin) left outer join "
						+ tmp_useronedata_race
						+ " t4  on (t1.iuin=t4.iuin) left outer join "
						+ tmp_useronedata_guanka
						+ " t5  on (t1.iuin=t5.iuin) left outer join "
						+ tmp_useronedata_pay + " t6  on (t1.iuin=t6.iuin) ";

				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					String v2 = rs.getString(2);
					int v3 = rs.getInt(3);
					String v4 = rs.getString(4);
					int v5 = rs.getInt(5);
					int v6 = rs.getInt(6);
					int v7 = rs.getInt(7);
					resultstr = zoneId + TAB + v1 + TAB + v2 + TAB + v3 + TAB
							+ v4 + TAB + v5 + TAB + v6 + TAB + v7 + NEWLINE;
					resultosw.write(resultstr);
				}
				resultosw.flush();
			}
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_vip_chongzhi_top() throws Exception {

		before_exec();

		String action_start_date = other_args.split(COMMA)[0];
		String action_stop_date = other_args.split(COMMA)[1];
		String final_action_stop_date = addDay(action_stop_date, 1);

		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		basesql = " select t1.iuin,max(t2.user_name) user_name ,max(t2.real_name) real_name ,max(t2.vzoneid) vzoneid , max(t1.v) v from "
				+ " ( select iuin, sum(money) v from caochuan_2.tab_charge where par_datetime in (par_partition) and dtLogTime >= 'par_starttime' and dtLogTime < 'par_stoptime' group by iuin ) "
				+ " t1 left outer join caochuan_2.tab_vip t2 on ( t1.iuin =t2.iuin and t2.par_datetime in (par_partition) ) group by t1.iuin order by v ";
		sqlstr = basesql.replaceAll("par_partition", action_par_partition)
				.replaceAll("par_starttime", action_start_date)
				.replaceAll("par_stoptime", final_action_stop_date);

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = hiveConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			String v2 = rs.getString(2);
			String v3 = rs.getString(3);
			String v4 = rs.getString(4);
			String v5 = rs.getString(5);
			resultosw.write(action_date + TAB + v1 + TAB + v2 + TAB + v3 + TAB
					+ v4 + TAB + v5 + NEWLINE);
		}
		resultosw.flush();

		after_exec();

	}

	public static void get_abnormaluserinfo() throws Exception {

		before_exec();

		String action_start_date = other_args.split(COMMA)[0];
		String action_stop_date = other_args.split(COMMA)[1];
		String iuinfile = other_args.split(COMMA)[2];
		String final_action_stop_date = addDay(action_stop_date, 1);

		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		// query userid

		String tmp_useronestr1 = tmp_tab_pre + "useronestr1" + UNDERLINE + step;
		drop_tmp_table(tmp_useronestr1);
		create_tmp_table(tmp_useronestr1);

		sqlstr = "LOAD DATA LOCAL INPATH '" + iuinfile
				+ "' OVERWRITE INTO TABLE " + tmp_useronestr1;
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query tab_char_create
		String tmp_useronedata_charcreate = tmp_tab_pre
				+ "useronedata_charcreate" + UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_charcreate);
		create_tmp_table(tmp_useronedata_charcreate);

		basesql = " select t2.iuin from "
				+ tmp_useronestr1
				+ " t1 join par_dbname.tab_char_create t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.icreatetime>='par_startdate' and t2.icreatetime<'par_stopdate')  ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_useronedata_charcreate
				+ " select t.iuin , count(t.iuin) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login
		String tmp_useronedata_login = tmp_tab_pre + "useronedata_login"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_login);
		create_tmp_table(tmp_useronedata_login);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_useronedata_login
				+ " select t2.iuin,count(t2.iuin) from "
				+ tmp_useronestr1
				+ " t1 join caochuan_2.tab_login t2 on (t1.iuin=t2.iuin) and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' group by t2.iuin ";
		sqlstr = sqlstr.replaceAll("par_partition", action_par_partition)
				.replaceAll("par_startdate", action_start_date)
				.replaceAll("par_stopdate", final_action_stop_date);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query 竞技

		String tmp_useronedata_race = tmp_tab_pre + "useronedata_race"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_race);
		create_tmp_table(tmp_useronedata_race);

		basesql = " select t1.iuin from "
				+ tmp_useronestr1
				+ " t1 join par_dbname.tab_map_race t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_useronedata_race
				+ " select t.iuin , count(t.iuin) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query 关卡

		String tmp_useronedata_guanka = tmp_tab_pre + "useronedata_guanka"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_guanka);
		create_tmp_table(tmp_useronedata_guanka);

		basesql = " select t1.iuin from "
				+ tmp_useronestr1
				+ " t1 join par_dbname.tab_map_guanka t2 on ( t1.iuin = t2.iuin and t2.par_datetime in (par_partition) and t2.iRecordTime>='par_startdate' and t2.iRecordTime<'par_stopdate' ) ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_useronedata_guanka
				+ " select t.iuin , count(t.iuin) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// 充值

		String tmp_useronedata_pay = tmp_tab_pre + "useronedata_pay"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_useronedata_pay);
		create_tmp_table(tmp_useronedata_pay);

		basesql = " select t2.iuin from "
				+ tmp_useronestr1
				+ " t1 join par_dbname.tab_m3g_money t2 on "
				+ " (t1.iuin=t2.iuin and t2.par_datetime in (par_partition) and t2.dtLogTime>='par_startdate' and t2.dtLogTime<'par_stopdate' and t2.iPayType in (4) ) ";
		sqlstr = "insert OVERWRITE TABLE " + tmp_useronedata_pay
				+ " select t.iuin,count(t.iuin) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);

			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query onlinetime and score
		String tmp_userone_onlinetimescore = tmp_tab_pre
				+ "usertwodata_onlinetimescore" + UNDERLINE + step;
		drop_tmp_table(tmp_userone_onlinetimescore);
		create_tmp_table(tmp_userone_onlinetimescore);

		basesql = " select t1.iuin,t2.onlinetime v1 , t2.score v2 from "
				+ tmp_useronestr1
				+ " t1 join par_dbname.tab_town_leave t2 on ( t1.iuin = t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate')  ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_userone_onlinetimescore
				+ " select t.iuin ,sum(t.v1) , max(t.v2) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", action_par_partition)
					.replaceAll("par_startdate", action_start_date)
					.replaceAll("par_stopdate", final_action_stop_date);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query user level

		try {
			sqlstr = "select t1.value1,t1.iuin,t2.value1,t3.value1,t4.value1,t5.value1,t6.value1,t6.value2,par_userlevel from "
					+ tmp_useronestr1
					+ " t1 left outer join "
					+ tmp_useronedata_charcreate
					+ " t2 on (t1.iuin=t2.iuin) left outer join "
					+ tmp_useronedata_login
					+ " t3 on (t1.iuin=t3.iuin) left outer join "
					+ tmp_useronedata_race
					+ " t4 on (t1.iuin=t4.iuin) left outer join "
					+ tmp_useronedata_guanka
					+ " t5 on (t1.iuin=t5.iuin) left outer join "
					+ tmp_userone_onlinetimescore + " t6 on (t1.iuin=t6.iuin) ";
			basesql = sql_userlevel_common.replaceAll("result_value",
					"t6.value2");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);

			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				String v2 = rs.getString(2);
				String v3 = rs.getString(3);
				String v4 = rs.getString(4);
				String v5 = rs.getString(5);
				String v6 = rs.getString(6);
				String v7 = rs.getString(7);
				String v8 = rs.getString(8);
				String v9 = rs.getString(9);
				resultstr = v1 + TAB + v2 + TAB + v3 + TAB + v4 + TAB + v5
						+ TAB + v6 + TAB + v7 + TAB + v8 + TAB + v9 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_userinfo3_userlevel() throws Exception {

		before_exec();

		String action_start_date = other_args.split(COMMA)[0];
		String action_stop_date = other_args.split(COMMA)[1];
		String iuinfile = other_args.split(COMMA)[2];
		String final_action_stop_date = addDay(action_stop_date, 1);

		String action_par_partition = cal_partitionstr(action_start_date,
				action_stop_date);

		// query userid

		String tmp_userid = tmp_tab_pre + "userid" + UNDERLINE + step;
		// drop_tmp_table(tmp_userid);
		// create_tmp_table(tmp_userid);
		//
		// sqlstr = "LOAD DATA LOCAL INPATH '" + iuinfile
		// + "' OVERWRITE INTO TABLE " + tmp_userid;
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);

		// execSql();

		// query score
		String tmp_userone_score = tmp_tab_pre + "useronedata_score"
				+ UNDERLINE + step;
		// drop_tmp_table(tmp_userone_score);
		// create_tmp_table(tmp_userone_score);
		//
		// basesql = " select t1.iuin,t2.score v from "
		// + tmp_userid
		// +
		// " t1 left outer join par_dbname.tab_town_leave t2 on ( t1.iuin = t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate')  ";
		//
		// sqlstr = "insert OVERWRITE TABLE " + tmp_userone_score
		// + " select t.iuin , max(t.v) from ( ";
		// for (int i = 0; i < zoneDbStrs.length; i++) {
		// String zoneStr = zoneDbStrs[i];
		// String s = basesql.replaceAll("par_dbname", zoneStr)
		// .replaceAll("par_partition", action_par_partition)
		// .replaceAll("par_startdate", action_start_date)
		// .replaceAll("par_stopdate", final_action_stop_date);
		// if (i == zoneDbStrs.length - 1) {
		// sqlstr += s + " ) t group by t.iuin ";
		// } else {
		// sqlstr += s + " union all ";
		// }
		// }
		// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		// printLogStr(logstr);

		// execSql();

		// query user level

		try {
			sqlstr = "select iuin, par_userlevel from " + tmp_userone_score;
			basesql = sql_userlevel_common.replaceAll("result_value", "value1");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);

			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				String v2 = rs.getString(2);
				resultstr = v1 + TAB + v2 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}
}
