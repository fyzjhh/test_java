package com.test.dianhun;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class Hadoop_Oss extends Hadoop {

	public static void main(String[] args) throws Exception {

		runOut(args);
		System.out.println("====success====");
	}

	public Hadoop_Oss() {
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

		Class c = Hadoop_Oss.class;
		Method m = c.getMethod(step);
		m.invoke(c);
	}

	public static void get_reguserlostcount() throws Exception {

		before_exec();

		String tmp_action_date = other_args.split(COMMA)[0];
		String reg_startdate = addMonth(tmp_action_date, -2);
		String reg_stopdate = addDay(addMonth(reg_startdate, 1), -1);

		String login_startdate = reg_startdate;
		String login_stopdate = reg_stopdate;

		String login1_startdate = addMonth(login_startdate, 1);
		String login1_stopdate = addDay(addMonth(login1_startdate, 1), -1);

		String final_reg_stopdate = addDay(reg_stopdate, 1);
		String final_login_stopdate = addDay(login_stopdate, 1);
		String final_login1_stopdate = addDay(login1_stopdate, 1);

		String reg_par_partition = cal_partitionstr(reg_startdate, reg_stopdate);
		// query all reg id
		String tmp_userid_1_all = tmp_tab_pre + "userid_1_all" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_1_all);
		create_tmp_table(tmp_userid_1_all);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_1_all
				+ " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query media reg id
		String tmp_userid_1_media = tmp_tab_pre + "userid_1_media" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_1_media);
		create_tmp_table(tmp_userid_1_media);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_1_media
				+ " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and media_id!=0 and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query nature reg id
		String tmp_userid_nature = tmp_tab_pre + "userid_1_nature" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_nature);
		create_tmp_table(tmp_userid_nature);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_nature
				+ " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and media_id=0 and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login id
		String login_par_partition = cal_partitionstr(login_startdate,
				login_stopdate);
		String tmp_userid_2 = tmp_tab_pre + "userid_2" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_2);
		create_tmp_table(tmp_userid_2);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_2
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login_par_partition)
				.replaceAll("par_startdate", login_startdate)
				.replaceAll("par_stopdate", final_login_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query nologin id
		String nologin_par_partition = cal_partitionstr(login1_startdate,
				login1_stopdate);
		String tmp_userid_3 = tmp_tab_pre + "userid_3" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_3);
		create_tmp_table(tmp_userid_3);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_3
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", nologin_par_partition)
				.replaceAll("par_startdate", login1_startdate)
				.replaceAll("par_stopdate", final_login1_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// final 媒体用户流失数量
		sqlstr = " select count(t11.iuin) from ( select t1.iuin from "
				+ tmp_userid_1_media + " t1 join " + tmp_userid_2
				+ " t2 on t1.iuin=t2.iuin ) t11 left outer join "
				+ tmp_userid_3
				+ " t12 on (t11.iuin=t12.iuin) where t12.iuin is null ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				Integer v1 = rs.getInt(1);
				resultosw.write(reg_startdate + TAB + "0" + TAB + v1 + NEWLINE);
			}
			resultosw.flush();
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		// final 自然用户流失数量
		sqlstr = " select count(t11.iuin) from ( select t1.iuin from "
				+ tmp_userid_nature + " t1 join " + tmp_userid_2
				+ " t2 on t1.iuin=t2.iuin ) t11 left outer join "
				+ tmp_userid_3
				+ " t12 on (t11.iuin=t12.iuin) where t12.iuin is null ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				Integer v1 = rs.getInt(1);
				resultosw.write(reg_startdate + TAB + "1" + TAB + v1 + NEWLINE);
			}
			resultosw.flush();
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		// final 本月注册未登陆流失数量
		sqlstr = " select count(t1.iuin) from " + tmp_userid_1_all
				+ " t1 left outer join " + tmp_userid_2
				+ " t2 on t1.iuin=t2.iuin where t2.iuin is null ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				Integer v1 = rs.getInt(1);
				resultosw.write(reg_startdate + TAB + "2" + TAB + v1 + NEWLINE);
			}
			resultosw.flush();
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		// final 非当月注册用户流失数量
		sqlstr = " select count(t11.iuin) from ( select t1.iuin from "
				+ tmp_userid_2
				+ " t1 left outer join "
				+ tmp_userid_1_all
				+ " t2 on t1.iuin=t2.iuin where t2.iuin is null ) t11 left outer join "
				+ tmp_userid_3
				+ " t12 on (t11.iuin=t12.iuin) where t12.iuin is null ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				Integer v1 = rs.getInt(1);
				resultosw.write(reg_startdate + TAB + "3" + TAB + v1 + NEWLINE);
			}
			resultosw.flush();
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_userlostlevel() throws Exception {

		before_exec();

		String step_type = other_args.split(COMMA)[0];
		String tmp_action_date = other_args.split(COMMA)[1];

		String reg_startdate = EMPTY;
		String reg_stopdate = EMPTY;

		String login_startdate = EMPTY;
		String login_stopdate = EMPTY;

		String login1_startdate = EMPTY;
		String login1_stopdate = EMPTY;

		String final_reg_stopdate = EMPTY;
		String final_login_stopdate = EMPTY;
		String final_login1_stopdate = EMPTY;

		if (MONTHLY.equals(step_type)) {
			reg_startdate = addMonth(tmp_action_date, -2);
			reg_stopdate = addDay(addMonth(reg_startdate, 1), -1);

			login_startdate = reg_startdate;
			login_stopdate = reg_stopdate;

			login1_startdate = addMonth(login_startdate, 1);
			login1_stopdate = addDay(addMonth(login1_startdate, 1), -1);

			final_reg_stopdate = addDay(reg_stopdate, 1);
			final_login_stopdate = addDay(login_stopdate, 1);
			final_login1_stopdate = addDay(login1_stopdate, 1);
		}
		if (WEEKLY.equals(step_type)) {

			reg_startdate = addDay(tmp_action_date, 0 - 2 * 7);
			reg_stopdate = addDay(addDay(reg_startdate, 1 * 7), -1);

			login_startdate = reg_startdate;
			login_stopdate = reg_stopdate;

			login1_startdate = addDay(login_startdate, 1 * 7);
			login1_stopdate = addDay(addDay(login1_startdate, 1 * 7), -1);

			final_reg_stopdate = addDay(reg_stopdate, 1);
			final_login_stopdate = addDay(login_stopdate, 1);
			final_login1_stopdate = addDay(login1_stopdate, 1);
		}

		// query all , nature , ad reg id

		String reg_par_partition = cal_partitionstr(reg_startdate, reg_stopdate);

		// get ad_id diff by nature and ad
		String par_ad_id_str = EMPTY;
		sqlstr = "select ad_id from gamemeta.m3gcn_ad_id where media_id in (157,182)";

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = hiveConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			par_ad_id_str += rs.getInt(1) + COMMA;
		}
		par_ad_id_str += "0";

		// query all , nature , ad reg id

		String tmp_userid_reg_all = tmp_tab_pre + "userid_reg_all" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_reg_all);
		create_tmp_table(tmp_userid_reg_all);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_all
				+ " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		String tmp_userid_reg_nature = tmp_tab_pre + "userid_reg_nature"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_reg_nature);
		create_tmp_table(tmp_userid_reg_nature);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_nature
				+ " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' and media_id in (par_ad_id)";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate)
				.replaceAll("par_ad_id", par_ad_id_str);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		String tmp_userid_reg_ad = tmp_tab_pre + "userid_reg_ad" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_reg_ad);
		create_tmp_table(tmp_userid_reg_ad);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_ad
				+ " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' and media_id not in (par_ad_id)";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate)
				.replaceAll("par_ad_id", par_ad_id_str);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login id
		String login_par_partition = cal_partitionstr(login_startdate,
				login_stopdate);
		String tmp_userid_login = tmp_tab_pre + "userid_login" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login);
		create_tmp_table(tmp_userid_login);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate'  ";
		sqlstr = sqlstr.replaceAll("par_partition", login_par_partition)
				.replaceAll("par_startdate", login_startdate)
				.replaceAll("par_stopdate", final_login_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login1 id
		String login1_par_partition = cal_partitionstr(login1_startdate,
				login1_stopdate);
		String tmp_userid_login1 = tmp_tab_pre + "userid_login1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login1);
		create_tmp_table(tmp_userid_login1);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login1
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate'  ";
		sqlstr = sqlstr.replaceAll("par_partition", login1_par_partition)
				.replaceAll("par_startdate", login1_startdate)
				.replaceAll("par_stopdate", final_login1_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// 当月流失
		String tmp_userid_lost = tmp_tab_pre + "userid_lost" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_lost);
		create_tmp_table(tmp_userid_lost);
		sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lost
				+ " select t1.iuin from " + tmp_userid_login
				+ " t1 left outer join " + tmp_userid_login1
				+ " t2 on t1.iuin=t2.iuin where t2.iuin is null ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// 媒体注册用户流失
		String tmp_userid_lost_ad = tmp_tab_pre + "userid_lost_ad" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_lost_ad);
		create_tmp_table(tmp_userid_lost_ad);
		sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lost_ad
				+ " select t1.iuin from " + tmp_userid_lost + " t1 join "
				+ tmp_userid_reg_ad + " t2 on t1.iuin=t2.iuin ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// 自然注册用户流失
		String tmp_userid_lost_nature = tmp_tab_pre + "userid_lost_nature"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_lost_nature);
		create_tmp_table(tmp_userid_lost_nature);
		sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lost_nature
				+ " select t1.iuin from " + tmp_userid_lost + " t1 join "
				+ tmp_userid_reg_nature + " t2 on t1.iuin=t2.iuin ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// 非当月注册用户流失数量
		String tmp_userid_lost_noreg = tmp_tab_pre + "userid_lost_noreg"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_lost_noreg);
		create_tmp_table(tmp_userid_lost_noreg);

		sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lost_noreg
				+ " select t1.iuin from " + tmp_userid_lost
				+ " t1 left outer join " + tmp_userid_reg_all
				+ " t2 on t1.iuin=t2.iuin where t2.iuin is null ";

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query score

		String tmp_userone_score = tmp_tab_pre + "useronedata_score"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userone_score);
		create_tmp_table(tmp_userone_score);

		basesql = " select t1.iuin,max(t2.score) v from "
				+ tmp_userid_lost
				+ " t1 join par_dbname.tab_town_leave t2 on ( t1.iuin = t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate' ) group by t1.iuin  ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_userone_score
				+ " select t.iuin , max(t.v) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", login_par_partition)
					.replaceAll("par_startdate", login_startdate)
					.replaceAll("par_stopdate", final_login_stopdate);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		sqlstr = sqlstr.replaceAll("par_partition", par_partition);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// final

		int type_reg_ad = 1;
		int type_reg_nature = 2;
		int type_noreg = 3;
		try {
			// ad
			sqlstr = "select userlevel , count(iuin) from ( select t1.iuin, par_userlevel from "
					+ tmp_userid_lost_ad
					+ " t1 left outer join "
					+ tmp_userone_score
					+ " t2 on (t1.iuin=t2.iuin) group by t1.iuin ) t group by userlevel ";
			basesql = sql_userlevel_common.replaceAll("result_value",
					"max(t2.value1)");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				int v2 = rs.getInt(2);
				resultstr = action_date + TAB + step_type + TAB + reg_startdate
						+ TAB + type_reg_ad + TAB + v1 + TAB + v2 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

			// nature
			sqlstr = "select userlevel , count(iuin) from ( select t1.iuin, par_userlevel from "
					+ tmp_userid_lost_nature
					+ " t1 left outer join "
					+ tmp_userone_score
					+ " t2 on (t1.iuin=t2.iuin) group by t1.iuin ) t group by userlevel ";
			basesql = sql_userlevel_common.replaceAll("result_value",
					"max(t2.value1)");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				int v2 = rs.getInt(2);
				resultstr = action_date + TAB + step_type + TAB + reg_startdate
						+ TAB + type_reg_nature + TAB + v1 + TAB + v2 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

			// no reg
			sqlstr = "select userlevel , count(iuin) from ( select t1.iuin, par_userlevel from "
					+ tmp_userid_lost_noreg
					+ " t1 left outer join "
					+ tmp_userone_score
					+ " t2 on (t1.iuin=t2.iuin) group by t1.iuin ) t group by userlevel ";
			basesql = sql_userlevel_common.replaceAll("result_value",
					"max(t2.value1)");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				int v2 = rs.getInt(2);
				resultstr = action_date + TAB + step_type + TAB + reg_startdate
						+ TAB + type_noreg + TAB + v1 + TAB + v2 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_zhibo_person_count() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		stmt = hiveConn.createStatement();
		stmt.execute("set hive.groupby.skewindata=false");
		
		basesql = " select count(distinct playerid),count(distinct raceid) from par_dbname.tab_live_record where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' ";
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
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}
		after_exec();

	}

	public static void get_zhibo_top() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = "select mapid,livetype,max(v) from ("
				+ " select raceid, max(mapid) mapid ,max(livetype) livetype, count(distinct playerid) v from par_dbname.tab_live_record where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' and type=1 and livetype in (0,1) group by raceid "
				+ " ) t group by mapid,livetype";
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
					int v2 = rs.getInt(2);
					int v3 = rs.getInt(3);
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

	public static void get_zhibo_map_count() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = "select mapid, count(distinct raceid) v  from par_dbname.tab_live_record where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' group by mapid ";
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
					int v2 = rs.getInt(2);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}
		after_exec();

	}

	public static void get_zhibo_online_person() throws Exception {

		before_exec();

		start_date = addDay(action_date, -1);
		stop_date = addDay(action_date, 1);

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		String str_hours = EMPTY;
		for (int i = 0; i < days_hours.length; i++) {
			str_hours += action_date + SPACE + days_hours[i] + COMMA;
		}
		str_hours = str_hours.substring(0, str_hours.length() - 1);

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			String tmp_usertwostr1 = tmp_tab_pre + "userthreestr3" + UNDERLINE
					+ step;
			drop_tmp_table(tmp_usertwostr1);
			create_tmp_table(tmp_usertwostr1);

			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_usertwostr1
					+ " select playerid ,raceid, start_hour,stop_hour from ( select playerid ,raceid , substr(min(recordtime),1,13) start_hour , substr(max(recordtime),1,13) stop_hour "
					+ " from par_dbname.tab_live_record where par_datetime in (par_partition) and recordtime>='par_start_date' and recordtime<'par_stop_date' "
					+ " group by playerid,raceid ) t where start_hour like 'par_action_date%' or stop_hour like 'par_action_date%' order by playerid ,raceid ";
			sqlstr = sqlstr.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_start_date", start_date)
					.replaceAll("par_stop_date", final_stop_date)
					.replaceAll("par_action_date", action_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			execSql();

			String tmp_strone = tmp_tab_pre + "strone" + UNDERLINE + step;
			drop_tmp_table(tmp_strone);
			create_tmp_table(tmp_strone);
			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_strone
					+ " select explode(split('par_hours',',')) x  from ( select 1 from "
					+ tmp_usertwostr1 + " limit 1 ) t  ";
			sqlstr = sqlstr.replaceAll("par_hours", str_hours);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			execSql();

			String tmp_useronestr1 = tmp_tab_pre + "useronestr1" + UNDERLINE
					+ step;
			drop_tmp_table(tmp_useronestr1);
			create_tmp_table(tmp_useronestr1);

			sqlstr = "insert OVERWRITE TABLE "
					+ tmp_useronestr1
					+ " select t2.iuin , t1.value1 from "
					+ tmp_strone
					+ " t1 join "
					+ tmp_usertwostr1
					+ " t2 where t1.value1>=t2.value2 and t1.value1<=t2.value3 and t1.value1 like 'par_action_date%' order by t2.iuin ";
			sqlstr = sqlstr.replaceAll("par_action_date", action_date);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			execSql();

			sqlstr = " select value1, count(distinct iuin) from "
					+ tmp_useronestr1 + " group by value1 order by value1 ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					String v1 = rs.getString(1);
					int v2 = rs.getInt(2);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}

		}
		after_exec();

	}

	/*
	 * start doc
	 * 
	 * 
	 * 
	 * 
	 * end doc
	 */
	public static void get_xxx() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = "x";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
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

					resultosw.write(zoneStr + TAB + v1 + TAB + v2 + TAB + v3
							+ NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_achievementcompletionrate() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = "select part,acrate,count(id) from ( select id,cast(max(ac01rate)/5 as int) acrate , 0 as part from par_dbname.tab_town_login where par_datetime in (par_partition) and record_time>='par_startdate' and record_time<'par_stopdate' group by id union all  select id,cast(max(ac02rate)/5 as int) acrate , 1 as part from par_dbname.tab_town_login where par_datetime in (par_partition) and record_time>='par_startdate' and record_time<'par_stopdate' group by id union all  select id,cast(max(ac03rate)/5 as int) acrate , 2 as part from par_dbname.tab_town_login where par_datetime in (par_partition) and record_time>='par_startdate' and record_time<'par_stopdate' group by id union all  select id,cast(max(ac04rate)/5 as int) acrate , 3 as part from par_dbname.tab_town_login where par_datetime in (par_partition) and record_time>='par_startdate' and record_time<'par_stopdate' group by id ) t group by part,acrate";

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
					Integer v2 = rs.getInt(2);
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

	public static void get_equipscoredistribution() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = " select equipscore_level,enation,count(id) from (select id,cast(max(equipscore)/1000 as int) equipscore_level ,max(enation) enation from par_dbname.tab_town_login where par_datetime in (par_partition) and record_time>='par_startdate' and record_time<'par_stopdate' group by id ) t group by equipscore_level,enation ";

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
					Integer v2 = rs.getInt(2);
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

	public static void get_match_count() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = "select imapid,count(iuin) from par_dbname.tab_map_race where par_datetime in (par_partition) and irecordtime>='par_startdate' and irecordtime<'par_stopdate' and bautomatchrace=1 group by imapid";

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
					Integer v2 = rs.getInt(2);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		basesql = "select imapid,count(iuin) ,count(distinct raceid) from par_dbname.tab_map_guanka where par_datetime in (par_partition) and irecordtime>='par_startdate' and irecordtime<'par_stopdate' and automatch=1 group by imapid";

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
					Integer v2 = rs.getInt(2);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_match_person_count() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = "select imapid,count(distinct iuin) from par_dbname.tab_map_race where par_datetime in (par_partition) and irecordtime>='par_startdate' and irecordtime<'par_stopdate' and bautomatchrace=1 group by imapid";

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
					Integer v2 = rs.getInt(2);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		basesql = "select imapid,count(distinct iuin) from par_dbname.tab_map_guanka where par_datetime in (par_partition) and irecordtime>='par_startdate' and irecordtime<'par_stopdate' and automatch=1 group by imapid";

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
					Integer v2 = rs.getInt(2);

					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_fight_score_distribution() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = " select fightlevel , count(id) from (select t2.id,case  when max(t2.iFightScoreFun)>-1 and max(t2.iFightScoreFun)<=3000 then 0  when max(t2.iFightScoreFun)>3000 and max(t2.iFightScoreFun)<=5000 then 1  when max(t2.iFightScoreFun)>5000 and max(t2.iFightScoreFun)<=6000 then 2  when max(t2.iFightScoreFun)>6000 and max(t2.iFightScoreFun)<=7000 then 3  when max(t2.iFightScoreFun)>7000 and max(t2.iFightScoreFun)<=8000 then 4  when max(t2.iFightScoreFun)>8000 and max(t2.iFightScoreFun)<=10000 then 5  when max(t2.iFightScoreFun)>10000 and max(t2.iFightScoreFun)<=12500 then 6  when max(t2.iFightScoreFun)>12500 and max(t2.iFightScoreFun)<=16000 then 7  when max(t2.iFightScoreFun)>16000 and max(t2.iFightScoreFun)<=20000 then 8  else 9 end as fightlevel from par_dbname.tab_map_race t1 join par_dbname.tab_town_login t2 on (t1.iuin=t2.id and t1.bautomatchrace=1 and t1.par_datetime in (par_partition) and t1.irecordtime>='par_startdate' and t1.irecordtime<'par_stopdate' and t2.par_datetime in (par_partition) and t2.record_time>='par_startdate' and t2.record_time<'par_stopdate') group by t2.id) t group by fightlevel ";

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
					Integer v2 = rs.getInt(2);

					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_dropeditem() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = " select igoodstype,sum(case when ibeforenum=iafternum then iafternum else (iafternum - ibeforenum) end) from par_dbname.tab_item where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' and ireason=3001 group by igoodstype  ";

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
					Integer v2 = rs.getInt(2);

					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_herofightscore() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = " select HeroTypeID, MapFightScore , count(FightValue), sum(FightValue) from ( select HeroTypeID,cast(MapFightScore/1000 as int) MapFightScore,case when iiswin=1 then FightValue-500 else FightValue-200 end as FightValue from par_dbname.tab_map_race where par_datetime in (par_partition) and irecordtime>='par_startdate' and irecordtime<'par_stopdate' and MapFightScore>0 and imapid in ('DT26','DT35','DT57') ) t group by HeroTypeID, MapFightScore  ";
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
					int v2 = rs.getInt(2);
					int v3 = rs.getInt(3);
					Long v4 = rs.getLong(4);
					resultosw.write(action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + TAB + v4 + NEWLINE);
				}
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}

		after_exec();

	}

	public static void get_userlogincount_bak() throws Exception {

		before_exec();

		String step_type = other_args.split(COMMA)[0];
		String tmp_action_date = other_args.split(COMMA)[1];

		String reg_startdate = EMPTY;
		String reg_stopdate = EMPTY;
		String login1_startdate = EMPTY;
		String login1_stopdate = EMPTY;
		String login2_startdate = EMPTY;
		String login2_stopdate = EMPTY;
		String login3_startdate = EMPTY;
		String login3_stopdate = EMPTY;
		String login4_startdate = EMPTY;
		String login4_stopdate = EMPTY;

		String final_reg_stopdate = EMPTY;
		String final_login1_stopdate = EMPTY;
		String final_login2_stopdate = EMPTY;
		String final_login3_stopdate = EMPTY;
		String final_login4_stopdate = EMPTY;
		if (MONTHLY.equals(step_type)) {

			reg_startdate = addMonth(tmp_action_date, -2);
			reg_stopdate = addDay(addMonth(reg_startdate, 1), -1);
			login1_startdate = reg_startdate;
			login1_stopdate = reg_stopdate;
			login2_startdate = addMonth(login1_startdate, -1);
			login2_stopdate = addDay(login1_startdate, -1);
			login3_startdate = addMonth(login2_startdate, -1);
			login3_stopdate = addDay(login2_startdate, -1);
			login4_startdate = addMonth(login3_startdate, -1);
			login4_stopdate = addDay(login3_startdate, -1);

			final_reg_stopdate = addDay(reg_stopdate, 1);
			final_login1_stopdate = addDay(login1_stopdate, 1);
			final_login2_stopdate = addDay(login2_stopdate, 1);
			final_login3_stopdate = addDay(login3_stopdate, 1);
			final_login4_stopdate = addDay(login4_stopdate, 1);
		}
		if (WEEKLY.equals(step_type)) {

			reg_startdate = addDay(tmp_action_date, 0 - 2 * 7);
			reg_stopdate = addDay(addDay(reg_startdate, 1 * 7), -1);
			login1_startdate = reg_startdate;
			login1_stopdate = reg_stopdate;
			login2_startdate = addDay(login1_startdate, 0 - 1 * 7);
			login2_stopdate = addDay(login1_startdate, -1);
			login3_startdate = addDay(login2_startdate, 0 - 1 * 7);
			login3_stopdate = addDay(login2_startdate, -1);
			login4_startdate = addDay(login3_startdate, 0 - 1 * 7);
			login4_stopdate = addDay(login3_startdate, -1);

			final_reg_stopdate = addDay(reg_stopdate, 1);
			final_login1_stopdate = addDay(login1_stopdate, 1);
			final_login2_stopdate = addDay(login2_stopdate, 1);
			final_login3_stopdate = addDay(login3_stopdate, 1);
			final_login4_stopdate = addDay(login4_stopdate, 1);
		}

		// query all , nature , ad reg id

		String reg_par_partition = cal_partitionstr(reg_startdate, reg_stopdate);

		String tmp_userid_reg_all = tmp_tab_pre + "userid_reg_all" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_reg_all);
		create_tmp_table(tmp_userid_reg_all);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_all
				+ " select t2.iuin from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on "
				+ "(t1.product_id=1297303375 and t1.ad_id=t2.media_id and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' ) ";
		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		String tmp_userid_reg_nature = tmp_tab_pre + "userid_reg_nature"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_reg_nature);
		create_tmp_table(tmp_userid_reg_nature);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_nature
				+ " select t2.iuin from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on "
				+ "(t1.product_id=1297303375 and t1.ad_id=t2.media_id and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' and t2.media_id=0 ) ";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		String tmp_userid_reg_ad = tmp_tab_pre + "userid_reg_ad" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_reg_ad);
		create_tmp_table(tmp_userid_reg_ad);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_ad
				+ " select t2.iuin from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on "
				+ "(t1.product_id=1297303375 and t1.ad_id=t2.media_id and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' and t2.media_id>0 ) ";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login1 id
		String login1_par_partition = cal_partitionstr(login1_startdate,
				login1_stopdate);
		String tmp_userid_login1 = tmp_tab_pre + "userid_login1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login1);
		create_tmp_table(tmp_userid_login1);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login1
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login1_par_partition)
				.replaceAll("par_startdate", login1_startdate)
				.replaceAll("par_stopdate", final_login1_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login2 id
		String login2_par_partition = cal_partitionstr(login2_startdate,
				login2_stopdate);
		String tmp_userid_login2 = tmp_tab_pre + "userid_login2" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login2);
		create_tmp_table(tmp_userid_login2);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login2
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login2_par_partition)
				.replaceAll("par_startdate", login2_startdate)
				.replaceAll("par_stopdate", final_login2_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login3 id
		String login3_par_partition = cal_partitionstr(login3_startdate,
				login3_stopdate);
		String tmp_userid_login3 = tmp_tab_pre + "userid_login3" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login3);
		create_tmp_table(tmp_userid_login3);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login3
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login3_par_partition)
				.replaceAll("par_startdate", login3_startdate)
				.replaceAll("par_stopdate", final_login3_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login4 id
		String login4_par_partition = cal_partitionstr(login4_startdate,
				login4_stopdate);
		String tmp_userid_login4 = tmp_tab_pre + "userid_login4" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login4);
		create_tmp_table(tmp_userid_login4);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login4
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login4_par_partition)
				.replaceAll("par_startdate", login4_startdate)
				.replaceAll("par_stopdate", final_login4_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// //
		int nature_reg_login = 0;
		int ad_reg_login = 0;
		int login_login = 0;
		int reg_prevlogin = 0;
		int noreg_login_nologin = 0;
		int noreg_login_nologin_1 = 0;
		int noreg_login_nologin_2 = 0;
		try {

			// 本月注册登陆数量
			sqlstr = " select count(t1.iuin) from " + tmp_userid_reg_nature
					+ " t1 join " + tmp_userid_login1
					+ " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				nature_reg_login = rs.getInt(1);
			}

			sqlstr = " select count(t1.iuin) from " + tmp_userid_reg_ad
					+ " t1 join " + tmp_userid_login1
					+ " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				ad_reg_login = rs.getInt(1);
			}

			// final 上月登录本月登录数量
			sqlstr = " select count(t1.iuin) from " + tmp_userid_login1
					+ " t1 join " + tmp_userid_login2
					+ " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				login_login = rs.getInt(1);
			}

			// 非本月注册 本月登录 公用用户id
			String tmp_userid_noreg_login = tmp_tab_pre + "userid_noreg_login"
					+ UNDERLINE + step;
			drop_tmp_table(tmp_userid_noreg_login);
			create_tmp_table(tmp_userid_noreg_login);

			sqlstr = "insert OVERWRITE TABLE " + tmp_userid_noreg_login
					+ " select t1.iuin from " + tmp_userid_login1
					+ " t1 left outer join " + tmp_userid_reg_all
					+ " t2 on t1.iuin=t2.iuin where t2.iuin is null  ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// 非本月注册 本月登录 上月未登录 公用用户id
			String tmp_userid_noreg_login_nologin = tmp_tab_pre
					+ "userid_noreg_login_nologin" + UNDERLINE + step;
			drop_tmp_table(tmp_userid_noreg_login_nologin);
			create_tmp_table(tmp_userid_noreg_login_nologin);

			sqlstr = "insert OVERWRITE TABLE " + tmp_userid_noreg_login_nologin
					+ " select t1.iuin from " + tmp_userid_noreg_login
					+ " t1 left outer join " + tmp_userid_login2
					+ " t2 on t1.iuin=t2.iuin where t2.iuin is null  ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// String tmp_userid_noreg_login_nologin = tmp_tab_pre
			// + "userid_noreg_login_nologin" + UNDERLINE + step;
			// drop_tmp_table(tmp_userid_noreg_login_nologin);
			// create_tmp_table(tmp_userid_noreg_login_nologin);
			//
			// sqlstr = "insert OVERWRITE TABLE " +
			// tmp_userid_noreg_login_nologin
			// + " select t1.iuin from " + tmp_userid_login1
			// + " t1 left outer join ( select iuin from "
			// + tmp_userid_reg_all + " union all select iuin from "
			// + tmp_userid_login2
			// + " ) t2 on t1.iuin=t2.iuin where t2.iuin is null  ";
			// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			// printLogStr(logstr);
			//
			// execSql();

			// 非本月注册 本月登录 上月未登录 用户数
			sqlstr = " select count(iuin) from "
					+ tmp_userid_noreg_login_nologin;
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				noreg_login_nologin = rs.getInt(1);
			}

			// 本月注册又上月登录 通常为0
			sqlstr = " select count(t1.iuin) from " + tmp_userid_reg_all
					+ " t1 join " + tmp_userid_login2
					+ " t2 on t1.iuin=t2.iuin  ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				reg_prevlogin = rs.getInt(1);
			}

			// 非本月注册 上月未登录 本月登录 隔1个月
			sqlstr = " select count(t1.iuin) from "
					+ tmp_userid_noreg_login_nologin + " t1 join "
					+ tmp_userid_login3 + " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				noreg_login_nologin_1 = rs.getInt(1);
			}
			// 非本月注册 上月未登录 本月登录 隔2个月
			String tmp_userid_yes2 = tmp_tab_pre + "userid_yes2" + UNDERLINE
					+ step;
			drop_tmp_table(tmp_userid_yes2);
			create_tmp_table(tmp_userid_yes2);

			sqlstr = "insert OVERWRITE TABLE " + tmp_userid_yes2
					+ " select t1.iuin from " + tmp_userid_noreg_login_nologin
					+ " t1 join " + tmp_userid_login4
					+ " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			sqlstr = " select count(t1.iuin) from " + tmp_userid_yes2
					+ " t1 left outer join " + tmp_userid_login3
					+ " t2 on t1.iuin=t2.iuin where t2.iuin is null ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				noreg_login_nologin_2 = rs.getInt(1);
			}

			// write to file
			resultstr = action_date + TAB + step_type + TAB + reg_startdate
					+ TAB + nature_reg_login + TAB + ad_reg_login + TAB
					+ login_login + TAB + noreg_login_nologin + TAB
					+ reg_prevlogin + TAB + noreg_login_nologin_1 + TAB
					+ noreg_login_nologin_2 + NEWLINE;
			resultosw.write(resultstr);
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_corptaskpersoncount() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			try {

				int normal_count = 0;
				int xuanshang_count = 0;
				int justrace_xuanshang = 0;
				int justguanka_xuanshang = 0;
				int raceandguanka_xuanshang = 0;

				//
				basesql = " select count(distinct dwplayerid) from par_dbname.tab_corp_task where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' and etasktype=3";
				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				if (rs.next()) {
					normal_count = rs.getInt(1);
				}
				//
				basesql = " select count(distinct dwplayerid) from par_dbname.tab_corp_task where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' and etasktype=12";
				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				if (rs.next()) {
					xuanshang_count = rs.getInt(1);
				}
				//
				String tmp_userid_race = tmp_tab_pre + "userid_1" + UNDERLINE
						+ step;
				drop_tmp_table(tmp_userid_race);
				create_tmp_table(tmp_userid_race);

				basesql = "insert OVERWRITE TABLE "
						+ tmp_userid_race
						+ " select distinct dwplayerid from par_dbname.tab_corp_task where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' and etasktype=12 and dwTaskID=1093677105 ";
				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				execSql();

				//
				String tmp_userid_guanka = tmp_tab_pre + "userid_2" + UNDERLINE
						+ step;
				drop_tmp_table(tmp_userid_guanka);
				create_tmp_table(tmp_userid_guanka);

				basesql = "insert OVERWRITE TABLE "
						+ tmp_userid_guanka
						+ " select distinct dwplayerid from par_dbname.tab_corp_task where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' and etasktype=12 and dwTaskID!=1093677105 ";
				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				execSql();
				//
				sqlstr = " select count(T1.iuin) from " + tmp_userid_race
						+ " T1 left outer join " + tmp_userid_guanka
						+ " T2 on T1.iuin=T2.iuin where T2.iuin is null ";
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				if (rs.next()) {
					justrace_xuanshang = rs.getInt(1);
				}

				//
				sqlstr = " select count(T1.iuin) from " + tmp_userid_guanka
						+ " T1 left outer join " + tmp_userid_race
						+ " T2 on T1.iuin=T2.iuin where T2.iuin is null ";
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				if (rs.next()) {
					justguanka_xuanshang = rs.getInt(1);
				}

				//
				raceandguanka_xuanshang = xuanshang_count - justrace_xuanshang
						- justguanka_xuanshang;

				//
				resultstr = action_date + TAB + zoneId + TAB + normal_count
						+ TAB + xuanshang_count + TAB + justrace_xuanshang
						+ TAB + justguanka_xuanshang + TAB
						+ raceandguanka_xuanshang + NEWLINE;
				resultosw.write(resultstr);

			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}

			resultosw.flush();
		}

		after_exec();

	}

	public static void get_corp_level_distribution() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		//
		basesql = "select nationtype, viplevel , count(corpid) from ("
				+ " select corpid,nationtype, max(viplevel) viplevel from par_dbname.tab_corp_info where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' group by corpid,nationtype"
				+ " ) t group by nationtype, viplevel ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			try {

				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					int v3 = rs.getInt(3);
					resultstr = action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE;
					resultosw.write(resultstr);
				}

			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}

			resultosw.flush();
		}

		after_exec();

	}

	public static void get_corp_person_distribution() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		//
		basesql = "select t1.nationtype , t2.person_count , count(t1.corpid) from ("
				+ " select corpid,max(nationtype) nationtype from par_dbname.tab_corp_info where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' group by corpid "
				+ " ) t1 join ( "
				+ " select corpid,cast(count(playerid)/50 as int) person_count from par_dbname.tab_corp_player where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' and latestonlinetime>='par_startdate' and latestonlinetime<'par_stopdate' group by corpid"
				+ " ) t2 on t1.corpid=t2.corpid group by t1.nationtype , t2.person_count ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			try {

				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					int v3 = rs.getInt(3);
					resultstr = action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE;
					resultosw.write(resultstr);
				}

			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}

			resultosw.flush();
		}

		after_exec();

	}

	public static void get_corp_equip_score_distribution() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = "select t1.nationtype , t2.equipscore , count(t1.corpid) from ("
				+ " select corpid,max(nationtype) nationtype from par_dbname.tab_corp_info where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' group by corpid "
				+ " ) t1 join ( select corpid, cast(sum(equipscore)/500 as int) equipscore from ( "
				+ " select t12.playerid,max(t11.corpid) corpid ,max(t12.equipscore) equipscore from par_dbname.tab_corp_player t11 join par_dbname.tab_town_leave t12 on "
				+ " ( t11.playerid=t12.playerid and t11.par_datetime in (par_partition) and t11.dtlogtime>='par_startdate' and t11.dtlogtime<'par_stopdate' and t12.par_datetime in (par_partition) and t12.recordtime>='par_startdate' and t12.recordtime<'par_stopdate' and t11.latestonlinetime>='par_startdate' and t11.latestonlinetime<'par_stopdate' ) group by t12.playerid "
				+ " ) t111 group by corpid ) t2 on t1.corpid=t2.corpid group by t1.nationtype , t2.equipscore ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			try {

				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					int v3 = rs.getInt(3);
					resultstr = action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE;
					resultosw.write(resultstr);
				}

			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}

			resultosw.flush();
		}

		after_exec();

	}

	public static void get_corp_fight_score_distribution() throws Exception {

		before_exec();

		start_date = action_date;
		stop_date = action_date;

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		basesql = "select t1.nationtype , t2.fightscorefun , count(t1.corpid) from ("
				+ " select corpid,max(nationtype) nationtype from par_dbname.tab_corp_info where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' group by corpid "
				+ " ) t1 join ( select corpid, cast(sum(fightscorefun)/500 as int) fightscorefun from ( "
				+ " select t12.playerid,max(t11.corpid) corpid ,max(t12.fightscorefun) fightscorefun from par_dbname.tab_corp_player t11 join par_dbname.tab_town_leave t12 on "
				+ " ( t11.playerid=t12.playerid and t11.par_datetime in (par_partition) and t11.dtlogtime>='par_startdate' and t11.dtlogtime<'par_stopdate' and t12.par_datetime in (par_partition) and t12.recordtime>='par_startdate' and t12.recordtime<'par_stopdate' and t11.latestonlinetime>='par_startdate' and t11.latestonlinetime<'par_stopdate' ) group by t12.playerid "
				+ " ) t111 group by corpid ) t2 on t1.corpid=t2.corpid group by t1.nationtype , t2.fightscorefun ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			try {

				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					int v1 = rs.getInt(1);
					int v2 = rs.getInt(2);
					int v3 = rs.getInt(3);
					resultstr = action_date + TAB + zoneId + TAB + v1 + TAB
							+ v2 + TAB + v3 + NEWLINE;
					resultosw.write(resultstr);
				}

			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}

			resultosw.flush();
		}

		after_exec();

	}

	public static void get_userlogincount() throws Exception {

		before_exec();

		String step_type = other_args.split(COMMA)[0];
		String tmp_action_date = other_args.split(COMMA)[1];

		String reg_startdate = EMPTY;
		String reg_stopdate = EMPTY;
		String login1_startdate = EMPTY;
		String login1_stopdate = EMPTY;
		String login2_startdate = EMPTY;
		String login2_stopdate = EMPTY;
		String login3_startdate = EMPTY;
		String login3_stopdate = EMPTY;
		String login4_startdate = EMPTY;
		String login4_stopdate = EMPTY;

		String final_reg_stopdate = EMPTY;
		String final_login1_stopdate = EMPTY;
		String final_login2_stopdate = EMPTY;
		String final_login3_stopdate = EMPTY;
		String final_login4_stopdate = EMPTY;
		if (MONTHLY.equals(step_type)) {
			// 为了提前跑上个月的登录概况，不需要向前倒推2个月

//			reg_startdate = addMonth(tmp_action_date, -2);
			reg_startdate = addMonth(tmp_action_date, -1);
			reg_stopdate = addDay(addMonth(reg_startdate, 1), -1);
			login1_startdate = reg_startdate;
			login1_stopdate = reg_stopdate;
			login2_startdate = addMonth(login1_startdate, -1);
			login2_stopdate = addDay(login1_startdate, -1);
			login3_startdate = addMonth(login2_startdate, -1);
			login3_stopdate = addDay(login2_startdate, -1);
			login4_startdate = addMonth(login3_startdate, -1);
			login4_stopdate = addDay(login3_startdate, -1);

			final_reg_stopdate = addDay(reg_stopdate, 1);
			final_login1_stopdate = addDay(login1_stopdate, 1);
			final_login2_stopdate = addDay(login2_stopdate, 1);
			final_login3_stopdate = addDay(login3_stopdate, 1);
			final_login4_stopdate = addDay(login4_stopdate, 1);
		}
		if (WEEKLY.equals(step_type)) {

			reg_startdate = addDay(tmp_action_date, 0 - 2 * 7);
			reg_stopdate = addDay(addDay(reg_startdate, 1 * 7), -1);
			login1_startdate = reg_startdate;
			login1_stopdate = reg_stopdate;
			login2_startdate = addDay(login1_startdate, 0 - 1 * 7);
			login2_stopdate = addDay(login1_startdate, -1);
			login3_startdate = addDay(login2_startdate, 0 - 1 * 7);
			login3_stopdate = addDay(login2_startdate, -1);
			login4_startdate = addDay(login3_startdate, 0 - 1 * 7);
			login4_stopdate = addDay(login3_startdate, -1);

			final_reg_stopdate = addDay(reg_stopdate, 1);
			final_login1_stopdate = addDay(login1_stopdate, 1);
			final_login2_stopdate = addDay(login2_stopdate, 1);
			final_login3_stopdate = addDay(login3_stopdate, 1);
			final_login4_stopdate = addDay(login4_stopdate, 1);
		}

		// get ad_id diff by nature and ad
		String par_ad_id_str = EMPTY;
		sqlstr = "select ad_id from gamemeta.m3gcn_ad_id where media_id in (157,182)";

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = hiveConn.createStatement();
		rs = stmt.executeQuery(sqlstr);
		while (rs.next()) {
			par_ad_id_str += rs.getInt(1) + COMMA;
		}
		par_ad_id_str += "0";

		// query all , nature , ad reg id

		String reg_par_partition = cal_partitionstr(reg_startdate, reg_stopdate);

		String tmp_userid_reg_all = tmp_tab_pre + "userid_reg_all" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_reg_all);
		create_tmp_table(tmp_userid_reg_all);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_all
				+ " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		String tmp_userid_reg_nature = tmp_tab_pre + "userid_reg_nature"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_reg_nature);
		create_tmp_table(tmp_userid_reg_nature);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_nature
				+ " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' and media_id in (par_ad_id)";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate)
				.replaceAll("par_ad_id", par_ad_id_str);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		String tmp_userid_reg_ad = tmp_tab_pre + "userid_reg_ad" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_reg_ad);
		create_tmp_table(tmp_userid_reg_ad);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_ad
				+ " select iuin from caochuan_2.accountlog where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' and media_id not in (par_ad_id)";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate)
				.replaceAll("par_ad_id", par_ad_id_str);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login1 id
		String login1_par_partition = cal_partitionstr(login1_startdate,
				login1_stopdate);
		String tmp_userid_login1 = tmp_tab_pre + "userid_login1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login1);
		create_tmp_table(tmp_userid_login1);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login1
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login1_par_partition)
				.replaceAll("par_startdate", login1_startdate)
				.replaceAll("par_stopdate", final_login1_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login2 id
		String login2_par_partition = cal_partitionstr(login2_startdate,
				login2_stopdate);
		String tmp_userid_login2 = tmp_tab_pre + "userid_login2" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login2);
		create_tmp_table(tmp_userid_login2);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login2
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login2_par_partition)
				.replaceAll("par_startdate", login2_startdate)
				.replaceAll("par_stopdate", final_login2_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login3 id
		String login3_par_partition = cal_partitionstr(login3_startdate,
				login3_stopdate);
		String tmp_userid_login3 = tmp_tab_pre + "userid_login3" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login3);
		create_tmp_table(tmp_userid_login3);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login3
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login3_par_partition)
				.replaceAll("par_startdate", login3_startdate)
				.replaceAll("par_stopdate", final_login3_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login4 id
		String login4_par_partition = cal_partitionstr(login4_startdate,
				login4_stopdate);
		String tmp_userid_login4 = tmp_tab_pre + "userid_login4" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login4);
		create_tmp_table(tmp_userid_login4);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login4
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate' ";
		sqlstr = sqlstr.replaceAll("par_partition", login4_par_partition)
				.replaceAll("par_startdate", login4_startdate)
				.replaceAll("par_stopdate", final_login4_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// //
		int nature_reg_login = 0;
		int ad_reg_login = 0;
		int login_login = 0;
		int reg_prevlogin = 0;
		int noreg_login_nologin = 0;
		int noreg_login_nologin_1 = 0;
		int noreg_login_nologin_2 = 0;
		try {

			// 本月注册登陆数量
			sqlstr = " select count(t1.iuin) from " + tmp_userid_reg_nature
					+ " t1 join " + tmp_userid_login1
					+ " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				nature_reg_login = rs.getInt(1);
			}

			sqlstr = " select count(t1.iuin) from " + tmp_userid_reg_ad
					+ " t1 join " + tmp_userid_login1
					+ " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				ad_reg_login = rs.getInt(1);
			}

			// final 上月登录本月登录数量
			sqlstr = " select count(t1.iuin) from " + tmp_userid_login1
					+ " t1 join " + tmp_userid_login2
					+ " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				login_login = rs.getInt(1);
			}

			// 非本月注册 本月登录 公用用户id
			String tmp_userid_noreg_login = tmp_tab_pre + "userid_noreg_login"
					+ UNDERLINE + step;
			drop_tmp_table(tmp_userid_noreg_login);
			create_tmp_table(tmp_userid_noreg_login);

			sqlstr = "insert OVERWRITE TABLE " + tmp_userid_noreg_login
					+ " select t1.iuin from " + tmp_userid_login1
					+ " t1 left outer join " + tmp_userid_reg_all
					+ " t2 on t1.iuin=t2.iuin where t2.iuin is null  ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// 非本月注册 本月登录 上月未登录 公用用户id
			String tmp_userid_noreg_login_nologin = tmp_tab_pre
					+ "userid_noreg_login_nologin" + UNDERLINE + step;
			drop_tmp_table(tmp_userid_noreg_login_nologin);
			create_tmp_table(tmp_userid_noreg_login_nologin);

			sqlstr = "insert OVERWRITE TABLE " + tmp_userid_noreg_login_nologin
					+ " select t1.iuin from " + tmp_userid_noreg_login
					+ " t1 left outer join " + tmp_userid_login2
					+ " t2 on t1.iuin=t2.iuin where t2.iuin is null  ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			// String tmp_userid_noreg_login_nologin = tmp_tab_pre
			// + "userid_noreg_login_nologin" + UNDERLINE + step;
			// drop_tmp_table(tmp_userid_noreg_login_nologin);
			// create_tmp_table(tmp_userid_noreg_login_nologin);
			//
			// sqlstr = "insert OVERWRITE TABLE " +
			// tmp_userid_noreg_login_nologin
			// + " select t1.iuin from " + tmp_userid_login1
			// + " t1 left outer join ( select iuin from "
			// + tmp_userid_reg_all + " union all select iuin from "
			// + tmp_userid_login2
			// + " ) t2 on t1.iuin=t2.iuin where t2.iuin is null  ";
			// logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			// printLogStr(logstr);
			//
			// execSql();

			// 非本月注册 本月登录 上月未登录 用户数
			sqlstr = " select count(iuin) from "
					+ tmp_userid_noreg_login_nologin;
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				noreg_login_nologin = rs.getInt(1);
			}

			// 本月注册又上月登录 通常为0
			sqlstr = " select count(t1.iuin) from " + tmp_userid_reg_all
					+ " t1 join " + tmp_userid_login2
					+ " t2 on t1.iuin=t2.iuin  ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				reg_prevlogin = rs.getInt(1);
			}

			// 非本月注册 上月未登录 本月登录 隔1个月
			sqlstr = " select count(t1.iuin) from "
					+ tmp_userid_noreg_login_nologin + " t1 join "
					+ tmp_userid_login3 + " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				noreg_login_nologin_1 = rs.getInt(1);
			}
			// 非本月注册 上月未登录 本月登录 隔2个月
			String tmp_userid_yes2 = tmp_tab_pre + "userid_yes2" + UNDERLINE
					+ step;
			drop_tmp_table(tmp_userid_yes2);
			create_tmp_table(tmp_userid_yes2);

			sqlstr = "insert OVERWRITE TABLE " + tmp_userid_yes2
					+ " select t1.iuin from " + tmp_userid_noreg_login_nologin
					+ " t1 join " + tmp_userid_login4
					+ " t2 on t1.iuin=t2.iuin ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			execSql();

			sqlstr = " select count(t1.iuin) from " + tmp_userid_yes2
					+ " t1 left outer join " + tmp_userid_login3
					+ " t2 on t1.iuin=t2.iuin where t2.iuin is null ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			if (rs.next()) {
				noreg_login_nologin_2 = rs.getInt(1);
			}

			// write to file
			resultstr = action_date + TAB + step_type + TAB + reg_startdate
					+ TAB + nature_reg_login + TAB + ad_reg_login + TAB
					+ login_login + TAB + noreg_login_nologin + TAB
					+ reg_prevlogin + TAB + noreg_login_nologin_1 + TAB
					+ noreg_login_nologin_2 + NEWLINE;
			resultosw.write(resultstr);
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}

	public static void get_userlostlevel_bak() throws Exception {

		before_exec();

		String step_type = other_args.split(COMMA)[0];
		String tmp_action_date = other_args.split(COMMA)[1];

		String reg_startdate = EMPTY;
		String reg_stopdate = EMPTY;

		String login_startdate = EMPTY;
		String login_stopdate = EMPTY;

		String login1_startdate = EMPTY;
		String login1_stopdate = EMPTY;

		String final_reg_stopdate = EMPTY;
		String final_login_stopdate = EMPTY;
		String final_login1_stopdate = EMPTY;

		if (MONTHLY.equals(step_type)) {
			reg_startdate = addMonth(tmp_action_date, -2);
			reg_stopdate = addDay(addMonth(reg_startdate, 1), -1);

			login_startdate = reg_startdate;
			login_stopdate = reg_stopdate;

			login1_startdate = addMonth(login_startdate, 1);
			login1_stopdate = addDay(addMonth(login1_startdate, 1), -1);

			final_reg_stopdate = addDay(reg_stopdate, 1);
			final_login_stopdate = addDay(login_stopdate, 1);
			final_login1_stopdate = addDay(login1_stopdate, 1);
		}
		if (WEEKLY.equals(step_type)) {

			reg_startdate = addDay(tmp_action_date, 0 - 2 * 7);
			reg_stopdate = addDay(addDay(reg_startdate, 1 * 7), -1);

			login_startdate = reg_startdate;
			login_stopdate = reg_stopdate;

			login1_startdate = addDay(login_startdate, 1 * 7);
			login1_stopdate = addDay(addDay(login1_startdate, 1 * 7), -1);

			final_reg_stopdate = addDay(reg_stopdate, 1);
			final_login_stopdate = addDay(login_stopdate, 1);
			final_login1_stopdate = addDay(login1_stopdate, 1);
		}

		// query all , nature , ad reg id

		String reg_par_partition = cal_partitionstr(reg_startdate, reg_stopdate);

		String tmp_userid_reg_all = tmp_tab_pre + "userid_reg_all" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_reg_all);
		create_tmp_table(tmp_userid_reg_all);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_all
				+ " select t2.iuin from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on "
				+ "(t1.product_id=1297303375 and t1.ad_id=t2.media_id and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' ) ";
		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		String tmp_userid_reg_nature = tmp_tab_pre + "userid_reg_nature"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_reg_nature);
		create_tmp_table(tmp_userid_reg_nature);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_nature
				+ " select t2.iuin from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on "
				+ "(t1.product_id=1297303375 and t1.ad_id=t2.media_id and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' and t2.media_id=0 ) ";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		String tmp_userid_reg_ad = tmp_tab_pre + "userid_reg_ad" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_reg_ad);
		create_tmp_table(tmp_userid_reg_ad);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_reg_ad
				+ " select t2.iuin from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on "
				+ "(t1.product_id=1297303375 and t1.ad_id=t2.media_id and t2.par_datetime in (par_partition) and t2.dtlogtime>='par_startdate' and t2.dtlogtime<'par_stopdate' and t2.media_id>0 ) ";

		sqlstr = sqlstr.replaceAll("par_partition", reg_par_partition)
				.replaceAll("par_startdate", reg_startdate)
				.replaceAll("par_stopdate", final_reg_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login id
		String login_par_partition = cal_partitionstr(login_startdate,
				login_stopdate);
		String tmp_userid_login = tmp_tab_pre + "userid_login" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login);
		create_tmp_table(tmp_userid_login);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate'  ";
		sqlstr = sqlstr.replaceAll("par_partition", login_par_partition)
				.replaceAll("par_startdate", login_startdate)
				.replaceAll("par_stopdate", final_login_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query login1 id
		String login1_par_partition = cal_partitionstr(login1_startdate,
				login1_stopdate);
		String tmp_userid_login1 = tmp_tab_pre + "userid_login1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_login1);
		create_tmp_table(tmp_userid_login1);

		sqlstr = "insert OVERWRITE TABLE "
				+ tmp_userid_login1
				+ " select distinct iuin from caochuan_2.tab_login where par_datetime in (par_partition) and dtlogtime>='par_startdate' and dtlogtime<'par_stopdate'  ";
		sqlstr = sqlstr.replaceAll("par_partition", login1_par_partition)
				.replaceAll("par_startdate", login1_startdate)
				.replaceAll("par_stopdate", final_login1_stopdate);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// 当月流失
		String tmp_userid_lost = tmp_tab_pre + "userid_lost" + UNDERLINE + step;
		drop_tmp_table(tmp_userid_lost);
		create_tmp_table(tmp_userid_lost);
		sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lost
				+ " select t1.iuin from " + tmp_userid_login
				+ " t1 left outer join " + tmp_userid_login1
				+ " t2 on t1.iuin=t2.iuin where t2.iuin is null ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// 媒体注册用户流失
		String tmp_userid_lost_ad = tmp_tab_pre + "userid_lost_ad" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userid_lost_ad);
		create_tmp_table(tmp_userid_lost_ad);
		sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lost_ad
				+ " select t1.iuin from " + tmp_userid_lost + " t1 join "
				+ tmp_userid_reg_ad + " t2 on t1.iuin=t2.iuin ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// 自然注册用户流失
		String tmp_userid_lost_nature = tmp_tab_pre + "userid_lost_nature"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_lost_nature);
		create_tmp_table(tmp_userid_lost_nature);
		sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lost_nature
				+ " select t1.iuin from " + tmp_userid_lost + " t1 join "
				+ tmp_userid_reg_nature + " t2 on t1.iuin=t2.iuin ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// 非当月注册用户流失数量
		String tmp_userid_lost_noreg = tmp_tab_pre + "userid_lost_noreg"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userid_lost_noreg);
		create_tmp_table(tmp_userid_lost_noreg);

		sqlstr = "insert OVERWRITE TABLE " + tmp_userid_lost_noreg
				+ " select t1.iuin from " + tmp_userid_lost
				+ " t1 left outer join " + tmp_userid_reg_all
				+ " t2 on t1.iuin=t2.iuin where t2.iuin is null ";

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// query score

		String tmp_userone_score = tmp_tab_pre + "useronedata_score"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_userone_score);
		create_tmp_table(tmp_userone_score);

		basesql = " select t1.iuin,max(t2.score) v from "
				+ tmp_userid_lost
				+ " t1 join par_dbname.tab_town_leave t2 on ( t1.iuin = t2.playerid and t2.par_datetime in (par_partition) and t2.recordtime>='par_startdate' and t2.recordtime<'par_stopdate' ) group by t1.iuin  ";

		sqlstr = "insert OVERWRITE TABLE " + tmp_userone_score
				+ " select t.iuin , max(t.v) from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", login_par_partition)
					.replaceAll("par_startdate", login_startdate)
					.replaceAll("par_stopdate", final_login_stopdate);
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.iuin ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		sqlstr = sqlstr.replaceAll("par_partition", par_partition);
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		execSql();

		// final

		int type_reg_ad = 1;
		int type_reg_nature = 2;
		int type_noreg = 3;
		try {
			// ad
			sqlstr = "select userlevel , count(iuin) from ( select t1.iuin, par_userlevel from "
					+ tmp_userid_lost_ad
					+ " t1 left outer join "
					+ tmp_userone_score
					+ " t2 on (t1.iuin=t2.iuin) group by t1.iuin ) t group by userlevel ";
			basesql = sql_userlevel_common.replaceAll("result_value",
					"max(t2.value1)");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				int v2 = rs.getInt(2);
				resultstr = action_date + TAB + step_type + TAB + reg_startdate
						+ TAB + type_reg_ad + TAB + v1 + TAB + v2 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

			// nature
			sqlstr = "select userlevel , count(iuin) from ( select t1.iuin, par_userlevel from "
					+ tmp_userid_lost_nature
					+ " t1 left outer join "
					+ tmp_userone_score
					+ " t2 on (t1.iuin=t2.iuin) group by t1.iuin ) t group by userlevel ";
			basesql = sql_userlevel_common.replaceAll("result_value",
					"max(t2.value1)");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				int v2 = rs.getInt(2);
				resultstr = action_date + TAB + step_type + TAB + reg_startdate
						+ TAB + type_reg_nature + TAB + v1 + TAB + v2 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

			// no reg
			sqlstr = "select userlevel , count(iuin) from ( select t1.iuin, par_userlevel from "
					+ tmp_userid_lost_noreg
					+ " t1 left outer join "
					+ tmp_userone_score
					+ " t2 on (t1.iuin=t2.iuin) group by t1.iuin ) t group by userlevel ";
			basesql = sql_userlevel_common.replaceAll("result_value",
					"max(t2.value1)");
			sqlstr = sqlstr.replaceAll("par_userlevel", basesql);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				String v1 = rs.getString(1);
				int v2 = rs.getInt(2);
				resultstr = action_date + TAB + step_type + TAB + reg_startdate
						+ TAB + type_noreg + TAB + v1 + TAB + v2 + NEWLINE;
				resultosw.write(resultstr);
			}
			resultosw.flush();

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

	}
}
