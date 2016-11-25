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

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class DianHun_Td extends Tools implements DianHunSql {

	/**
	 * 计算流失用户流失前的等级
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_lostuserlevel_xxtd() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = td_ips;
		open_Conns();
		open_TDMainConns();

		if (other_args == null) {
			return;
		}
		int datecount = 0;
		datecount = new Integer(other_args.split(COMMA)[0]);

		basesql = "select A.char_id , max(A.level) from `game_data_log`.`par_table` A where  A.time >= 'par_starttime' and A.time < 'par_stoptime' group by A.char_id;";

		String before_startdate = addDay(action_date, 1 - 2 * datecount);
		String before_stopdate = addDay(action_date, 1 - datecount);

		List before_tablist = new ArrayList();
		for (int n = 0; n < datecount; n++) {
			tmpstr = addDay(before_startdate, n);
			tmpstr = tmpstr.substring(0, 4) + UNDERLINE
					+ tmpstr.substring(5, 7);
			if (before_tablist.contains(tmpstr) == false) {
				before_tablist.add(tmpstr);
			}
		}

		String after_startdate = addDay(action_date, 1 - datecount);
		String after_stopdate = addDay(action_date, 1);

		List after_tablist = new ArrayList();
		for (int n = 0; n < datecount; n++) {
			tmpstr = addDay(after_startdate, n);
			tmpstr = tmpstr.substring(0, 4) + UNDERLINE
					+ tmpstr.substring(5, 7);
			if (after_tablist.contains(tmpstr) == false) {
				after_tablist.add(tmpstr);
			}
		}

		for (int l = 0; l < conns.length; l++) {

			conn = conns[l];
			int zoneId = td_zoneIds[l];

			// cal before login user
			String tmp_tabname = "test.tmp_lostuser_beforeuserinfo";
			drop_tdmain_tmp_table(tmp_tabname);
			create_tdmain_tmp_table(tmp_tabname);

			String insertsql = "replace into " + tmp_tabname
					+ " (iuin,result_value) values (?,?) ;";
			for (int i = 0; i < before_tablist.size(); i++) {
				String par_tablename = (String) before_tablist.get(i);
				sqlstr = basesql
						.replaceAll("par_table", "tab_login_" + par_tablename)
						.replaceAll("par_starttime", before_startdate)
						.replaceAll("par_stoptime", before_stopdate);

				logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
				printLogStr(logstr);
				conn = conns[l];
				try {
					stmt = conn.createStatement();
					rs = stmt.executeQuery(sqlstr);

					PreparedStatement insertstmt = tdmainConn
							.prepareStatement(insertsql);
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

			// cal after login user

			tmp_tabname = "test.tmp_lostuser_afteruserinfo";
			drop_tdmain_tmp_table(tmp_tabname);
			create_tdmain_tmp_table(tmp_tabname);

			insertsql = "replace into " + tmp_tabname
					+ " (iuin,result_value) values (?,?) ;";

			for (int i = 0; i < after_tablist.size(); i++) {
				String par_tablename = (String) after_tablist.get(i);
				sqlstr = basesql
						.replaceAll("par_table", "tab_login_" + par_tablename)
						.replaceAll("par_starttime", after_startdate)
						.replaceAll("par_stoptime", after_stopdate);

				logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
				printLogStr(logstr);
				conn = conns[l];
				try {
					stmt = conn.createStatement();
					rs = stmt.executeQuery(sqlstr);

					PreparedStatement insertstmt = tdmainConn
							.prepareStatement(insertsql);
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

			// cal lost user level distribution

			sqlstr = std_lostuser_leveldistribution;
			logstr = "conn=" + tdmainhostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			Statement q1 = tdmainConn.createStatement();
			ResultSet rs1 = q1.executeQuery(sqlstr);

			while (rs1.next()) {
				String v1 = rs1.getString(1);
				int v2 = rs1.getInt(2);
				resultosw.write(zoneId + TAB + action_date + TAB + datecount
						+ TAB + v1 + TAB + v2 + NEWLINE);
			}
			resultosw.flush();
		}

		close_Conns();
		close_TDMainConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void get_charcreate_reg_category() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = td_ips;
		open_Conns();
		open_TDMainConns();
		open_ResultConn();

		// parameter
		String start_date = action_date;
		String stop_date = addDay(action_date, 1);

		String par_tablename = action_date.substring(0, 4) + UNDERLINE
				+ action_date.substring(5, 7);

		// get ad_id
		sqlstr = "select ad_id from tab_m3gcn_ad.m3gcn_ad_id where media_id in (157);";
		logstr = "conn=" + resulthostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = resultConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String par_ad_id_str = EMPTY;
		while (rs.next()) {
			par_ad_id_str += rs.getInt(1) + COMMA;
		}
		par_ad_id_str += "0";

		// get ad_id and p_id
		sqlstr = "select ad_id,product_id from tab_m3gcn_ad.m3gcn_ad_id ;";
		logstr = "conn=" + resulthostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		stmt = resultConn.createStatement();
		rs = stmt.executeQuery(sqlstr);

		String tmp_ad_pid = tmp_tab_pre + "ad_pid" + UNDERLINE + step;
		drop_tmp_table(tmp_ad_pid);
		create_tmp_table(tmp_ad_pid);

		String loadfile = workDir + step + load_suffix;
		FileOutputStream fos = new FileOutputStream(loadfile, true);
		OutputStreamWriter loadosw = new OutputStreamWriter(fos);
		while (rs.next()) {
			int v1 = rs.getInt(1);
			long v2 = rs.getLong(2);
			loadosw.write(v1 + TAB + v2 + NEWLINE);
		}
		loadosw.flush();
		loadosw.close();

		sqlstr = "LOAD DATA LOCAL INFILE '"
				+ loadfile
				+ "' replace into table "
				+ tmp_ad_pid
				+ " fields terminated by '\t' lines terminated by '\n' (ad_id,p_id) ;";

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

		for (int l = 0; l < conns.length; l++) {
			conn = conns[l];
			int zoneId = td_zoneIds[l];

			try {

				int nature_count = 0;
				int ad_count = 0;
				String p_count = EMPTY;

				// nature_count
				sqlstr = " select count(distinct A.id) from `game_data_log`.`par_table` A where  A.create_time >= 'par_starttime' and A.create_time < 'par_stoptime' and A.register_source in (par_ad_id)";
				sqlstr = sqlstr
						.replaceAll("par_table",
								"tab_create_char_" + par_tablename)
						.replaceAll("par_starttime", start_date)
						.replaceAll("par_stoptime", stop_date)
						.replaceAll("par_ad_id", par_ad_id_str);

				logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
				printLogStr(logstr);
				conn = conns[l];

				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				while (rs.next()) {
					nature_count = rs.getInt(1);
				}

				// ad_count
				sqlstr = " select count(distinct A.id) from `game_data_log`.`par_table` A where  A.create_time >= 'par_starttime' and A.create_time < 'par_stoptime' and A.register_source not in (par_ad_id)";
				sqlstr = sqlstr
						.replaceAll("par_table",
								"tab_create_char_" + par_tablename)
						.replaceAll("par_starttime", start_date)
						.replaceAll("par_stoptime", stop_date)
						.replaceAll("par_ad_id", par_ad_id_str);

				logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
				printLogStr(logstr);
				conn = conns[l];

				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				while (rs.next()) {
					ad_count = rs.getInt(1);
				}

				// p_count
				sqlstr = " select t2.p_id , count(distinct t1.id) from `game_data_log`.`par_table` t1 join "
						+ tmp_ad_pid
						+ " t2 on ( t1.register_source = t2.ad_id and t1.create_time >= 'par_starttime' and t1.create_time < 'par_stoptime' ) group by t2.p_id";
				sqlstr = sqlstr
						.replaceAll("par_table",
								"tab_create_char_" + par_tablename)
						.replaceAll("par_starttime", start_date)
						.replaceAll("par_stoptime", stop_date);

				logstr = "conn=" + ips[l] + ",sql=" + sqlstr;
				printLogStr(logstr);
				conn = conns[l];

				stmt = conn.createStatement();
				rs = stmt.executeQuery(sqlstr);

				while (rs.next()) {
					long v1 = rs.getLong(1);
					int v2 = rs.getInt(2);
					p_count += v1 + VERTICAL_LINE + v2 + COMMA;
				}
				if (p_count.endsWith(COMMA)) {
					p_count = p_count.substring(0, p_count.length() - 1);
				}

				resultosw.write(zoneId + TAB + action_date + TAB + nature_count
						+ TAB + ad_count + TAB + p_count + NEWLINE);
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}

		close_Conns();
		close_TDMainConn();
		close_ResultConn();
		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	/**
	 * 计算流失用户流失数
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void get_lostusercount() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		ips = td_ips;
		open_Conns();
		open_TDMainConns();

		if (other_args == null) {
			return;
		}
		int datecount = 0;
		datecount = new Integer(other_args.split(COMMA)[0]);

		int before_usercount = 0;
		int after_usercount = 0;

		// cal before login user
		String basesql = " select distinct A.char_id  from `game_data_log`.`par_table` A where  A.time >= 'par_starttime' and A.time < 'par_stoptime' ";

		String before_startdate = addDay(action_date, 1 - 2 * datecount);
		String before_stopdate = addDay(action_date, 1 - datecount);

		List before_tablist = new ArrayList();
		for (int n = 0; n < datecount; n++) {
			tmpstr = addDay(before_startdate, n);
			tmpstr = tmpstr.substring(0, 4) + UNDERLINE
					+ tmpstr.substring(5, 7);
			if (before_tablist.contains(tmpstr) == false) {
				before_tablist.add(tmpstr);
			}
		}

		String bebore_sqlstr = "select count(distinct char_id) from ( ";

		for (int i = 0; i < before_tablist.size(); i++) {
			String par_tablename = (String) before_tablist.get(i);
			bebore_sqlstr = bebore_sqlstr
					+ basesql
							.replaceAll("par_table",
									"tab_login_" + par_tablename)
							.replaceAll("par_starttime", before_startdate)
							.replaceAll("par_stoptime", before_stopdate)
					+ " union ";

		}
		bebore_sqlstr = bebore_sqlstr.substring(0, bebore_sqlstr.length() - 6);
		bebore_sqlstr = bebore_sqlstr + " ) T ;";

		// cal after login user

		String before_sql = "select distinct char_id from ( ";

		for (int i = 0; i < before_tablist.size(); i++) {
			String par_tablename = (String) before_tablist.get(i);

			before_sql = before_sql
					+ basesql
							.replaceAll("par_table",
									"tab_login_" + par_tablename)
							.replaceAll("par_starttime", before_startdate)
							.replaceAll("par_stoptime", before_stopdate)
					+ " union ";

		}
		before_sql = before_sql.substring(0, before_sql.length() - 6);
		before_sql = before_sql + " ) T ";

		// =======================================================
		String after_startdate = addDay(action_date, 1 - datecount);
		String after_stopdate = addDay(action_date, 1);
		List after_tablist = new ArrayList();
		for (int n = 0; n < datecount; n++) {
			tmpstr = addDay(after_startdate, n);
			tmpstr = tmpstr.substring(0, 4) + UNDERLINE
					+ tmpstr.substring(5, 7);
			if (after_tablist.contains(tmpstr) == false) {
				after_tablist.add(tmpstr);
			}
		}

		String after_sql = "select distinct char_id from ( ";

		for (int i = 0; i < after_tablist.size(); i++) {
			String par_tablename = (String) after_tablist.get(i);

			after_sql = after_sql
					+ basesql
							.replaceAll("par_table",
									"tab_login_" + par_tablename)
							.replaceAll("par_starttime", after_startdate)
							.replaceAll("par_stoptime", after_stopdate)
					+ " union ";

		}
		after_sql = after_sql.substring(0, after_sql.length() - 6);
		after_sql = after_sql + " ) T ";

		String after_sqlstr = "select count(distinct A1.char_id) from ( "
				+ after_sql + " ) A1 , ( " + before_sql
				+ " ) A2 where A1.char_id=A2.char_id ;";

		// final exec
		for (int l = 0; l < conns.length; l++) {
			conn = conns[l];
			int zoneId = td_zoneIds[l];
			try {

				// cal before user count
				logstr = "conn=" + tdmainhostportstr + ",sql=" + bebore_sqlstr;
				printLogStr(logstr);

				stmt = conn.createStatement();
				rs = stmt.executeQuery(bebore_sqlstr);
				while (rs.next()) {
					before_usercount = rs.getInt(1);
				}

				// cal after user count

				logstr = "conn=" + tdmainhostportstr + ",sql=" + after_sqlstr;
				printLogStr(logstr);

				stmt = conn.createStatement();
				rs = stmt.executeQuery(after_sqlstr);
				while (rs.next()) {
					after_usercount = rs.getInt(1);
				}
				resultosw.write(zoneId + TAB + action_date + TAB + datecount
						+ TAB + before_usercount + TAB + after_usercount
						+ NEWLINE);
				resultosw.flush();
			} catch (SQLException e) {
				printLogStr(ips[l] + COMMA + e.getMessage() + NEWLINE);
				continue;
			}
		}
		// =======================================================
		// =======================================================
		// =======================================================

		close_Conns();
		close_TDMainConn();

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

		Class c = DianHun_Td.class;
		Method m = c.getMethod(step);
		m.invoke(c);

	}

	public DianHun_Td() {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void runOut(String[] args) throws Exception {
		Options opts = new Options();
		opts.addOption("h", "help", false, "");
		opts.addOption("w", "workDir", true, "");
		opts.addOption("a", "action_day", true, "");
		opts.addOption("s", "steps", true, "");
		opts.addOption("l", "ad_id_limit_cnt", true, "");
		opts.addOption("d", "register_day_cnt", true, "");
		opts.addOption("H", "action_hour", true, "");
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
			System.out
					.println("-w D:\\temp\\stats\\ -a  2013-07-20 -s get_login_hours -l 500 -d 5 -i 23200,310,27580");
		}

	}
}
