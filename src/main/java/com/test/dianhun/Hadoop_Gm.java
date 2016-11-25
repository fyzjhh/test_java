package com.test.dianhun;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.codehaus.jackson.map.ObjectMapper;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class Hadoop_Gm extends Hadoop {

	public static void main(String[] args) throws Exception {

		runOut(args);
		System.out.println("====success====");
	}

	public Hadoop_Gm() {
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

		Class c = Hadoop_Gm.class;
		Method m = c.getMethod(step);
		m.invoke(c);
	}

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

	public static void get_userhiddenfightscoredistribution() throws Exception {

		before_exec();

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String min_fightscore = other_args.split(COMMA)[2];
		String max_fightscore = other_args.split(COMMA)[3];
		String stats_name = other_args.split(COMMA)[4];

		start_timestamp = spacedatetimeformat.format(new Date());

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		open_MongoConn();
		DB i_db = m.getDB(gm_db);
		DBCollection icol = i_db.getCollection(step);
		DBObject bo = new BasicDBObject();
		bo.put("start_date", start_date);
		bo.put("stop_date", stop_date);
		bo.put("min_fightscore", min_fightscore);
		bo.put("max_fightscore", max_fightscore);
		bo.put("stats_name", stats_name);

		// basesql =
		// "select count(char_id) from ( select T1.char_id, case when T2.sub_type=1004 then T2.param2+T2.param3 when T2.sub_type=1005 then 1073741824-T2.param3+T2.param2 else 0L end fightscore from ( select char_id,max(create_time) create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 group by char_id ) T1 join ( select char_id,sub_type,param2,param3,create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 ) T2 on (T1.char_id=T2.char_id and T1.create_time=T2.create_time) ) t where fightscore>=par_min and fightscore<=par_max  ";
		basesql = "select count(char_id) from ( select T1.char_id, case when T2.sub_type=1004 then T2.param2+T2.param3 when T2.sub_type=1005 then T2.param2-T2.param3 else 0L end fightscore from ( select char_id,max(create_time) create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 group by char_id ) T1 join ( select char_id,sub_type,param2,param3,create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 ) T2 on (T1.char_id=T2.char_id and T1.create_time=T2.create_time) ) t where fightscore>=par_min and fightscore<=par_max  ";
		// basesql =
		// "select count(char_id) from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 ";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date)
					.replaceAll("par_min", min_fightscore)
					.replaceAll("par_max", max_fightscore);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			List<DBObject> subbolist = new ArrayList<DBObject>();
			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					Integer v1 = rs.getInt(1);
					DBObject subbo = new BasicDBObject();
					subbo.put("value", v1);
					subbolist.add(subbo);
				}
				bo.put("zoneId_" + zoneId, subbolist);

			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}
		stop_timestamp = spacedatetimeformat.format(new Date());
		bo.put("start_timestamp", start_timestamp);
		bo.put("stop_timestamp", stop_timestamp);
		// write to json
		ObjectMapper objectMapper = new ObjectMapper();
		resultstr = objectMapper.writeValueAsString(bo.toString()) + NEWLINE;
		resultosw.write(resultstr);
		resultosw.flush();

		icol.save(bo);
		close_MongoConn();
		// //
		after_exec();

	}

	public static void get_userhiddenfightscoredistribution_byzoneid()
			throws Exception {

		before_exec();

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String stats_zoneId = other_args.split(COMMA)[2];
		String stats_name = other_args.split(COMMA)[3];

		start_timestamp = spacedatetimeformat.format(new Date());

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		open_MongoConn();
		DB i_db = m.getDB(gm_db);
		DBCollection icol = i_db.getCollection(step);
		DBObject bo = new BasicDBObject();
		bo.put("start_date", start_date);
		bo.put("stop_date", stop_date);
		bo.put("zoneId", stats_zoneId);
		bo.put("stats_name", stats_name);

		basesql = "select fightscorelevel , count(char_id) from ( select T1.char_id, case when T2.sub_type=1004 then cast((T2.param2+T2.param3)/1000 as int) when T2.sub_type=1005 then cast((T2.param2-T2.param3)/1000 as int) else 0 end fightscorelevel from ( select char_id,max(create_time) create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 and sub_type in (1004,1005) group by char_id ) T1 join ( select char_id,sub_type,param2,param3,create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 and sub_type in (1004,1005) ) T2 on (T1.char_id=T2.char_id and T1.create_time=T2.create_time) ) t group by fightscorelevel";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			if (String.valueOf(zoneId).equalsIgnoreCase(stats_zoneId)) {

				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				List<DBObject> subbolist = new ArrayList<DBObject>();
				try {
					stmt = hiveConn.createStatement();
					rs = stmt.executeQuery(sqlstr);
					while (rs.next()) {
						Integer v1 = rs.getInt(1);
						Integer v2 = rs.getInt(2);
						DBObject subbo = new BasicDBObject();
						subbo.put("fightscorelevel_" + v1, v2);
						subbolist.add(subbo);
					}
					bo.put("zoneId_" + zoneId, subbolist);

				} catch (SQLException e) {
					printLogStr(hivehostportstr + COMMA + e.getMessage()
							+ NEWLINE);
				}
			}
		}
		stop_timestamp = spacedatetimeformat.format(new Date());
		bo.put("start_timestamp", start_timestamp);
		bo.put("stop_timestamp", stop_timestamp);
		// write to json
		ObjectMapper objectMapper = new ObjectMapper();
		resultstr = objectMapper.writeValueAsString(bo.toString()) + NEWLINE;
		resultosw.write(resultstr);
		resultosw.flush();

		icol.save(bo);
		close_MongoConn();
		// //
		after_exec();

	}

	public static void get_userhiddenfightscore() throws Exception {

		before_exec();

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String iuinstr = other_args.split(COMMA)[2];
		String stats_name = other_args.split(COMMA)[3];

		start_timestamp = spacedatetimeformat.format(new Date());

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		String[] iuin_arr = iuinstr.split(UNDERLINE);
		String iuins = Arrays.toString(iuin_arr);
		iuins = iuins.substring(1, iuins.length() - 1);

		open_MongoConn();
		DB i_db = m.getDB(gm_db);
		DBCollection icol = i_db.getCollection(step);

		String tmpsql1 = EMPTY;
		for (int i = 0; i < zoneDbStrs.length; i++) {
			int zoneId = zoneIds[i];
			tmpsql1 += " max(case when zoneId=" + zoneId
					+ " then v end) zoneId_" + zoneId + " ,";
		}
		tmpsql1 = tmpsql1.substring(0, tmpsql1.length() - 2);

		basesql = "select char_id,par_zoneId zoneId, sub_type,param2,param3,create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 and sub_type in (1004,1005) and char_id in (par_iuins) ";
		sqlstr = "select char_id, "
				+ tmpsql1
				+ " from ( select t.char_id , t.zoneId , collect_set(concat(t.sub_type,'|',t.param2,'|',t.param3,'|',t.create_time)) v from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date)
					.replaceAll("par_iuins", iuins)
					.replaceAll("par_zoneId", String.valueOf(zoneId));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s
						+ " ) t group by t.char_id , t.zoneId ) tt group by char_id ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			stop_timestamp = spacedatetimeformat.format(new Date());
			while (rs.next()) {

				Integer v1 = rs.getInt(1);
				String v2 = rs.getString(2);
				String v3 = rs.getString(3);
				String v4 = rs.getString(4);
				String v5 = rs.getString(5);
				String v6 = rs.getString(6);
				String v7 = rs.getString(7);
				String v8 = rs.getString(8);
				String v9 = rs.getString(9);

				DBObject bo = new BasicDBObject();
				bo.put("iuin", String.valueOf(v1));
				bo.put("start_date", start_date);
				bo.put("stop_date", stop_date);
				bo.put("stats_name", stats_name);
				bo.put("zoneId_1", v2);
				bo.put("zoneId_2", v3);
				bo.put("zoneId_3", v4);
				bo.put("zoneId_6", v5);
				bo.put("zoneId_7", v6);
				bo.put("zoneId_8", v7);
				bo.put("zoneId_9", v8);
				bo.put("zoneId_10", v9);
				bo.put("start_timestamp", start_timestamp);
				bo.put("stop_timestamp", stop_timestamp);

				// write to json
				ObjectMapper objectMapper = new ObjectMapper();
				resultstr = objectMapper.writeValueAsString(bo.toString())
						+ NEWLINE;
				resultosw.write(resultstr);
				resultosw.flush();

				icol.save(bo);
			}

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		close_MongoConn();
		// //
		after_exec();

	}

	public static void get_user_fightscore_minus_byzoneid() throws Exception {

		before_exec();

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String stats_zoneId = other_args.split(COMMA)[2];
		String stats_name = other_args.split(COMMA)[3];

		start_timestamp = spacedatetimeformat.format(new Date());

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		open_MongoConn();
		DB i_db = m.getDB(gm_db);
		DBCollection icol = i_db.getCollection(step);
		DBObject bo = new BasicDBObject();
		bo.put("start_date", start_date);
		bo.put("stop_date", stop_date);
		bo.put("zoneId", stats_zoneId);
		bo.put("stats_name", stats_name);

		basesql = "select fightscorelevel , count(char_id) from ( select T1.char_id, case when T2.sub_type=1004 then cast((T2.param2+T2.param3)/1000 as int) when T2.sub_type=1005 then cast((T2.param2-T2.param3)/1000 as int) else 0 end fightscorelevel from ( select char_id,max(create_time) create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 and sub_type in (1004,1005) group by char_id ) T1 join ( select char_id,sub_type,param2,param3,create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 and sub_type in (1004,1005) ) T2 on (T1.char_id=T2.char_id and T1.create_time=T2.create_time) ) t group by fightscorelevel";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			if (String.valueOf(zoneId).equalsIgnoreCase(stats_zoneId)) {

				sqlstr = basesql.replaceAll("par_dbname", zoneStr)
						.replaceAll("par_partition", par_partition)
						.replaceAll("par_startdate", start_date)
						.replaceAll("par_stopdate", final_stop_date);
				logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
				printLogStr(logstr);

				List<DBObject> subbolist = new ArrayList<DBObject>();
				try {
					stmt = hiveConn.createStatement();
					rs = stmt.executeQuery(sqlstr);
					while (rs.next()) {
						Integer v1 = rs.getInt(1);
						Integer v2 = rs.getInt(2);
						DBObject subbo = new BasicDBObject();
						subbo.put("fightscorelevel_" + v1, v2);
						subbolist.add(subbo);
					}
					bo.put("zoneId_" + zoneId, subbolist);

				} catch (SQLException e) {
					printLogStr(hivehostportstr + COMMA + e.getMessage()
							+ NEWLINE);
				}
			}
		}
		stop_timestamp = spacedatetimeformat.format(new Date());
		bo.put("start_timestamp", start_timestamp);
		bo.put("stop_timestamp", stop_timestamp);
		// write to json
		ObjectMapper objectMapper = new ObjectMapper();
		resultstr = objectMapper.writeValueAsString(bo.toString()) + NEWLINE;
		resultosw.write(resultstr);
		resultosw.flush();

		icol.save(bo);
		close_MongoConn();
		// //
		after_exec();

	}

	public static void get_user_fightscore_minus_old() throws Exception {

		before_exec();

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String iuinstr = other_args.split(COMMA)[2];
		String stats_name = other_args.split(COMMA)[3];

		start_timestamp = spacedatetimeformat.format(new Date());

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		String[] iuin_arr = iuinstr.split(UNDERLINE);
		String iuins = Arrays.toString(iuin_arr);
		iuins = iuins.substring(1, iuins.length() - 1);

		open_MongoConn();
		DB i_db = m.getDB(gm_db);
		DBCollection icol = i_db.getCollection(step);

		// minus count
		String tmp_usertwodata_1 = tmp_tab_pre + "usertwodata_1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_usertwodata_1);
		create_tmp_table(tmp_usertwodata_1);

		basesql = "select char_id,par_zoneId zoneId from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 and sub_type in (1003) and char_id in (par_iuins) ";
		sqlstr = "INSERT OVERWRITE TABLE " + tmp_usertwodata_1
				+ " select t.char_id , t.zoneId ,count(t.char_id) v from ( ";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date)
					.replaceAll("par_iuins", iuins)
					.replaceAll("par_zoneId", String.valueOf(zoneId));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.char_id , t.zoneId ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		execSql();

		// before minus 1

		String another_start_date = addDay(start_date, -30);
		String another_stop_date = stop_date;
		String another_final_stop_date = addDay(another_stop_date, 1);
		String another_par_partition = cal_partitionstr(another_start_date,
				another_stop_date);

		String tmp_userthreestr1 = tmp_tab_pre + "userthreestr1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userthreestr1);
		create_tmp_table(tmp_userthreestr1);

		// basesql =
		// " select playerid,par_zoneId zoneId,fightscorefun,recordtime from par_dbname.tab_town_leave where par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' and playerid in (par_iuins) and fightscorefun>0 ";
		basesql = " select id,par_zoneId zoneId,ifightscorefun,record_time from par_dbname.tab_town_login where par_datetime in (par_partition) and record_time>='par_startdate' and record_time<'par_stopdate' and id in (par_iuins) ";
		sqlstr = "INSERT OVERWRITE TABLE " + tmp_userthreestr1
				+ " select * from ( ";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", another_par_partition)
					.replaceAll("par_startdate", another_start_date)
					.replaceAll("par_stopdate", another_final_stop_date)
					.replaceAll("par_iuins", iuins)
					.replaceAll("par_zoneId", String.valueOf(zoneId));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t  ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		execSql();

		// before minus 2
		String tmp_usertwodata_2 = tmp_tab_pre + "usertwodata_2" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_usertwodata_2);
		create_tmp_table(tmp_usertwodata_2);

		sqlstr = "INSERT OVERWRITE TABLE "
				+ tmp_usertwodata_2
				+ " select t1.iuin , t1.value1 , t2.value2 from "
				+ " ( select iuin,value1,max(value3) v from "
				+ tmp_userthreestr1
				+ " group by iuin , value1 ) t1  join "
				+ tmp_userthreestr1
				+ " t2 on ( t1.iuin=t2.iuin and t1.value1=t2.value1 and t1.v=t2.value3 ) ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		execSql();

		// final
		String tmpsql1 = EMPTY;
		for (int i = 0; i < zoneDbStrs.length; i++) {
			int zoneId = zoneIds[i];
			tmpsql1 += " case when zoneId=" + zoneId + " then v end zoneId_"
					+ zoneId + " ,";
		}
		tmpsql1 = tmpsql1.substring(0, tmpsql1.length() - 2);

		sqlstr = "select t11.iuin, "
				+ tmpsql1
				+ " from ( select distinct iuin from ( select explode(split('par_iuins',',') ) as iuin from "
				+ tmp_usertwodata_1
				+ " ) t0 ) t11 left outer join "
				+ " ( select t1.iuin , t1.value1 zoneId ,concat(case when t2.value2 is null then -1 else t2.value2 end,'|', case when t1.value2 is null then -1 else t1.value2 end) v from "
				+ tmp_usertwodata_2
				+ " t1 left outer join "
				+ tmp_usertwodata_1
				+ " t2 on (t1.iuin=t2.iuin and t1.value1=t2.value1) ) t12 on (t11.iuin = t12.iuin)";
		sqlstr = sqlstr.replace("par_iuins", iuins.replaceAll(SPACE, EMPTY));
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			stop_timestamp = spacedatetimeformat.format(new Date());
			while (rs.next()) {

				Integer v1 = rs.getInt(1);
				String v2 = rs.getString(2);
				String v3 = rs.getString(3);
				String v4 = rs.getString(4);
				String v5 = rs.getString(5);
				String v6 = rs.getString(6);
				String v7 = rs.getString(7);
				String v8 = rs.getString(8);
				String v9 = rs.getString(9);

				DBObject bo = new BasicDBObject();
				bo.put("iuin", String.valueOf(v1));
				bo.put("start_date", start_date);
				bo.put("stop_date", stop_date);
				bo.put("stats_name", stats_name);
				bo.put("zoneId_1", v2);
				bo.put("zoneId_2", v3);
				bo.put("zoneId_3", v4);
				bo.put("zoneId_6", v5);
				bo.put("zoneId_7", v6);
				bo.put("zoneId_8", v7);
				bo.put("zoneId_9", v8);
				bo.put("zoneId_10", v9);
				bo.put("start_timestamp", start_timestamp);
				bo.put("stop_timestamp", stop_timestamp);

				// write to json
				ObjectMapper objectMapper = new ObjectMapper();
				resultstr = objectMapper.writeValueAsString(bo.toString())
						+ NEWLINE;
				resultosw.write(resultstr);
				resultosw.flush();

				icol.save(bo);
			}

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		close_MongoConn();
		//
		after_exec();

		basesql = " select playerid, count(playerid) from par_dbname.tab_town_leave par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' and playerid in () ";

	}

	public static void get_userhiddenfightscore_bak() throws Exception {

		before_exec();

		String iuin = other_args.split(COMMA)[0];
		start_date = other_args.split(COMMA)[1];
		stop_date = other_args.split(COMMA)[2];
		String stats_name = other_args.split(COMMA)[3];

		start_timestamp = spacedatetimeformat.format(new Date());

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		open_MongoConn();
		DB i_db = m.getDB(gm_db);
		DBCollection icol = i_db.getCollection(step);
		DBObject bo = new BasicDBObject();
		bo.put("iuin", iuin);
		bo.put("start_date", start_date);
		bo.put("stop_date", stop_date);
		bo.put("stats_name", stats_name);

		basesql = "select char_id,sub_type,param2,param3,create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 and sub_type in (1004,1005) and char_id=par_iuin order by create_time ";
		// basesql =
		// "select char_id,sub_type,param2,param3,create_time from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and char_id=par_iuin limit 10";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date)
					.replaceAll("par_iuin", iuin);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);

			List<DBObject> subbolist = new ArrayList<DBObject>();
			try {
				stmt = hiveConn.createStatement();
				rs = stmt.executeQuery(sqlstr);
				while (rs.next()) {
					// Integer v1 = rs.getInt(1);
					Integer v2 = rs.getInt(2);
					Integer v3 = rs.getInt(3);
					Integer v4 = rs.getInt(4);
					String v5 = rs.getString(5);
					DBObject subbo = new BasicDBObject();
					subbo.put("sub_type", v2);
					subbo.put("param2", v3);
					subbo.put("param3", v4);
					subbo.put("create_time", v5);
					subbolist.add(subbo);
				}
				bo.put("zoneId_" + zoneId, subbolist);

			} catch (SQLException e) {
				printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
			}
		}
		stop_timestamp = spacedatetimeformat.format(new Date());
		bo.put("start_timestamp", start_timestamp);
		bo.put("stop_timestamp", stop_timestamp);
		// write to json
		ObjectMapper objectMapper = new ObjectMapper();
		resultstr = objectMapper.writeValueAsString(bo.toString()) + NEWLINE;
		resultosw.write(resultstr);
		resultosw.flush();

		icol.save(bo);
		close_MongoConn();
		// //
		after_exec();

	}

	public static void get_user_fightscore_minus() throws Exception {

		before_exec();

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function userlevel as 'com.udf.UserLevel'");

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String iuinstr = other_args.split(COMMA)[2];
		String stats_name = other_args.split(COMMA)[3];

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		String another_start_date = addDay(start_date, -30);
		String another_stop_date = stop_date;
		String another_final_stop_date = addDay(another_stop_date, 1);
		String another_par_partition = cal_partitionstr(another_start_date,
				another_stop_date);

		String[] iuin_arr = iuinstr.split(UNDERLINE);
		String iuins = Arrays.toString(iuin_arr);
		iuins = iuins.substring(1, iuins.length() - 1);

		start_timestamp = spacedatetimeformat.format(new Date());

		// final
		String tmp_usertwostr1_final = tmp_tab_pre + "usertwostr1_final"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_usertwostr1_final);
		create_tmp_table(tmp_usertwostr1_final);

//		zoneDbStrs = new String[] { "chibi_1", "caochuan_2", "shuiyan_3",
//				"jingzhou_6", "qingmei_7", "guandu_8", "taoyuan_9", "maolu_10" };
//		zoneIds = new int[] { 1, 2, 3, 6, 7, 8, 9, 10 };
		printLogStr(Arrays.toString(zoneDbStrs));
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];

			// minus count
			String tmp_useronedata_1 = tmp_tab_pre + "useronedata_1"
					+ UNDERLINE + step;
			drop_tmp_table(tmp_useronedata_1);
			create_tmp_table(tmp_useronedata_1);

			// before
			String tmp_usertwostr1 = tmp_tab_pre + "usertwostr1" + UNDERLINE
					+ step;
			drop_tmp_table(tmp_usertwostr1);
			create_tmp_table(tmp_usertwostr1);

			String tmp_useronedata_2 = tmp_tab_pre + "useronedata_2"
					+ UNDERLINE + step;
			drop_tmp_table(tmp_useronedata_2);
			create_tmp_table(tmp_useronedata_2);

			//
			String tmp_useronedata_score = tmp_tab_pre + "useronedata_score"
					+ UNDERLINE + step;
			drop_tmp_table(tmp_useronedata_score);
			create_tmp_table(tmp_useronedata_score);
			//
			String tmp_useronedata_money = tmp_tab_pre + "useronedata_money"
					+ UNDERLINE + step;
			drop_tmp_table(tmp_useronedata_money);
			create_tmp_table(tmp_useronedata_money);

			// minus count
			basesql = "INSERT OVERWRITE TABLE "
					+ tmp_useronedata_1
					+ " select char_id, count(1) v from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 and sub_type in (1003) and char_id in (par_iuins) group by char_id";

			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date)
					.replaceAll("par_iuins", iuins);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			execSql();

			// before minus
			basesql = "INSERT OVERWRITE TABLE "
					+ tmp_usertwostr1
					+ " select id,ifightscorefun,record_time from par_dbname.tab_town_login where par_datetime in (par_partition) and record_time>='par_startdate' and record_time<'par_stopdate' and id in (par_iuins) ";

			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", another_par_partition)
					.replaceAll("par_startdate", another_start_date)
					.replaceAll("par_stopdate", another_final_stop_date)
					.replaceAll("par_iuins", iuins);
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			execSql();

			sqlstr = "INSERT OVERWRITE TABLE " + tmp_useronedata_2
					+ " select t1.iuin , t2.value1 from "
					+ " ( select iuin,max(value2) v from " + tmp_usertwostr1
					+ " group by iuin ) t1  join " + tmp_usertwostr1
					+ " t2 on ( t1.iuin=t2.iuin and t1.v=t2.value2 ) ";
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			execSql();

			// query score

			basesql = "INSERT OVERWRITE TABLE "
					+ tmp_useronedata_score
					+ " select playerid ,max(score) v from par_dbname.tab_town_leave where par_datetime in (par_partition) and playerid in (par_iuins) and recordtime>='par_startdate' and recordtime<'par_stopdate' group by playerid ";

			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", another_par_partition)
					.replaceAll("par_startdate", another_start_date)
					.replaceAll("par_stopdate", another_final_stop_date)
					.replaceAll("par_iuins", iuins);

			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			execSql();

			// money

			basesql = "INSERT OVERWRITE TABLE "
					+ tmp_useronedata_money
					+ " select iuin , sum(changed_money) v from par_dbname.tab_m3g_money where iuin in (par_iuins) and par_datetime in (par_partition) and dtLogTime>='par_startdate' and dtLogTime<'par_stopdate' and iPayType in (4) group by iuin ";
			sqlstr = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", another_par_partition)
					.replaceAll("par_startdate", another_start_date)
					.replaceAll("par_stopdate", another_final_stop_date)
					.replaceAll("par_iuins", iuins);

			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			execSql();

			// zone final
			sqlstr = "INSERT INTO TABLE "
					+ tmp_usertwostr1_final
					+ " select t11.iuin ,par_zoneId zoneId, "
					+ " concat(case when t12.value1 is null then -1 else t12.value1 end ,'|', case when t13.value1 is null then -1 else t13.value1 end ,'|', case when t14.value1 is null then -1 else userlevel(t14.value1) end ,'|', case when t15.value1 is null then -1 else t15.value1 end ) "
					+ " v from "
					+ " ( select distinct iuin from ( select explode(split('par_iuins',',') ) as iuin from "
					+ tmp_useronedata_1 + " ) t0 ) t11 left outer join "
					+ tmp_useronedata_1
					+ " t12 on (t11.iuin=t12.iuin) left outer join "
					+ tmp_useronedata_2
					+ " t13 on (t11.iuin=t13.iuin) left outer join "
					+ tmp_useronedata_score
					+ " t14 on (t11.iuin=t14.iuin) left outer join "
					+ tmp_useronedata_money + " t15 on (t11.iuin = t15.iuin) ";

			sqlstr = sqlstr
					.replace("par_iuins", iuins.replaceAll(SPACE, EMPTY))
					.replaceAll("par_zoneId", String.valueOf(zoneId));
			logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
			printLogStr(logstr);
			execSql();

		}

		// final

		open_MongoConn();
		DB i_db = m.getDB(gm_db);
		DBCollection icol = i_db.getCollection(step);

		String tmpsql1 = EMPTY;
		String tmpsql2 = EMPTY;
		for (int i = 0; i < zoneDbStrs.length; i++) {
			int zoneId = zoneIds[i];
			tmpsql1 += " case when value1=" + zoneId
					+ " then value2 end zoneId_" + zoneId + " ,";
			tmpsql2 += " max(zoneId_" + zoneId + ") ,";
		}
		tmpsql1 = tmpsql1.substring(0, tmpsql1.length() - 2);
		tmpsql2 = tmpsql2.substring(0, tmpsql2.length() - 2);
		sqlstr = "select iuin, " + tmpsql2 + " from ( select iuin, " + tmpsql1
				+ " from " + tmp_usertwostr1_final + " ) t group by iuin ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			stop_timestamp = spacedatetimeformat.format(new Date());
			while (rs.next()) {

				Integer v1 = rs.getInt(1);
				String v2 = rs.getString(2);
				String v3 = rs.getString(3);
				String v4 = rs.getString(4);
				String v5 = rs.getString(5);
				String v6 = rs.getString(6);
				String v7 = rs.getString(7);
				String v8 = rs.getString(8);
//				String v9 = rs.getString(9);

				DBObject bo = new BasicDBObject();
				bo.put("iuin", String.valueOf(v1));
				bo.put("start_date", start_date);
				bo.put("stop_date", stop_date);
				bo.put("stats_name", stats_name);
//				bo.put("zoneId_1", v2);
//				bo.put("zoneId_2", v3);
//				bo.put("zoneId_3", v4);
//				bo.put("zoneId_6", v5);
//				bo.put("zoneId_7", v6);
//				bo.put("zoneId_8", v7);
//				bo.put("zoneId_9", v8);
//				bo.put("zoneId_10", v9);
				bo.put("zoneId_1", v2);
				bo.put("zoneId_2", v3);
				bo.put("zoneId_6", v4);
				bo.put("zoneId_7", v5);
				bo.put("zoneId_8", v6);
				bo.put("zoneId_9", v7);
				bo.put("zoneId_11", v8);

				bo.put("start_timestamp", start_timestamp);
				bo.put("stop_timestamp", stop_timestamp);

				// write to json
				ObjectMapper objectMapper = new ObjectMapper();
				resultstr = objectMapper.writeValueAsString(bo.toString())
						+ NEWLINE;
				resultosw.write(resultstr);
				resultosw.flush();

				icol.save(bo);
			}

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		close_MongoConn();

		//
		after_exec();

	}

	public static void get_user_fightscore_minus_x1() throws Exception {

		before_exec();

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function userlevel as 'com.udf.UserLevel'");

		start_date = other_args.split(COMMA)[0];
		stop_date = other_args.split(COMMA)[1];
		String iuinstr = other_args.split(COMMA)[2];
		String stats_name = other_args.split(COMMA)[3];

		start_timestamp = spacedatetimeformat.format(new Date());

		String final_stop_date = addDay(stop_date, 1);
		par_partition = cal_partitionstr(start_date, stop_date);

		String[] iuin_arr = iuinstr.split(UNDERLINE);
		String iuins = Arrays.toString(iuin_arr);
		iuins = iuins.substring(1, iuins.length() - 1);

		open_MongoConn();
		DB i_db = m.getDB(gm_db);
		DBCollection icol = i_db.getCollection(step);

		// minus count
		String tmp_usertwodata_1 = tmp_tab_pre + "usertwodata_1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_usertwodata_1);
		create_tmp_table(tmp_usertwodata_1);

		basesql = "select char_id,par_zoneId zoneId from par_dbname.tab_misc where par_datetime in (par_partition) and create_time>='par_startdate' and create_time<'par_stopdate' and type=4 and sub_type in (1003) and char_id in (par_iuins) ";
		sqlstr = "INSERT OVERWRITE TABLE " + tmp_usertwodata_1
				+ " select t.char_id , t.zoneId ,count(t.char_id) v from ( ";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date)
					.replaceAll("par_iuins", iuins)
					.replaceAll("par_zoneId", String.valueOf(zoneId));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t group by t.char_id , t.zoneId ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		execSql();

		// before minus 1

		String another_start_date = addDay(start_date, -30);
		String another_stop_date = stop_date;
		String another_final_stop_date = addDay(another_stop_date, 1);
		String another_par_partition = cal_partitionstr(another_start_date,
				another_stop_date);

		String tmp_userthreestr1 = tmp_tab_pre + "userthreestr1" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_userthreestr1);
		create_tmp_table(tmp_userthreestr1);
		basesql = " select id,par_zoneId zoneId,ifightscorefun,record_time from par_dbname.tab_town_login where par_datetime in (par_partition) and record_time>='par_startdate' and record_time<'par_stopdate' and id in (par_iuins) ";
		sqlstr = "INSERT OVERWRITE TABLE " + tmp_userthreestr1
				+ " select * from ( ";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", another_par_partition)
					.replaceAll("par_startdate", another_start_date)
					.replaceAll("par_stopdate", another_final_stop_date)
					.replaceAll("par_iuins", iuins)
					.replaceAll("par_zoneId", String.valueOf(zoneId));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t  ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		execSql();

		// before minus 2
		String tmp_usertwodata_2 = tmp_tab_pre + "usertwodata_2" + UNDERLINE
				+ step;
		drop_tmp_table(tmp_usertwodata_2);
		create_tmp_table(tmp_usertwodata_2);

		sqlstr = "INSERT OVERWRITE TABLE "
				+ tmp_usertwodata_2
				+ " select t1.iuin , t1.value1 , t2.value2 from "
				+ " ( select iuin,value1,max(value3) v from "
				+ tmp_userthreestr1
				+ " group by iuin , value1 ) t1  join "
				+ tmp_userthreestr1
				+ " t2 on ( t1.iuin=t2.iuin and t1.value1=t2.value1 and t1.v=t2.value3 ) ";
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		execSql();

		// query score
		String tmp_usertwodata_score = tmp_tab_pre + "usertwodata_score"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_usertwodata_score);
		create_tmp_table(tmp_usertwodata_score);

		basesql = " select playerid ,par_zoneId zoneId ,max(score) v from par_dbname.tab_town_leave where par_datetime in (par_partition) and playerid in (par_iuins) and recordtime>='par_startdate' and recordtime<'par_stopdate' group by playerid ";

		sqlstr = "INSERT OVERWRITE TABLE " + tmp_usertwodata_score
				+ " select * from ( ";

		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", another_par_partition)
					.replaceAll("par_startdate", another_start_date)
					.replaceAll("par_stopdate", another_final_stop_date)
					.replaceAll("par_iuins", iuins)
					.replaceAll("par_zoneId", String.valueOf(zoneId));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t  ";
			} else {
				sqlstr += s + " union all ";
			}
		}
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		execSql();

		// money
		String tmp_usertwodata_money = tmp_tab_pre + "usertwodata_money"
				+ UNDERLINE + step;
		drop_tmp_table(tmp_usertwodata_money);
		create_tmp_table(tmp_usertwodata_money);
		basesql = " select iuin ,par_zoneId zoneId, sum(changed_money) v from par_dbname.tab_m3g_money where iuin in (par_iuins) and par_datetime in (par_partition) and dtLogTime>='par_startdate' and dtLogTime<'par_stopdate' and iPayType in (4) group by iuin ";
		sqlstr = "INSERT OVERWRITE TABLE " + tmp_usertwodata_money
				+ " select * from ( ";
		for (int i = 0; i < zoneDbStrs.length; i++) {
			String zoneStr = zoneDbStrs[i];
			int zoneId = zoneIds[i];
			String s = basesql.replaceAll("par_dbname", zoneStr)
					.replaceAll("par_partition", par_partition)
					.replaceAll("par_startdate", start_date)
					.replaceAll("par_stopdate", final_stop_date)
					.replaceAll("par_iuins", iuins)
					.replaceAll("par_zoneId", String.valueOf(zoneId));
			if (i == zoneDbStrs.length - 1) {
				sqlstr += s + " ) t  ";
			} else {
				sqlstr += s + " union all ";
			}
		}

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);
		execSql();

		// final
		String tmpsql1 = EMPTY;
		for (int i = 0; i < zoneDbStrs.length; i++) {
			int zoneId = zoneIds[i];
			tmpsql1 += " case when zoneId=" + zoneId + " then v end zoneId_"
					+ zoneId + " ,";
		}
		tmpsql1 = tmpsql1.substring(0, tmpsql1.length() - 2);

		sqlstr = "select t11.iuin, "
				+ tmpsql1
				+ " , userlevel(t13.value1) , t14.value1 from ( select distinct iuin from ( select explode(split('par_iuins',',') ) as iuin from "
				+ tmp_usertwodata_1
				+ " ) t0 ) t11 left outer join "
				+ " ( select t1.iuin , t1.value1 zoneId ,concat(case when t2.value2 is null then -1 else t2.value2 end,'|', case when t1.value2 is null then -1 else t1.value2 end) v from "
				+ tmp_usertwodata_2
				+ " t1 left outer join "
				+ tmp_usertwodata_1
				+ " t2 on (t1.iuin=t2.iuin and t1.value1=t2.value1) ) t12 on (t11.iuin = t12.iuin) "
				+ " left outer join " + tmp_usertwodata_score
				+ " t13 on (t11.iuin = t13.iuin) " + " left outer join "
				+ tmp_usertwodata_money + " t14 on (t11.iuin = t14.iuin) ";

		sqlstr = "select t11.iuin, "
				+ tmpsql1
				+ " , userlevel(t13.value1) , t14.value1 "
				+ " ( select t11.iuin , t12.value1,t12.value2 , t13.value1,t13.value2 , t14.value1,t14.value2 , t15.value1,t15.value2  from ( select distinct iuin from ( select explode(split('par_iuins',',') ) as iuin from "
				+ tmp_usertwodata_1 + " ) t0 ) t11 left outer join "
				+ tmp_usertwodata_1
				+ " t12 on (t11.iuin=t12.iuin) left outer join "
				+ tmp_usertwodata_2
				+ " t13 on (t11.iuin=t13.iuin) left outer join "
				+ tmp_usertwodata_score
				+ " t14 on (t11.iuin=t14.iuin) left outer join "
				+ tmp_usertwodata_money
				+ " t15 on (t11.iuin = t15.iuin) ) t group by iuin ";

		sqlstr = sqlstr.replace("par_iuins", iuins.replaceAll(SPACE, EMPTY));
		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			stop_timestamp = spacedatetimeformat.format(new Date());
			while (rs.next()) {

				Integer v1 = rs.getInt(1);
				String v2 = rs.getString(2);
				String v3 = rs.getString(3);
				String v4 = rs.getString(4);
				String v5 = rs.getString(5);
				String v6 = rs.getString(6);
				String v7 = rs.getString(7);
				String v8 = rs.getString(8);
				String v9 = rs.getString(9);
				String v10 = rs.getString(10);
				String v11 = rs.getString(11);

				DBObject bo = new BasicDBObject();
				bo.put("iuin", String.valueOf(v1));
				bo.put("start_date", start_date);
				bo.put("stop_date", stop_date);
				bo.put("stats_name", stats_name);
				bo.put("zoneId_1", v2);
				bo.put("zoneId_2", v3);
				bo.put("zoneId_3", v4);
				bo.put("zoneId_6", v5);
				bo.put("zoneId_7", v6);
				bo.put("zoneId_8", v7);
				bo.put("zoneId_9", v8);
				bo.put("zoneId_10", v9);
				bo.put("userlevel", v10);
				bo.put("pay", v11);
				bo.put("start_timestamp", start_timestamp);
				bo.put("stop_timestamp", stop_timestamp);

				// write to json
				ObjectMapper objectMapper = new ObjectMapper();
				resultstr = objectMapper.writeValueAsString(bo.toString())
						+ NEWLINE;
				resultosw.write(resultstr);
				resultosw.flush();

				icol.save(bo);
			}

		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		close_MongoConn();
		//
		after_exec();

		basesql = " select playerid, count(playerid) from par_dbname.tab_town_leave par_datetime in (par_partition) and recordtime>='par_startdate' and recordtime<'par_stopdate' and playerid in () ";

	}
}
