package com.test.dianhun;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

public class Hadoop extends Tools implements DianHunSql {

	public Hadoop() {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void execSql() {
		try {
			stmt = hiveConn.createStatement();
			stmt.execute(sqlstr);
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}
	}

	public static String cal_partitionstr(String start_date, String stop_date)
			throws Exception {

		if (start_date.compareTo(stop_date) > 0) {
			return "'2000-01-01'";
		}
		if (start_date.length()>10 ){
			stop_date = start_date.substring(0,10);
		}
		if (stop_date.length()>10 ){
			stop_date = stop_date.substring(0,10);
		}
		String ret = EMPTY;
		Calendar cal = Calendar.getInstance();
		Date tmpdate = null;
		tmpdate = dateformat.parse(start_date);
		cal.setTime(tmpdate);
		tmpdate = cal.getTime();
		String a = partitiondatetimeformat.format(tmpdate);

		tmpdate = dateformat.parse(stop_date);
		cal.setTime(tmpdate);
		tmpdate = cal.getTime();
		String b = partitiondatetimeformat.format(tmpdate);

		String tmp = a;
		while (tmp.compareTo(b) <= 0) {
			ret = ret + S_QUOTE + tmp + S_QUOTE + COMMA;
			cal = Calendar.getInstance();
			tmpdate = partitiondatetimeformat.parse(tmp);
			cal.setTime(tmpdate);
			cal.add(Calendar.MONTH, 1);
			tmpdate = cal.getTime();
			tmp = partitiondatetimeformat.format(tmpdate);
		}
		if (ret.endsWith(COMMA)) {
			ret = ret.substring(0, ret.length() - 1);
		}
		return ret;
	}

	public static void drop_tmp_table(String t) throws Exception {

		String sql = "DROP TABLE IF EXISTS " + t;
		logstr = "conn=" + hivehostportstr + ",sql=" + sql;
		printLogStr(logstr);

		stmt = hiveConn.createStatement();
		stmt.execute(sql);

	}

	public static void create_tmp_table(String t) throws SQLException {

		String sql = EMPTY;
		if (t.matches("test.tmp_userid.*")) {
			sql = "CREATE TABLE if not exists " + t + " ( iuin int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ";

		}
		if (t.matches("test.tmp_useronedata.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " (iuin int , value1 int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_useronestr1.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " (iuin int , value1 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_usertwodata.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " (iuin int , value1 int, value2 int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_usertwostr1.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " (iuin int , value1 int, value2 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_usertwostr2.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " (iuin int , value1 string, value2 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_userthreedata.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " (iuin int , value1 int, value2 int,value3 int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_userthreestr1.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " (iuin int , value1 int, value2 int,value3 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_userfourstr1.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " (iuin int , value1 int, value2 int, value3 int , value4 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_userthreestr3.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " (iuin int , value1 string, value2 string, value3 string  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_strone.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " ( value1 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_strtwo.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " ( value1 string , value2 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_strthree.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " ( value1 string , value2 string , value3 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		if (t.matches("test.tmp_strfour.*")) {
			sql = "CREATE TABLE if not exists " + t
					+ " ( value1 string , value2 string , value3 string , value4 string ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";

		}
		
		logstr = "conn=" + hivehostportstr + ",sql=" + sql;
		printLogStr(logstr);

		stmt = hiveConn.createStatement();
		stmt.execute(sql);

	}

	public static void after_exec() throws Exception {
		close_HiveConn();

		closeResultFile();

		logstr = stop_str + step;
		printLogStr(logstr);
		closeLogFile();
	}

	public static void before_exec() throws Exception {
		logfile = workDir + step + log_suffix;
		openLogFile();
		resultfile = workDir + step + result_suffix;
		openResultFile();

		logstr = start_str + step;
		printLogStr(logstr);

		open_HiveConn();
	}

}
