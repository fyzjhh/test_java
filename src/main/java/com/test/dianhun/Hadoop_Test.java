package com.test.dianhun;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;

public class Hadoop_Test extends Hadoop {
	String workdir = "/data/tmp/resultdir";
	String action_date = EMPTY;
	String teststr = null;
	String[] args = null;

	public Hadoop_Test() {

		super();

		action_date = dateformat.format(new Date());
		try {
			action_date = addDay(action_date, -1);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// action_date = "2014-04-20";
	}

	public static void test_exec() throws Exception {
		before_exec();
		sqlstr = "select distinct iuin from  caochuan_2.tab_login  where  par_datetime>=201403 and par_datetime<=201404 and dtlogtime>='2014-03-29 00:00:00' and dtlogtime<='2014-04-04 23:59:59' ";

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

	public static void test_udf_iuinrank() throws Exception {
		before_exec();

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function rank as 'com.udf.Rank'");

		sqlstr = "select id, rank(id) csum, record_time from ( select id, record_time from chibi_1.tab_town_login where par_datetime in ('201403') distribute by id sort by id,record_time desc limit 40) t ;";

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				Integer v1 = rs.getInt(1);
				Integer v2 = rs.getInt(2);
				String v3 = rs.getString(3);
				printLogStr(v1 + TAB + v2 + TAB + v3);
			}
			resultosw.flush();
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();
	}

	public static void test_udf_ipgetter() throws Exception {
		before_exec();

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function ip2str as 'com.udf.IpGetter'");

		sqlstr = "select iuin,vclientip,ip2str(vclientip) from caochuan_2.tab_login where par_datetime in ('201401') limit 10";

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				Integer v1 = rs.getInt(1);
				String v2 = rs.getString(2);
				String v3 = rs.getString(3);
				printLogStr(v1 + TAB + v2 + TAB + v3);
			}
			resultosw.flush();
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();
	}

	public static void test_udf_converter() throws Exception {
		before_exec();

		stmt = hiveConn.createStatement();
		stmt.execute("add jar lib/udf-1.0.jar");
		stmt.execute("create temporary function long2str as 'com.udf.Converter_long2str'");
		stmt.execute("create temporary function str2long as 'com.udf.Converter_str2long'");
		sqlstr = "select str2long('U065') , long2str(1429222965)  from caochuan_2.accountLog where par_datetime in ('201401') limit 2";

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {

			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);

			while (rs.next()) {
				Long v1 = rs.getLong(1);
				String v2 = rs.getString(2);
				printLogStr(v1 + TAB + v2);
			}
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();
	}

	public void test_before_exec() throws Exception {
		before_exec();
	}
}
