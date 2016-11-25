package com.test.dianhun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

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
public class Hadoop_Platform extends Hadoop implements DianHunSql {

	public static void main(String[] args) throws Exception {
		runOut(args);
	}

	public static void get_product_register_count() throws Exception {

		before_exec();

		String reg_start_date = action_date;
		String reg_stop_date = action_date;
		String final_reg_stop_date = addDay(reg_stop_date, 1);

		String reg_par_partition = cal_partitionstr(reg_start_date,
				reg_stop_date);

		int type_nature = 0;
		int type_ad = 1;
		int type_plat = 2;

		sqlstr = "select t1.product_id  , count(distinct t2.iuin) from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on (t1.ad_id=t2.media_id and t2.par_datetime in (reg_par_partition) and t2.dtlogtime>='reg_start_date' and t2.dtlogtime<'final_reg_stop_date' and t1.media_id=157 and t2.media_id>0 ) group by t1.product_id ";
		sqlstr = sqlstr.replaceAll("reg_par_partition", reg_par_partition)
				.replaceAll("reg_start_date", reg_start_date)
				.replaceAll("final_reg_stop_date", final_reg_stop_date);

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				int v1 = rs.getInt(1);
				int v2 = rs.getInt(2);
				resultstr = action_date + TAB + type_nature + TAB + v1 + TAB
						+ v2 + NEWLINE;
				resultosw.write(resultstr);
			}
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		sqlstr = "select t1.product_id  , count(distinct t2.iuin) from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on (t1.ad_id=t2.media_id and t2.par_datetime in (reg_par_partition) and t2.dtlogtime>='reg_start_date' and t2.dtlogtime<'final_reg_stop_date' and t1.media_id!=157 and t2.media_id>0 ) group by t1.product_id ";
		sqlstr = sqlstr.replaceAll("reg_par_partition", reg_par_partition)
				.replaceAll("reg_start_date", reg_start_date)
				.replaceAll("final_reg_stop_date", final_reg_stop_date);

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				int v1 = rs.getInt(1);
				int v2 = rs.getInt(2);
				resultstr = action_date + TAB + type_ad + TAB + v1 + TAB + v2
						+ NEWLINE;
				resultosw.write(resultstr);
			}
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		sqlstr = "select t1.product_id  , count(distinct t2.iuin) from gamemeta.m3gcn_ad_id t1 join caochuan_2.accountlog t2 on (t1.ad_id=t2.media_id and t2.par_datetime in (reg_par_partition) and t2.dtlogtime>='reg_start_date' and t2.dtlogtime<'final_reg_stop_date' and t2.media_id<=0 ) group by t1.product_id ";
		sqlstr = sqlstr.replaceAll("reg_par_partition", reg_par_partition)
				.replaceAll("reg_start_date", reg_start_date)
				.replaceAll("final_reg_stop_date", final_reg_stop_date);

		logstr = "conn=" + hivehostportstr + ",sql=" + sqlstr;
		printLogStr(logstr);

		try {
			stmt = hiveConn.createStatement();
			rs = stmt.executeQuery(sqlstr);
			while (rs.next()) {
				int v1 = rs.getInt(1);
				int v2 = rs.getInt(2);
				resultstr = action_date + TAB + type_plat + TAB + v1 + TAB + v2
						+ NEWLINE;
				resultosw.write(resultstr);
			}
		} catch (SQLException e) {
			printLogStr(hivehostportstr + COMMA + e.getMessage() + NEWLINE);
		}

		after_exec();

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

		Class c = Hadoop_Platform.class;
		Method m = c.getMethod(step);
		m.invoke(c);

	}

	public Hadoop_Platform() {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void runOut(String[] args) throws Exception {
		Options opts = new Options();
		opts.addOption("h", "help", false, EMPTY);
		opts.addOption("w", "workDir", true, EMPTY);
		opts.addOption("a", "action_date", true, EMPTY);
		opts.addOption("s", "step", true, EMPTY);
		opts.addOption("i", "ad_ids", true, EMPTY);
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
			HelpFormatter hf = new HelpFormatter();
			hf.printHelp("Options", opts);
		}

	}
}
