package com.test.dianhun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class Util_Excel extends Tools {

	static String workDir = "D:/temp/";
	static String action_day = null;

	public static void main(String[] args) throws Exception {

		// for (int m = 13; m <= 13; m++) {
		// String teststr = "-w D:\\temp\\resultdir\\ -a 2013-08-" + m;
		// args = teststr.split(" +");
		// System.out.println("deal " + teststr);
		// runOut(args);
		// }

		// String dir = "D:\\temp\\x_0306_";
		// String xlsxfile = "D:\\temp\\查询用户id.xlsx";
		// String filematchstr = ".*";
		// read1(xlsxfile, filematchstr, dir);

		String dir = "D:\\temp\\";
		String xlsxfile = "D:\\temp\\统计结果\\统计结果(jianghehui_2014-04-16_广告需求201312).xlsx";
		String filematchstr = "12_.*\\.txt";
		write1(dir, xlsxfile, filematchstr);
		System.out.println("====success====");
	}

	public static void runOut(String[] args) throws Exception {
		Options opts = new Options();
		opts.addOption("h", "help", false, "");
		opts.addOption("w", "workDir", true, "");
		opts.addOption("a", "action_day", true, "");
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
				action_day = cl.getOptionValue("a");

				write_excel();
			}
		} else {
			System.out.println("-w D:\\temp\\stats\\ -a  2013-07-20 ");
		}

	}

	public static void write1(String dir, String xlsxfile, String filematchstr)
			throws Exception {

		step = "generate excel file ";
		logfile = workDir + step + log_suffix;
		openLogFile();

		logstr = start_str + step;
		printLogStr(logstr);

		Workbook wb = new XSSFWorkbook();

		Sheet sheet = null;
		String[] result_files = null;

		File parentF = new File(dir);
		result_files = parentF.list();

		int row_num = 0;

		for (int i = 0; i < result_files.length; i++) {
			String srcfile = result_files[i].trim();
			if (srcfile.matches(filematchstr) == false) {
				logstr = i + " skip file " + srcfile;
				printLogStr(logstr);
				continue;
			}
			logstr = i + " deal file " + srcfile;
			printLogStr(logstr);

			// String shname = srcfile.replaceAll("kg", EMPTY)
			// .replaceAll("\\.result", EMPTY).replaceAll("lz", EMPTY)
			// .replaceAll("rz", EMPTY);
			String shname = srcfile;
			// String[] shnames = srcfile.split(UNDERLINE);
			// String shname = repfilename(shnames[1] + shnames[3]);
			sheet = wb.createSheet(shname);
			row_num = 0;
			File fdf = new File(dir + srcfile);
			String tls = null;
			BufferedReader rf = new BufferedReader(new FileReader(fdf));

			while ((tls = rf.readLine()) != null) {
				String[] colvals = tls.split(TAB);
				int colnums = colvals.length;
				String val = EMPTY;
				Row row = null;
				Cell cell = null;
				row_num++;
				row = sheet.createRow(row_num);
				for (int j = 0; j < colnums; j++) {
					val = colvals[j];
					cell = row.createCell(j);
					cell.setCellValue(val);
				}
			}
			rf.close();
			ajustColSize(sheet);

		}

		try {
			logstr = start_str + "write excel file";
			printLogStr(logstr);
			File file = new File(xlsxfile);
			FileOutputStream fileOut = new FileOutputStream(file);
			wb.write(fileOut);
			fileOut.close();
			logstr = stop_str + "write excel file";
			printLogStr(logstr);
		} catch (Exception e) {
			logstr = "generate excel file error";
			printLogStr(logstr);
			e.printStackTrace();
		}

		logstr = stop_str + step;
		printLogStr(logstr);

		closeLogFile();
	}

	public static void read1(String excelFile, String filematchstr, String dir)
			throws Exception {
		DecimalFormat df = new DecimalFormat("0");
		DecimalFormat nf = new DecimalFormat("0");
		step = "read excel to txt, one worksheet one txt";
		logfile = workDir + step + log_suffix;
		openLogFile();

		logstr = start_str + step;
		printLogStr(logstr);

		InputStream is = new FileInputStream(excelFile);
		Workbook wb = new XSSFWorkbook(is);
		Sheet sheet = null;
		String sheetName = null;
		for (int i = 0; i < wb.getNumberOfSheets(); i++) {

			sheet = wb.getSheetAt(i);
			sheetName = sheet.getSheetName();

			if (sheetName.matches(filematchstr) == false) {
				logstr = i + " skip sheet " + sheetName;
				printLogStr(logstr);
				continue;
			}
			logstr = i + " deal sheet " + sheetName;
			printLogStr(logstr);
			Row row = null;
			Cell cell = null;
			String value = EMPTY;
			FileOutputStream fos = new FileOutputStream(dir + sheetName, false);
			OutputStreamWriter tmposw = new OutputStreamWriter(fos, "UTF8");

			for (int j = 0; j < sheet.getPhysicalNumberOfRows(); j++) {
				row = sheet.getRow(j);
				int cell_cnt = row.getPhysicalNumberOfCells();
				for (int k = 0; k < cell_cnt; k++) {
					cell = row.getCell(k);
					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_NUMERIC:
						CellStyle cs = cell.getCellStyle();
						String csStr = cs.getDataFormatString();
						double numval = cell.getNumericCellValue();
						if ("@".equals(csStr)) {
							value = df.format(numval);
						} else if ("General".equals(csStr)) {
							value = nf.format(numval);
						}
						break;
					case Cell.CELL_TYPE_STRING:
						value = cell.getStringCellValue();
						break;
					case Cell.CELL_TYPE_FORMULA:
						if (!cell.getStringCellValue().equals("")) {
							value = cell.getStringCellValue();
						} else {
							value = cell.getNumericCellValue() + "";
						}
						break;
					case Cell.CELL_TYPE_BLANK:
						value = EMPTY;
						break;

					default:
						value = cell.toString();
					}
					if (k == cell_cnt - 1) {
						tmposw.write(value);
					} else {
						tmposw.write(value + TAB);
					}
					tmposw.write(NEWLINE);
				}
			}
			tmposw.close();
		}

		logstr = stop_str + step;
		printLogStr(logstr);

		closeLogFile();
	}

	private static void ajustColSize(Sheet sheet) {
		for (int i = 0; i < 10; i++) {
			sheet.autoSizeColumn((short) i);
		}
	}

	public static void write_excel() throws IOException {
		Workbook wb = new XSSFWorkbook();
		String xlsxfile = "媒体统计结果-" + action_day + ".xlsx";
		Sheet sheet = null;
		String[] result_files = null;
		int row_num = 0;
		Row headrow = null;

		result_files = new String[] { "userlevel.result" };
		sheet = wb.createSheet("userlevel");
		row_num = 1;

		headrow = sheet.createRow(0);
		headrow.createCell(0).setCellValue("行为日期");
		headrow.createCell(1).setCellValue("用户级别");
		headrow.createCell(2).setCellValue("值");

		for (int i = 0; i < result_files.length; i++) {
			logstr = result_files[i];
			logstr = workDir + action_day + "/" + logstr;

			File fdf = new File(logstr);
			String tls = null;
			BufferedReader rf = new BufferedReader(new FileReader(fdf));

			while ((tls = rf.readLine()) != null) {
				String action_day = tls.split(TAB)[0];
				String userlevel = tls.split(TAB)[1];
				String result_value = tls.split(TAB)[2];
				Row row = sheet.createRow(row_num);
				row_num++;
				Cell c0 = row.createCell(0);
				Cell c1 = row.createCell(1);
				Cell c2 = row.createCell(2);
				c0.setCellValue(action_day);
				c1.setCellValue(userlevel);
				c2.setCellValue(result_value);
			}
			rf.close();
		}

		ajustColSize(sheet);

		result_files = new String[] { "register_parts.result" };
		sheet = wb.createSheet("register_parts");

		row_num = 1;
		headrow = sheet.createRow(0);
		headrow.createCell(0).setCellValue("广告id");
		headrow.createCell(1).setCellValue("行为日期");
		headrow.createCell(2).setCellValue("注册日期");
		headrow.createCell(3).setCellValue("时间范围");
		headrow.createCell(4).setCellValue("值");
		for (int i = 0; i < result_files.length; i++) {
			logstr = result_files[i].trim();
			logstr = workDir + action_day + "/" + logstr;

			File fdf = new File(logstr);
			String tls = null;
			BufferedReader rf = new BufferedReader(new FileReader(fdf));

			while ((tls = rf.readLine()) != null) {
				String ad_id = tls.split(TAB)[0];
				String action_day = tls.split(TAB)[1];
				String register_day = tls.split(TAB)[2];
				String timerange = tls.split(TAB)[3];
				String result_value = tls.split(TAB)[4];
				Row row = sheet.createRow(row_num);
				row_num++;
				Cell c0 = row.createCell(0);
				Cell c1 = row.createCell(1);
				Cell c2 = row.createCell(2);
				Cell c3 = row.createCell(3);
				Cell c4 = row.createCell(4);
				c0.setCellValue(ad_id);
				c1.setCellValue(action_day);
				c2.setCellValue(register_day);
				c3.setCellValue(timerange);
				c4.setCellValue(result_value);
			}
			rf.close();
		}
		ajustColSize(sheet);

		result_files = new String[] { "register_hours.result",
				"login_all_hours.result", "login_distinct_hours.result",
				"char_create_hours.result" };
		sheet = wb.createSheet("hours_action");

		row_num = 1;
		headrow = sheet.createRow(0);
		headrow.createCell(0).setCellValue("广告id");
		headrow.createCell(1).setCellValue("用户行为");
		headrow.createCell(2).setCellValue("行为日期");
		headrow.createCell(3).setCellValue("时间范围");
		headrow.createCell(4).setCellValue("注册日期");
		headrow.createCell(5).setCellValue("值");
		for (int i = 0; i < result_files.length; i++) {
			logstr = result_files[i].trim();
			logstr = workDir + action_day + "/" + logstr;

			File fdf = new File(logstr);
			String tls = null;
			BufferedReader rf = new BufferedReader(new FileReader(fdf));

			while ((tls = rf.readLine()) != null) {
				String ad_id = tls.split(TAB)[0];
				String action_desc = tls.split(TAB)[1];
				String action_day = tls.split(TAB)[2];
				String timerange = tls.split(TAB)[3];
				String register_day = tls.split(TAB)[4];
				String result_value = tls.split(TAB)[5];
				Row row = sheet.createRow(row_num);
				row_num++;
				Cell c0 = row.createCell(0);
				Cell c1 = row.createCell(1);
				Cell c2 = row.createCell(2);
				Cell c3 = row.createCell(3);
				Cell c4 = row.createCell(4);
				Cell c5 = row.createCell(5);
				c0.setCellValue(ad_id);
				c1.setCellValue(action_desc);
				c2.setCellValue(action_day);
				c3.setCellValue(timerange);
				c4.setCellValue(register_day);
				c5.setCellValue(result_value);
			}
			rf.close();
		}
		ajustColSize(sheet);
		sheet.setColumnWidth(1, 8000);

		result_files = new String[] { "buyitem_all.result",
				"buyitem_distinct.result", "guanka_all.result",
				"guanka_distinct.result", "item_all.result",
				"itemchange.result", "item_distinct.result",
				"itemprice.result", "login_all.result",
				"login_distinct.result", "pay_all.result",
				"pay_distinct.result", "race_all.result",
				"race_distinct.result", "useitem_all.result",
				"useitem_distinct.result", };
		sheet = wb.createSheet("action");

		row_num = 1;
		headrow = sheet.createRow(0);
		headrow.createCell(0).setCellValue("广告id");
		headrow.createCell(1).setCellValue("用户行为");
		headrow.createCell(2).setCellValue("行为日期");
		headrow.createCell(3).setCellValue("注册日期");
		headrow.createCell(4).setCellValue("值");
		for (int i = 0; i < result_files.length; i++) {
			logstr = result_files[i].trim();
			logstr = workDir + action_day + "/" + logstr;

			File fdf = new File(logstr);
			String tls = null;
			BufferedReader rf = new BufferedReader(new FileReader(fdf));
			while ((tls = rf.readLine()) != null) {
				String ad_id = tls.split(TAB)[0];
				String action_desc = tls.split(TAB)[1];
				String action_day = tls.split(TAB)[2];
				String register_day = tls.split(TAB)[3];
				String result_value = tls.split(TAB)[4];
				Row row = sheet.createRow(row_num);
				row_num++;
				Cell c0 = row.createCell(0);
				Cell c1 = row.createCell(1);
				Cell c2 = row.createCell(2);
				Cell c3 = row.createCell(3);
				Cell c4 = row.createCell(4);
				c0.setCellValue(ad_id);
				c1.setCellValue(action_desc);
				c2.setCellValue(action_day);
				c3.setCellValue(register_day);
				c4.setCellValue(result_value);
			}
			rf.close();
		}
		ajustColSize(sheet);
		sheet.setColumnWidth(1, 8000);

		try {
			File file = new File(workDir + xlsxfile);
			FileOutputStream fileOut = new FileOutputStream(file);
			wb.write(fileOut);
			fileOut.close();
		} catch (Exception e) {
			System.out.println("generate excel file error");
			e.printStackTrace();
		}
	}
}
