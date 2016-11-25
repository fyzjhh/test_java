package com.test.dianhun;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util_Doc extends Tools {
	static String dir = "D:\\downloads\\eclipse-SDK-3.8.2-win32\\workspace\\test_svn\\src\\com\\dianhun\\";
	static String destdir = "D:\\temp\\javadocs\\";
	static String[] srcFiles = new String[] { "Hadoop_Gm", "Hadoop_Oss" };

	static String header = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">  <html lang=\"zh\"> <head>  <meta http-equiv=\"Content-Type\" content=\"text/html\" charset=\"utf-8\"> <title>javadoc</title> <link rel=\"stylesheet\" type=\"text/css\" href=\"./stylesheet.css\" title=\"Style\"> </head>  <body>    ";
	static String tailer = "</body></html>";

	static String detail_header = "<ul class=\"blockList\"> <li class=\"blockList\"> <a name=\"method_detail\"> </a> <h3>详细资料</h3>";
	static String detail_tailer = "</li></ul>";
	static String detail_template = "<a name=\"statnameStr\"></a> <ul class=\"blockList\">  <li class=\"blockList\"> <h4>statnameStr</h4> <div class=\"block\"> docStr </div> </li>  </ul>";

	static String nav_header = "<ul class=\"blockList\"> <li class=\"blockList\"> <a name=\"method_detail\"> </a> <h3>概要</h3>";
	static String nav_template = "<ul class=\"blockList\">  <li class=\"blockList\"><a href=\"#statnameStr\">statnameStr</a>  </li>  </ul> ";
	static String nav_tailer = "</li></ul>";

	public static void main(String[] args) throws Exception {
		gen_md();
		gen_html();
	}

	private static void gen_md() throws FileNotFoundException, IOException {

		String fileName = null;
		BufferedReader reader = null;
		String tempString = null;
		String methodEx = ".*public static void (get.*)\\(\\).*";
		String startEx = "[\t ]*/\\*\\*[\t ]*";
		String stopEx = "[\t ]* \\*/[\t ]*";
		String docEx = "[\t ]* \\* .*";
		String dealEx = "<li>([^<>]*)</li>[\t ]*<p>([^<>]*)</p>";
		String nameEx = ".*(项目名称)</li>[\t ]*<p>([^<>]*)</p>.*";
		String emptyEx = "[\t ]*";
		Pattern deal_p = Pattern.compile(dealEx);
		Pattern method_p = Pattern.compile(methodEx);
		Pattern name_p = Pattern.compile(nameEx);
		BufferedWriter output = null;

		for (int i = 0; i < srcFiles.length; i++) {

			fileName = dir + srcFiles[i] + ".java";
			System.out.println("gen_doc for " + fileName);

			reader = new BufferedReader(new FileReader(fileName));

			String methodStr = EMPTY;
			String statnameStr = EMPTY;
			String docStr = EMPTY;
			while ((tempString = reader.readLine()) != null) {

				Matcher method_m = method_p.matcher(tempString);
				if (tempString.matches(startEx)) {
					docStr = EMPTY;
					continue;
				} else if (tempString.matches(stopEx)) {
					docStr = docStr + EMPTY;
					continue;
				} else if (tempString.matches(docEx)) {
					docStr = docStr + tempString.replaceAll(" \\* ", EMPTY);
					continue;
				} else if (tempString.matches(emptyEx)) {
					continue;
				} else if (method_m.find()) {
					if (docStr.length() > 50) {
						methodStr = srcFiles[i] + POINT + method_m.group(1);
						Matcher name_m = name_p.matcher(docStr);
						if (name_m.find()) {
							statnameStr = name_m.group(2).trim();
						} else {
							statnameStr = EMPTY;
						}

						output = new BufferedWriter(new FileWriter(destdir
								+ statnameStr + ".md"));
						Matcher deal_m = deal_p.matcher(docStr);
						resultstr = EMPTY;
						while (deal_m.find()) {
							String name = deal_m.group(1).trim();
							String value = deal_m.group(2).trim();

							resultstr = resultstr + "### " + name + NEWLINE
									+ value + NEWLINE;
						}
						resultstr = resultstr + "### 代码位置" + NEWLINE
								+ methodStr + NEWLINE;
						output.write(resultstr + NEWLINE);
						output.close();
					}
				} else {
					docStr = EMPTY;
				}
			}

			reader.close();
		}

	}

	private static void gen_html() throws FileNotFoundException, IOException {

		String fileName = null;
		String destHtml = null;
		BufferedReader reader = null;
		String tempString = null;
		String methodEx = ".*public static void (get.*)\\(\\).*";
		String startEx = "[\t ]*/\\*\\*[\t ]*";
		String stopEx = "[\t ]* \\*/[\t ]*";
		String docEx = "[\t ]* \\* .*";
		String nameEx = ".*(项目名称)</li>[\t ]*<p>([^<>]*)</p>.*";
		String emptyEx = "[\t ]*";
		Pattern method_p = Pattern.compile(methodEx);
		Pattern name_p = Pattern.compile(nameEx);
		BufferedWriter output = null;

		reader = new BufferedReader(
				new FileReader(
						"D:\\dianhun\\jianghehui\\dianhun\\src\\resource\\stylesheet.css"));
		output = new BufferedWriter(new FileWriter(destdir + "stylesheet.css"));
		while ((tempString = reader.readLine()) != null) {
			output.write(tempString + NEWLINE);
		}

		output.close();
		reader.close();

		destHtml = destdir + "index.html";
		output = new BufferedWriter(new FileWriter(destHtml));
		output.write(header + NEWLINE);
		String tab_str = EMPTY;
		String detail_str = EMPTY;

		for (int i = 0; i < srcFiles.length; i++) {

			fileName = dir + srcFiles[i] + ".java";
			System.out.println("gen_doc for " + fileName + TAB + destHtml);

			reader = new BufferedReader(new FileReader(fileName));

			String methodStr = EMPTY;
			String statnameStr = EMPTY;
			String docStr = EMPTY;
			while ((tempString = reader.readLine()) != null) {

				Matcher method_m = method_p.matcher(tempString);
				if (tempString.matches(startEx)) {
					docStr = EMPTY;
					continue;
				} else if (tempString.matches(stopEx)) {
					docStr = docStr + EMPTY;
					continue;
				} else if (tempString.matches(docEx)) {
					docStr = docStr + tempString.replaceAll(" \\* ", EMPTY);
					continue;
				} else if (tempString.matches(emptyEx)) {
					continue;
				} else if (method_m.find()) {
					if (docStr.length() > 50) {
						methodStr = srcFiles[i] + POINT + method_m.group(1);
						Matcher name_m = name_p.matcher(docStr);
						if (name_m.find()) {
							statnameStr = name_m.group(2).trim();
						} else {
							statnameStr = EMPTY;
						}
						docStr = docStr.replace("</ul>", "<li>代码位置</li><p>"
								+ methodStr + "  </p> </ul>");
						;
						tab_str = tab_str
								+ nav_template
										.replaceAll("statnameStr", statnameStr)
										.replaceAll("methodStr", methodStr)
										.replaceAll("docStr", docStr);
						detail_str = detail_str
								+ detail_template
										.replaceAll("statnameStr", statnameStr)
										.replaceAll("methodStr", methodStr)
										.replaceAll("docStr", docStr);
					}
				} else {
					methodStr = EMPTY;
					docStr = EMPTY;
				}
			}

			reader.close();
		}
		output.write(nav_header + NEWLINE);
		output.write(tab_str + NEWLINE);
		output.write(nav_tailer + NEWLINE);

		output.write(detail_header + NEWLINE);
		output.write(detail_str);
		output.write(detail_tailer + NEWLINE);
		output.write(tailer + NEWLINE);
		output.close();
	}
}
