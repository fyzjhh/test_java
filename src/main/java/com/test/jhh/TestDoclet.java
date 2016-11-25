package com.test.jhh;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestDoclet {

	public static void main(String[] args) throws Exception {

		String[] srcFiles = new String[] {};
		File file = null;
		BufferedReader reader = null;
		String tempString = null;

		for (int i = 0; i < srcFiles.length; i++) {
			String fileName = srcFiles[i];
			file = new File(fileName);
			reader = new BufferedReader(new FileReader(file));
			String docStr = "";
			String methodStr = "";
			while ((tempString = reader.readLine()) != null) {

				if (tempString.matches("[\t ]*/\\*\\*[\t ]*")) {
					docStr = "" + tempString;
				} else if (tempString.matches("[\t ]*\\*/[\t ]*")) {
					docStr = docStr + tempString;
				} else {
					docStr = docStr + tempString;
				}

				String regEx = ".*public static void (get.*)\\(\\).*";
				Pattern p = Pattern.compile(regEx);
				Matcher m = p.matcher(tempString);
				if (m.find()) {
					methodStr = m.group(1);
				}
				
				System.out.println(methodStr+"\r\n"+docStr);
			}
			reader.close();
		}

		System.out.println("====success====");
	}

}
