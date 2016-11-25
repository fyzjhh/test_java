package com.test.log4j.console;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.test.log4j.file.TestFileLog4J;

public class TestConsoleLog4J implements Serializable {
	// static Logger logger = Logger.getLogger(TestConsoleLog4J.class);
	static Log logger = LogFactory.getLog(TestFileLog4J.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 3014716914084285225L;
	private static final int MIN_INDEX = 19968;
	private static final int MAX_INDEX = 40869;
	private static final String CR = "\r\n";
	private static final String TAB = "\t";

	public static void main(String[] args) throws Exception {

		test();

		System.out.println();
		System.out.println("====success====");
	}

	private static void test() {
		logger.info("console info mesg;");
		logger.warn("console warn mesg;");
		logger.error("console error mesg;");
	}
}