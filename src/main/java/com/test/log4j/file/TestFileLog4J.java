package com.test.log4j.file;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestFileLog4J implements Serializable {
	// static Logger logger = Logger.getLogger(TestFileLog4J.class);
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
		logger.info("file info mesg;");
		logger.warn("file warn mesg;");
		logger.error("file error mesg;");
	}
}