package com.test.numbers;

import java.io.IOException;

import org.apache.commons.math3.random.RandomData;
import org.apache.commons.math3.random.RandomDataImpl;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class Test {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3014716914084285225L;
	private static final int MIN_INDEX = 19968;
	private static final int MAX_INDEX = 40869;
	private static final String CR = "\r\n";
	private static final String TAB = "\t";

	public static void main(String[] args) throws Exception {
		test1();

		System.out.println("====success====");
	}

	@SuppressWarnings("deprecation")
	public static void test1() throws IOException {

		// Get a DescriptiveStatistics instance
		DescriptiveStatistics stats = new DescriptiveStatistics();
		RandomData randomData = new RandomDataImpl();

		// Add the data from the array
		for (int i = 0; i < 100; i++) {
			stats.addValue(randomData.nextInt(1, 10000));
		}

		// Compute some statistics
		double mean = stats.getMean();
		double std = stats.getStandardDeviation();
		double median = stats.getPercentile(50);

		System.out.println("mean " + mean);
		System.out.println("std " + std);
		System.out.println("median " + median);
	}

}
