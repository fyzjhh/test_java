package com.test.lucene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

/**
 * ´´½¨Ë÷Òý Lucene 3.0+
 * 
 * @author Administrator
 * 
 */
public class Test {

	/**
	 * @param args
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		test();
	}

	public static void test() throws Exception {
		byte[] bytes = { 0x00,0x00,0x00,0x00,0xA9-256,0xE6-256,0xAE-256,0x4F };

		int retLow = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16)
				| ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);


		System.out.println(retLow);

		int retHigh = ((bytes[4] & 0xFF) << 24) | ((bytes[5] & 0xFF) << 16)
		| ((bytes[6] & 0xFF) << 8) | (bytes[7] & 0xFF);
		long retLong = (((long) retLow) << 32) | (retHigh & 0xFFFFFFFFL);
		System.out.println(retLong);
	}

}