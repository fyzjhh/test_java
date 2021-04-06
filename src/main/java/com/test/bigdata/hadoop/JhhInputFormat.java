package com.test.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Implements the identity function, mapping inputs directly to outputs.
 */
public class JhhInputFormat<K, V> extends FileInputFormat<Text, LongWritable> {

	public RecordReader<Text, LongWritable> getRecordReader(
			InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {

		reporter.setStatus(genericSplit.toString());
		return new JhhKeyValueLineRecordReader(job, (FileSplit) genericSplit);
	}

}
