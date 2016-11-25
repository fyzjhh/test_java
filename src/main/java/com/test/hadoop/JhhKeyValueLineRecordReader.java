package com.test.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

public class JhhKeyValueLineRecordReader implements
		RecordReader<Text, LongWritable> {

	private KeyValueLineRecordReader lineReader;
	private Text lineKey;
	private LongWritable lineValue;

	public JhhKeyValueLineRecordReader(Configuration job, FileSplit split)
			throws IOException {
		lineReader = new KeyValueLineRecordReader(job, split);
		lineKey = lineReader.createKey();
		String s = lineReader.createValue().toString();
		if (s == null || "".equals(s))
			s = "0";
		lineValue = new LongWritable(Long.valueOf(s));
	}

	@Override
	public boolean next(Text key, LongWritable value) throws IOException {
		// TODO Auto-generated method stub
		Text tmpv = new Text("0");
		if (!lineReader.next(lineKey, tmpv)) {
			return false;
		}

		key.set(lineKey);
		value.set(Long.valueOf(tmpv.toString()));
		return true;
	}

	public Text createKey() {
		// TODO Auto-generated method stub
		return new Text("0");
	}

	@Override
	public LongWritable createValue() {
		// TODO Auto-generated method stub
		return new LongWritable(0);
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return lineReader.getPos();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		lineReader.close();
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return lineReader.getProgress();
	}

}
