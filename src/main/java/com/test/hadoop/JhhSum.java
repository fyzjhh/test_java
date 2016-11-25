package com.test.hadoop;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JhhSum<K, V> extends Configured implements Tool {
	private RunningJob jobResult = null;

	@SuppressWarnings({ "rawtypes" })
	public int run(String[] args) throws Exception {

		JobConf jobConf = new JobConf(getConf(), JhhSum.class);
		jobConf.setJobName("sum");
		jobConf.set("mapred.job.tracker", "192.168.12.200:9001");
		jobConf.set("fs.default.name", "hdfs://192.168.12.200:9000");
		jobConf.setMapperClass(IdentityMapper.class);
		jobConf.setReducerClass(LongSumReducer.class);
		
		JobClient client = new JobClient(jobConf);
		ClusterStatus cluster = client.getClusterStatus();
		int num_reduces = (int) (cluster.getMaxReduceTasks() * 0.5);
		String sort_reduces = jobConf.get("test.sort.reduces_per_host");
		if (sort_reduces != null) {
			num_reduces = cluster.getTaskTrackers()
					* Integer.parseInt(sort_reduces);
		}
		Class<? extends InputFormat> inputFormatClass = JhhInputFormat.class;
		Class<? extends OutputFormat> outputFormatClass = TextOutputFormat.class;
		Class<? extends WritableComparable> outputKeyClass = Text.class;
		Class<? extends Writable> outputValueClass = LongWritable.class;
		List<String> otherArgs = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			otherArgs.add(args[i]);
		}

		// Set user-supplied (possibly default) job configs
		jobConf.setNumReduceTasks(num_reduces);

		jobConf.setInputFormat(inputFormatClass);
		jobConf.setOutputFormat(outputFormatClass);

		jobConf.setOutputKeyClass(outputKeyClass);
		jobConf.setOutputValueClass(outputValueClass);

		if (otherArgs.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ otherArgs.size() + " instead of 2.");
		}
		FileInputFormat.setInputPaths(jobConf, otherArgs.get(0));
		FileOutputFormat.setOutputPath(jobConf, new Path(otherArgs.get(1)));

		System.out.println("Running on " + cluster.getTaskTrackers()
				+ " nodes to sort from "
				+ FileInputFormat.getInputPaths(jobConf)[0] + " into "
				+ FileOutputFormat.getOutputPath(jobConf) + " with "
				+ num_reduces + " reduces.");
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		jobResult = JobClient.runJob(jobConf);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		return 0;
	}

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {
		String teststr = "/tmp/a.txt /tmp/r-a";
		args = teststr.split(" +");
		int res = ToolRunner.run(new Configuration(), new JhhSum(), args);
		System.exit(res);
	}

	/**
	 * Get the last job that was run using this instance.
	 * 
	 * @return the results of the last job that was run
	 */
	public RunningJob getResult() {
		return jobResult;
	}
}
