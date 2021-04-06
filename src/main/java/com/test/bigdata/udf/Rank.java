package com.test.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Rank.
 * 
 */
@Description(name = "rank", value = "_FUNC_(str) - returns count of the key scaned", extended = "Example:\nselect id, rank(id) csum, score ,record_time from \n( select id, score, record_time from c distribute by id sort by id,score desc ,record_time desc limit 100) b;")
public class Rank extends UDF {

	private int counter;
	private long long_last_key;
	private int int_last_key;

	public int evaluate(final int k) {
		if (k != this.int_last_key) {
			this.counter = 0;
			this.int_last_key = k;
		}
		return this.counter++;

	}

	public int evaluate(final long k) {
		if (k != this.long_last_key) {
			this.counter = 0;
			this.long_last_key = k;
		}
		return this.counter++;
	}

}
