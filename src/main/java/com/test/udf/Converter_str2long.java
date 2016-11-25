package com.test.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Rank.
 * 
 */
@Description(name = "str2long", value = "_FUNC_(str) - returns count of the key str scaned", extended = "Example:\nselect id, rank(id) csum, score ,record_time from \n( select id, score, record_time from c distribute by id sort by id,score desc ,record_time desc limit 100) b;")
public class Converter_str2long extends UDF {

	public static long evaluate(String str) {

		byte[] bytes = str.getBytes();
		long value = 0;
		// 由高位到低位
		for (int i = 0; i < 4; i++) {
			int shift = (4 - 1 - i) * 8;
			value += (bytes[i] & 0x000000FF) << shift;// 往高位游
		}
		return value;
	}

}
