package com.test.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Rank.
 * 
 */
@Description(name = "long2str", value = "_FUNC_(str) - returns count of the key str scaned", extended = "Example:\nselect id, rank(id) csum, score ,record_time from \n( select id, score, record_time from c distribute by id sort by id,score desc ,record_time desc limit 100) b;")
public class Converter_long2str extends UDF {

	public static String evaluate(long t) {

		char[] result = new char[4];
		result[0] = (char) ((t >> 24) & 0xFF);
		result[1] = (char) ((t >> 16) & 0xFF);
		result[2] = (char) ((t >> 8) & 0xFF);
		result[3] = (char) (t & 0xFF);
		String v = String.valueOf(result);
		return v;
	}

}
