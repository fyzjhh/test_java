package com.test.dianhun;


public class Util_Sql extends Tools {


	public static String replace(String sql, String[] args, String[] values)
			throws Exception {

		int arg_len = args.length;
		int value_len = values.length;
		if (arg_len != value_len) {
			return EMPTY;
		}

		String ret = sql;
		for (int i = 0; i < arg_len; i++) {
			String arg = args[i];
			String value = values[i];
			ret = ret.replaceAll(arg, value);
		}
		return ret;
	}
}
