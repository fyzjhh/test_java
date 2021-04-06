package com.test.udf.ipseek;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.ipseek.IPLocation;
import com.ipseek.IPSeeker;

/**
 * IpGetter.
 * 
 */
@Description(name = "ip2str", value = "_FUNC_(str) - returns location of the ip", extended = "Example:\nselect id,ip2str(ip) from t limit 10;")
public class IpGetter extends UDF {
	public static String default_ipdatfile = "/data/app/mysql/stats/resource/CoralWry_lite.dat";
	public static IPSeeker ipseeker = new IPSeeker(default_ipdatfile);

	public IpGetter() {
	}

	public String evaluate(String ip) {
		if (null == ip || ip.length() < 7) {
			return null;
		}
		IPLocation ret = ipseeker.getIPLocation(ip);
		return (ret.getCountry() + "," + ret.getArea());

	}
}
