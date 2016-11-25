package com.test.servyou;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.rmi.ServerException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SimpleTimeZone;
import java.util.TreeMap;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.drds.model.v20150413.DescribeDrdsDBsRequest;
import com.aliyuncs.drds.model.v20150413.DescribeDrdsDBsResponse;
import com.aliyuncs.drds.model.v20150413.DescribeDrdsDBsResponse.Db;
import com.aliyuncs.drds.model.v20150413.DescribeDrdsInstancesRequest;
import com.aliyuncs.drds.model.v20150413.DescribeDrdsInstancesResponse;
import com.aliyuncs.drds.model.v20150413.DescribeRdsListRequest;
import com.aliyuncs.drds.model.v20150413.DescribeRdsListResponse;
import com.aliyuncs.drds.model.v20150413.DescribeRdsListResponse.RdsInstance;
import com.aliyuncs.ecs.model.v20140526.DescribeInstancesRequest;
import com.aliyuncs.ecs.model.v20140526.DescribeInstancesResponse;
import com.aliyuncs.http.FormatType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse;
import com.aliyuncs.utils.Base64Helper;

public class TestAliyunApi {
	static Map<String, String> ecs_region_map = new HashMap<String, String>();
	static Map<String, String> drds_region_map = new HashMap<String, String>();
	static Map<String, String> rds_region_map = new HashMap<String, String>();
	static List<DescribeInstancesResponse.Instance> ecs_list = new ArrayList<DescribeInstancesResponse.Instance>();
	static List<DescribeDrdsInstancesResponse.Instance> drds_list = new ArrayList<DescribeDrdsInstancesResponse.Instance>();
	static List<DescribeDBInstancesResponse.DBInstance> rds_list = new ArrayList<DescribeDBInstancesResponse.DBInstance>();

	// ak与sk
	static String accessKey = "rU4I6WtZ1m253ycd";
	static String accessSecret = "nDRQszivKejCALO7uld1nEjlULbxYt";

	static {
		ecs_region_map.put("cn-shenzhen", "华南 1");
		ecs_region_map.put("ap-southeast-1", "亚太东南 1 (新加坡)");
		ecs_region_map.put("cn-qingdao", "华北 1");
		ecs_region_map.put("cn-beijing", "华北 2");
		ecs_region_map.put("cn-shanghai", "华东 2");
		ecs_region_map.put("us-east-1", "美国东部 1 (弗吉尼亚)");
		ecs_region_map.put("cn-hongkong", "香港");
		ecs_region_map.put("cn-hangzhou", "华东 1");
		ecs_region_map.put("ap-northeast-1", "亚太东北 1 (日本)");
		ecs_region_map.put("us-west-1", "美国西部 1 (硅谷)");

		drds_region_map.put("cn-shenzhen", "华南1");
		drds_region_map.put("cn-hangzhou", "华东1");
		drds_region_map.put("cn-shanghai", "华东2");
		drds_region_map.put("cn-qingdao", "华北1");
		drds_region_map.put("cn-beijing", "华北2");

		rds_region_map.put("ap-southeast-1", "亚太东南 1 (新加坡)");
		rds_region_map.put("cn-beijing-gov-1", "北京政务区域1");
		rds_region_map.put("cn-qingdao", "华北 1");
		rds_region_map.put("us-east-1", "美国东部 1 (弗吉尼亚)");
		rds_region_map.put("cn-shenzhen-finance-1", "华南 1");
		rds_region_map.put("cn-beijing", "华北 2");
		rds_region_map.put("cn-shanghai-finance-1", "华东 2 金融云");
		rds_region_map.put("cn-shanghai", "华东 2");
		rds_region_map.put("ap-northeast-1", "亚太东北 1 (日本)");
		rds_region_map.put("cn-shenzhen", "华南 1");
		rds_region_map.put("cn-hongkong", "香港");
		rds_region_map.put("cn-yushanfang", "御膳房");
		rds_region_map.put("us-west-1", "美国西部 1 (硅谷)");
		rds_region_map.put("cn-fujian", "福建");
		rds_region_map.put("cn-hangzhou", "华东 1");
	}

	static String[] drds_instance_action_arr = { "CreateDrdsInstance", "DescribeDrdsInstance",
			"ModifyDrdsInstanceDescription", "DescribeDrdsInstances", "RemoveDrdsInstance", };
	static String[] drds_db_action_arr = { "CeateDrdsDB", "DescribeDrdsDB", "DeleteDrdsDB", "ModifyDrdsDBPasswd",
			"DescribeDrdsDBs", "ModifyDrdsIpWhiteList", "DescribeDrdsDBIpWhiteList", "ModifyRdsReadWeight",
			"DeleteFailedDrdsDB", "DescribeShardDBs", "DescribeRdsList", "ModifyFullTableScan", "CreateReadOnlyAccount",
			"DescribeReadOnlyAccount", "ModifyReadOnlyAccount", "RemoveReadOnlyAccount", };

	public static void main(String[] args) throws Exception {
		// test();
		// get();
		//
		get_cms_data();
		// System.out.println("********************************");
		// System.out.println("get_ecs_list");
		// System.out.println("********************************");
		// get_ecs_list();
		//
		// System.out.println("********************************");
		// System.out.println("get_drds_list");
		// System.out.println("********************************");
		// get_drds_list();
		//
		// System.out.println("********************************");
		// System.out.println("get_rds_list");
		// System.out.println("********************************");
		// get_rds_list();
	}

	public static void test() throws Exception {

		IClientProfile profile = DefaultProfile.getProfile("cn-shenzhen", accessKey, accessSecret);
		IAcsClient client = new DefaultAcsClient(profile);

		DescribeRdsListRequest req2 = new DescribeRdsListRequest();
		req2.setDrdsInstanceId("drdsi1av0gqh4k33");
		req2.setDbName("gddzfp");
		DescribeRdsListResponse res2 = client.getAcsResponse(req2);
		List<RdsInstance> data2 = res2.getData();

		for (Iterator<RdsInstance> it2 = data2.iterator(); it2.hasNext();) {
			RdsInstance rds = it2.next();

			String str2 = JSON.toJSONString(rds);
			System.out.println(str2);
		}

	}

	@SuppressWarnings("rawtypes")
	public static void get_cms_data() throws Exception {
		IClientProfile profile = DefaultProfile.getProfile("cn-shenzhen", accessKey, accessSecret);
		IAcsClient client = new DefaultAcsClient(profile);

		QueryMetricListRequest request = new QueryMetricListRequest();

		
		request.setPeriod("300");
		request.setLength("10");
		request.setCursor("1467963900000003d16b31e22329672d3e101a55d2caa31303030323432383030353930393834242c692d393435667539376633242c");

		
		request.setContentType(FormatType.JSON);
		request.setAcceptFormat(FormatType.JSON);
		request.setProject("acs_ecs");
		request.setMetric("CPUUtilization");
		request.setDimensions("{instanceId:'i-94dsabj6j'}");

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar startCal = Calendar.getInstance();
		startCal.add(Calendar.HOUR_OF_DAY, -2);
		String startTime = format.format(startCal.getTime());
		request.setStartTime(startTime);

		Calendar endCal = Calendar.getInstance();
		endCal.add(Calendar.HOUR_OF_DAY, 0);
		String endTime = format.format(endCal.getTime());
		request.setStartTime(endTime);
		
		QueryMetricListResponse response = client.getAcsResponse(request);

		List<JSONObject> data = response.getDatapoints();
		for (Iterator<JSONObject> iterator = data.iterator(); iterator.hasNext();) {
			JSONObject instance = iterator.next();
			String str = instance.toJSONString();
			System.out.println(str);
		}
	}

	@SuppressWarnings("rawtypes")
	public static void get_ecs_list() throws Exception {

		Iterator<Entry<String, String>> iter = ecs_region_map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String regionid = (String) entry.getKey();
			String regionname = (String) entry.getValue();
			System.out.println("********regionid:" + regionid + ",regionname:" + regionname);

			try {
				DescribeInstancesRequest describe = new DescribeInstancesRequest();
				IClientProfile profile = DefaultProfile.getProfile(regionid, accessKey, accessSecret);
				IAcsClient client = new DefaultAcsClient(profile);

				DescribeInstancesResponse response = client.getAcsResponse(describe);
				List<DescribeInstancesResponse.Instance> data = response.getInstances();
				for (Iterator<DescribeInstancesResponse.Instance> iterator = data.iterator(); iterator.hasNext();) {
					DescribeInstancesResponse.Instance instance = iterator.next();

					String str = JSON.toJSONString(instance);
					System.out.println(str);
				}
			} catch (Exception e) {
				// e.printStackTrace();
			}

		}

	}

	@SuppressWarnings("rawtypes")
	public static void get_drds_list() throws Exception {

		Iterator<Entry<String, String>> iter = drds_region_map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String regionid = (String) entry.getKey();
			String regionname = (String) entry.getValue();
			System.out.println("********regionid:" + regionid + ",regionname:" + regionname);

			try {
				DescribeDrdsInstancesRequest describe = new DescribeDrdsInstancesRequest();
				IClientProfile profile = DefaultProfile.getProfile(regionid, accessKey, accessSecret);
				IAcsClient client = new DefaultAcsClient(profile);

				DescribeDrdsInstancesResponse response = client.getAcsResponse(describe);
				List<DescribeDrdsInstancesResponse.Instance> data = response.getData();

				for (Iterator<DescribeDrdsInstancesResponse.Instance> iterator = data.iterator(); iterator.hasNext();) {
					DescribeDrdsInstancesResponse.Instance instance = iterator.next();

					String str = JSON.toJSONString(instance);
					System.out.println(" drds " + str);

					DescribeDrdsDBsRequest req1 = new DescribeDrdsDBsRequest();
					req1.setDrdsInstanceId(instance.getDrdsInstanceId());

					DescribeDrdsDBsResponse res1 = client.getAcsResponse(req1);
					List<Db> data1 = res1.getData();

					for (Iterator<Db> it1 = data1.iterator(); it1.hasNext();) {
						Db db = it1.next();
						String str1 = JSON.toJSONString(db);
						System.out.println("\t drds db " + str1);
						try {
							DescribeRdsListRequest req2 = new DescribeRdsListRequest();
							req2.setDrdsInstanceId(instance.getDrdsInstanceId());
							req2.setDbName(db.getDbName());
							DescribeRdsListResponse res2 = client.getAcsResponse(req2);
							List<RdsInstance> data2 = res2.getData();

							for (Iterator<RdsInstance> it2 = data2.iterator(); it2.hasNext();) {
								RdsInstance rds = it2.next();

								String str2 = JSON.toJSONString(rds);
								System.out.println("\t\t drds db rds " + str2);
							}

						} catch (Exception e) {
							e.printStackTrace();
						}
					}

				}
			} catch (Exception e) {
				// e.printStackTrace();
			}

		}

	}

	@SuppressWarnings("rawtypes")
	public static void get_rds_list() throws Exception {
		Iterator<Entry<String, String>> iter = rds_region_map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String regionid = (String) entry.getKey();
			String regionname = (String) entry.getValue();
			System.out.println("********regionid:" + regionid + ",regionname:" + regionname);

			try {
				DescribeDBInstancesRequest describe = new DescribeDBInstancesRequest();
				IClientProfile profile = DefaultProfile.getProfile(regionid, accessKey, accessSecret);
				IAcsClient client = new DefaultAcsClient(profile);

				DescribeDBInstancesResponse response = client.getAcsResponse(describe);
				List<DescribeDBInstancesResponse.DBInstance> data = response.getItems();
				for (Iterator<DescribeDBInstancesResponse.DBInstance> iterator = data.iterator(); iterator.hasNext();) {
					DescribeDBInstancesResponse.DBInstance instance = iterator.next();

					String str = JSON.toJSONString(instance);
					System.out.println(str);
				}
			} catch (Exception e) {
				// e.printStackTrace();
			}

		}
	}

	public static void get() throws Exception {
		// System.setProperty("webdriver.chrome.driver",
		// "D:\\jhh\\other_download\\chromedriver_win32\\chromedriver.exe");
		// WebDriver driver = new ChromeDriver();
		System.setProperty("webdriver.gecko.driver",
				"D:\\jhh\\other_download\\geckodriver-v0.11.1-win64\\geckodriver.exe");
		WebDriver driver = new FirefoxDriver();
		driver.manage().window().maximize();

		driver.get("https://account.aliyun.com/login/login.htm");

		// WebElement sw = driver.findElement(By.id("J_Quick2Static"));
		// sw.click();

		// WebElement loginid = driver.findElement(By.id("fm-login-id"));
		// WebElement loginpassword =
		// driver.findElement(By.id("fm-login-password"));
		// WebElement loginBtn = driver.findElement(By.id("fm-login-submit"));

		WebElement loginid = driver.findElement(By.name("loginId"));
		WebElement loginpassword = driver.findElement(By.name("password"));
		WebElement loginBtn = driver.findElement(By.name("submit-btn"));

		loginid.sendKeys("servyoualic@servyou.com.cn");
		loginpassword.sendKeys("chongzhiadmin!@#");
		loginBtn.click();

		driver.close();

	}

	public static void drdsOpenAPI() throws NoSuchAlgorithmException, InvalidKeyException, IOException {

		// 公共参数
		Map<String, String> parameters = new TreeMap<String, String>();
		parameters.put("Format", "JSON");
		parameters.put("Action", "DescribeRdsList");// 调用DescribeDrdsInstances接口
		parameters.put("Version", "2015-04-13");
		parameters.put("AccessKeyId", accessKey);
		parameters.put("SignatureMethod", "HMAC-SHA1");
		parameters.put("Timestamp", getISO8601Time());
		parameters.put("SignatureVersion", "1.0");
		parameters.put("SignatureNonce", UUID.randomUUID().toString());
		parameters.put("RegionId", "cn-shenzhen");

		parameters.put("DrdsInstanceId", "drdsi1av0gqh4k33");
		parameters.put("DbName", "gddzfp");

		StringBuilder paramStr = new StringBuilder();
		// 拼接请求参数
		for (Map.Entry<String, String> entry : parameters.entrySet()) {
			paramStr.append(percentEncode(entry.getKey())).append("=").append(percentEncode(entry.getValue()))
					.append("&");
		}
		paramStr.deleteCharAt(paramStr.length() - 1);
		// 计算签名
		StringBuilder stringToSign = new StringBuilder();
		stringToSign.append("GET").append("&").append(percentEncode("/")).append("&")
				.append(percentEncode(paramStr.toString()));
		Mac mac = Mac.getInstance("HmacSHA1");
		mac.init(new SecretKeySpec((accessSecret + "&").getBytes("UTF-8"), "HmacSHA1"));
		byte[] signData = mac.doFinal(stringToSign.toString().getBytes("UTF-8"));
		String signStr = Base64Helper.encode(signData);
		// 拼接URL
		String requestUrl = "http://drds.aliyuncs.com/?" + paramStr.toString() + "&Signature=" + percentEncode(signStr);
		// 准备发送HTTP请求
		URL url = new URL(requestUrl);
		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		httpConn.setRequestMethod("GET");
		httpConn.setDoOutput(true);
		httpConn.setDoInput(true);
		httpConn.setUseCaches(false);
		httpConn.connect();
		InputStream content = httpConn.getInputStream();
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		byte[] buff = new byte[1024];
		while (true) {
			final int read = content.read(buff);
			if (read == -1)
				break;
			outputStream.write(buff, 0, read);
		}
		System.out.println(new String(outputStream.toByteArray()));
		;
	}

	public static String percentEncode(String value) throws UnsupportedEncodingException {
		return value != null
				? URLEncoder.encode(value, "UTF-8").replace("+", "%20").replace("*", "%2A").replace("%7E", "~") : null;
	}

	static String getISO8601Time() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		df.setTimeZone(new SimpleTimeZone(0, "GMT"));
		return df.format(new Date());
	}
}
