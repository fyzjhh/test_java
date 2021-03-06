package com.test.cloud;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.concurrent.CountDownLatch;

import com.netease.cloud.services.nos.NosClient;

public class TestHelper {
	/*
	 * //娴嬭瘯鏂囦欢鐨勫瓨鏀剧洰褰� public static String testFilePath =
	 * "TestFile"+File.separator;
	 * 
	 * public static String localTestFilePath; //璁块棶鍑瘉瀛樻斁浣嶇疆 public static
	 * final String credential = "conf/credentials.properties"; //NOS鏈嶅姟鍣ㄥ湴鍧�
	 * public static final String hostaddr = "conf/host.properties";
	 * 
	 * static { Properties properties = new Properties(); try { InputStream t =
	 * new FileInputStream("conf/settings.properties"); try {
	 * properties.load(t); } finally { t.close(); } } catch (Exception e) {
	 * e.printStackTrace(); } localTestFilePath = (String)
	 * properties.get("testfiles"); }
	 */
	/*
	 * 棰勮鎸囧畾鏁伴噺鐨勬祴璇曟枃浠�
	 */
	public static String[] putObjects(NosClient client, String bucketName,
			String[] objectNames, String[] fileNames, int quantity)
			throws InterruptedException {
		int threadNum = 10;
		CountDownLatch doneSignal = new CountDownLatch(threadNum);
		int averageRunTimes = quantity / threadNum;
		int remainder = quantity % threadNum;
		int lastRunTimes = remainder == 0 ? averageRunTimes : averageRunTimes
				+ remainder;
		PutObjectThread[] putObjects = new PutObjectThread[threadNum];
		String[] objectKeyPrefix = new String[threadNum];
		int currentIndex = 0;
		int[] objectNameCount = new int[objectNames.length];
		String[] allObjects = new String[quantity];

		for (int i = 0; i < objectNames.length; i++) {
			objectNameCount[i] = 0;
		}

		for (int i = 0; i < threadNum; i++) {
			currentIndex = i % objectNames.length;
			objectKeyPrefix[i] = objectNames[currentIndex]
					+ (objectNameCount[currentIndex]++);
			if (i == (threadNum - 1)) {
				putObjects[i] = new PutObjectThread(client, i, threadNum,
						averageRunTimes, lastRunTimes, doneSignal, bucketName,
						objectKeyPrefix[i], fileNames[i % fileNames.length]);
			} else {
				putObjects[i] = new PutObjectThread(client, i, threadNum,
						averageRunTimes, averageRunTimes, doneSignal,
						bucketName, objectKeyPrefix[i], fileNames[i
								% fileNames.length]);
			}
			putObjects[i].start();
		}

		for (int i = 0; i < threadNum; i++) {
			if (i == (threadNum - 1)) {
				for (int j = 0; j < lastRunTimes; j++) {
					allObjects[i * averageRunTimes + j] = objectKeyPrefix[i]
							+ "_" + j;
				}
			} else {
				for (int j = 0; j < averageRunTimes; j++) {
					allObjects[i * averageRunTimes + j] = objectKeyPrefix[i]
							+ "_" + j;
				}
			}
		}
		doneSignal.await();

		return allObjects;
	}

	/******************************* 璁＄畻MD5(S) ******************************/
	public static String getMD5(String fileName) throws Exception {
		FileInputStream fis = new FileInputStream(fileName);
		return getMD5(fis);

	}

	public static String getMD5(InputStream is) throws Exception {
		ByteArrayOutputStream output = null;
		byte[] MD5 = null;

		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			is.reset();
			byte[] buffer = new byte[1024 * 1024 * 20];
			int len;

			while ((len = is.read(buffer)) != -1) {
				// logger.debug("len = " + len);

				md5.update(buffer, 0, len);
			}
			MD5 = md5.digest();
		} catch (Exception e) {
			throw e;
		} finally {
			if (is != null) {
				is.close();
			}

		}
		return getMD5HexString(MD5);
	}

	public static String getMD5HexString(byte[] MD5) throws Exception {

		return byteArrayToHexString(MD5);
	}

	private static String byteArrayToHexString(byte[] b) {
		StringBuffer resultSb = new StringBuffer();
		for (int i = 0; i < b.length; i++) {
			resultSb.append(byteToHexString(b[i]));
		}
		return resultSb.toString();
	}

	private static String byteToHexString(byte b) {
		int n = b;
		if (n < 0)
			n = 256 + n;
		int d1 = n / 16;
		int d2 = n % 16;
		return hexDigits[d1] + hexDigits[d2];
	}

	private final static String[] hexDigits = { "0", "1", "2", "3", "4", "5",
			"6", "7", "8", "9", "A", "B", "C", "D", "E", "F" };

	/******************************* 璁＄畻MD5(E) ******************************/

	/**
	 * 鑾峰彇srcFileName鏂囦欢鐨勬寚瀹氬垎鍧楋紝鍐欏叆鐩爣鏂囦欢destFileName
	 */
	public static void getPartFile(String srcFileName, long startIndex,
			long endIndex, String destFileName) throws IOException {
		long currentLen = 0;
		FileInputStream fis = null;
		FileOutputStream fos = null;
		boolean isStartPos = true;
		File f = new File(destFileName);
		if (f.exists()) {
			f.delete();
		}
		try {
			fis = new FileInputStream(srcFileName);
			fos = new FileOutputStream(destFileName);

			int len = -1;
			byte[] buf = new byte[1024 * 1024 * 5];
			while ((len = fis.read(buf)) > 0) {
				if ((currentLen + len) >= startIndex) {
					if (isStartPos) {
						if ((currentLen + len) >= endIndex) {
							fos.write(buf, (int) (startIndex - currentLen),
									(int) (endIndex - startIndex + 1));
							break;
						} else {
							fos.write(buf, (int) (startIndex - currentLen),
									(int) (currentLen + len - startIndex));
							isStartPos = false;
						}
					} else {
						if ((currentLen + len) >= endIndex) {
							fos.write(buf, 0, (int) (endIndex - currentLen + 1));
							break;
						} else {
							fos.write(buf, 0, len);
						}

					}
				}
				currentLen += len;
			}
		} catch (FileNotFoundException e) {
			System.err.println(e);
		} catch (IOException e) {
			System.err.println(e);
		} finally {
			fos.flush();
			fos.close();
			fis.close();
		}
	}

	public static String getRedirectedHost(String message) {
		int pos = message.lastIndexOf(":");
		String redirectedHost = message.substring(pos + 1).trim();
		return redirectedHost;
	}
}
