package com.test.jhh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class TestPopoBinlogStat {

	public static void main(String[] args) throws Exception {
		// deal2("E:\\sftpdir\\new1.sort", "E:\\sftpdir\\res12.txt");
		// cal("E:\\sftpdir\\");
	}

	public static void cal(String path) throws Exception {
		for (int i = 1; i <= 2; i++) {
			String fn = path + i + "-1.sort";
			String sn = path + i + "-1.res";
			readFileByLines(fn, sn);
		}
	}

	// ���˳�ʱ���sql���
	public static void deal2(String sn, String rn) throws Exception {
		BufferedWriter output = new BufferedWriter(new FileWriter(rn));

		File file = new File(sn);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = reader.readLine();
			int tmp_msgid = 0;
			String tmp_uid = null;
			String tmp_tid = null;
			String tmp_time = null;

			tmp_msgid = Integer.valueOf(tempString.split(" ")[4]);
			tmp_uid = tempString.split(" ")[0];
			tmp_tid = tempString.split(" ")[1];
			tmp_time = tempString.split(" ")[2] + tempString.split(" ")[3];

			while ((tempString = reader.readLine()) != null) {

				if (tempString.contains("#11")) {
					// System.out.println(tempString);
					String[] arrs = tempString.split(" ");

					int tmsgid = Integer.valueOf(arrs[4]);
					String tuid = arrs[0];
					String ttid = arrs[1];
					String ttime = arrs[2] + arrs[3];

					if (tuid.equals(tmp_uid) && ttid.equals(tmp_tid)) { // uid��tid��ͬ�����
						if ((ttime.compareTo(tmp_time) >= 0)
								&& (tmsgid < tmp_msgid)) {

							System.out.println("xxxxxxxx:" + tempString);
							output.write("xxxxxxxx:" + tempString + "\r\n");

						} else {
							tmp_msgid = tmsgid;
							tmp_time = ttime;
						}
					} else {
						tmp_uid = tuid;
						tmp_tid = ttid;
						tmp_msgid = tmsgid;
						tmp_time = ttime;
					}
				}

			}
			reader.close();
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}

	public static void readFileByLines(String sn, String rn) throws Exception {
		BufferedWriter output = new BufferedWriter(new FileWriter(rn));

		File file = new File(sn);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = reader.readLine();
			int vmsgid = 0;
			String vuid = "";
			String vtid = "";
			if (tempString.split(" ").length == 14) {
				vmsgid = Integer.valueOf(tempString.split(" ")[5]);
				vuid = tempString.split(" ")[9];
				vtid = tempString.split(" ")[13];
			}

			while ((tempString = reader.readLine()) != null) {
				// System.out.println(tempString);
				String[] arrs = tempString.split(" ");
				if (arrs.length == 14) {

					int tmsgid = Integer.valueOf(arrs[5]);
					String tuid = arrs[9];
					String ttid = arrs[13];

					if (tuid.equals(vuid) && ttid.equals(vtid)) { // uid��tid��ͬ�����
						if (tmsgid < vmsgid) { // ��ǰ��tmsgid С�� ǰ������
							System.out
									.println("xxxxxxxxxxxxxxxx:" + tempString);
							output.write("xxxxxxxxxxxxxxxx:" + tempString
									+ "\r\n");
						} else {
							vmsgid = tmsgid;
						}
					} else {
						vuid = tuid;
						vtid = ttid;
						vmsgid = tmsgid;
					}
				}
			}
			reader.close();
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}
}
