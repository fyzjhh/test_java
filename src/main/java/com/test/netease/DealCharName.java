package com.test.jhh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sun.misc.BASE64Encoder;

public class DealCharName {

	public static void main(String[] args) throws Exception {
		// String[] files = new String[] { "草船allplayerdata.csv",
		// "九伐中原AllPlayerData.csv", "青梅煮酒AllPlayerData.csv",
		// "官渡之战AllPlayerData.csv", "火烧赤壁AllPlayerData.csv",
		// "荆州之战layerData.csv", "桃园结义AllPlayerData.csv" };
		//
		// // files = new String[] { "草船allplayerdata.csv" };
		//
		// for (int i = 0; i < files.length; i++) {
		// deal("F:/m3gdata/" + files[i]);
		// }

		String[] files = new String[] { "草船corpplayerinfo", "赤壁corpplayerinfo",
				"官渡corpplayerinfo", "荆州corpplayerinfo", "九伐corpplayerinfo",
				"青梅corpplayerinfo", "桃园corpplayerinfo" };

//		files = new String[] { "桃园corpplayerinfo" };

		for (int i = 0; i < files.length; i++) {
			deal_corp("D:\\temp\\corpplayerinfo\\" + files[i]
					+ "\\corpinfo.sql");
		}
	}

	// 字符流处理
	public static void deal_corp(String src_fn) throws Exception {

		DateFormat datetimeformat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

		// 源文件
		File src_file = new File(src_fn);
		if (src_file.exists() == false) {
			return;
		}

		FileInputStream fis = new FileInputStream(src_file);
		InputStreamReader isr = new InputStreamReader(fis, "latin1");
		BufferedReader br = new BufferedReader(isr);

		// 目标文件
		File dest_file = new File(src_fn + ".txt");
		if (dest_file.exists() == false) {
			dest_file.createNewFile();
		}
		FileOutputStream fos = new FileOutputStream(dest_file, false);
		OutputStreamWriter osr = new OutputStreamWriter(fos, "latin1");
		BufferedWriter bw = new BufferedWriter(osr);

		// 错误目标文件
		File dest_file_err = new File(src_fn + ".err");
		if (dest_file_err.exists() == false) {
			dest_file_err.createNewFile();
		}
		FileOutputStream fos_err = new FileOutputStream(dest_file_err, false);
		OutputStreamWriter osr_err = new OutputStreamWriter(fos_err, "latin1");
		BufferedWriter bw_err = new BufferedWriter(osr_err);

		// 3个字段
		// 军团id、军团规模、军团名
		String pat_str = "([^\t]+)\t([^\t]+)\t(.*)";
		Pattern p = Pattern.compile(pat_str);

		BASE64Encoder encoder = new sun.misc.BASE64Encoder();

		// 开始读，跳过第一行
		String l_str = "";
		int i = 0;
		String tmp_corpname = "";
		String tmp_base64 = "";
		String r = "";
		byte[] bs;
		while ((l_str = br.readLine()) != null) {
			i++;

			Matcher matcher = p.matcher(l_str);

			if (matcher.find()) {

				tmp_corpname = matcher.group(3);
				bs = tmp_corpname.getBytes("latin1");
				tmp_base64 = encoder.encode(bs);

				r = matcher.group(1) + "," + matcher.group(2) + ","
						+ tmp_base64 + "\r\n";
				bw.write(r);
			} else {
				bw_err.write(r);// 打印之前的记录
				r = i + ",," + l_str + ",,"
						+ Arrays.toString(l_str.getBytes("latin1")) + "\r\n";
				bw_err.write(r);
			}

			if (i % 50000 == 0) {
				System.out.println(datetimeformat.format(new Date())
						+ "\tdealed " + i);
				bw.flush();
				bw_err.flush();
			}
		}
		br.close();
		bw_err.close();
		bw.close();
	}

	// 字符流处理
	public static void deal(String src_fn) throws Exception {

		DateFormat datetimeformat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

		// 源文件
		File src_file = new File(src_fn);
		if (src_file.exists() == false) {
			return;
		}

		FileInputStream fis = new FileInputStream(src_file);
		InputStreamReader isr = new InputStreamReader(fis, "latin1");
		BufferedReader br = new BufferedReader(isr);

		// 目标文件
		File dest_file = new File(src_fn + ".txt");
		if (dest_file.exists() == false) {
			dest_file.createNewFile();
		}
		FileOutputStream fos = new FileOutputStream(dest_file, false);
		OutputStreamWriter osr = new OutputStreamWriter(fos, "latin1");
		BufferedWriter bw = new BufferedWriter(osr);

		// 错误目标文件
		File dest_file_err = new File(src_fn + ".err");
		if (dest_file_err.exists() == false) {
			dest_file_err.createNewFile();
		}
		FileOutputStream fos_err = new FileOutputStream(dest_file_err, false);
		OutputStreamWriter osr_err = new OutputStreamWriter(fos_err, "latin1");
		BufferedWriter bw_err = new BufferedWriter(osr_err);

		// 14 个字段
		// 用户ID,注册时间,在线时长,游戏积分,
		// 竞技胜利盘数,竞技总盘数,竞技平均击杀,竞技击杀总人数,
		// 第一章成就完成率,第二章成就完成率,第三章成就完成率,第四章成就完成率,
		// 最后登录时间,角色昵称
		String pat_str = "([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),(.*)";
		Pattern p = Pattern.compile(pat_str);

		BASE64Encoder encoder = new sun.misc.BASE64Encoder();

		// 开始读，跳过第一行
		String l_str = br.readLine();
		int i = 0;
		String tmp_score = "";
		String tmp_level = "";
		String tmp_char = "";
		String tmp_base64 = "";
		String r = "";
		byte[] bs;
		while ((l_str = br.readLine()) != null) {
			i++;

			Matcher matcher = p.matcher(l_str);

			if (matcher.find()) {
				tmp_score = matcher.group(4);
				tmp_level = userlevel(new Integer(tmp_score));
				tmp_char = matcher.group(14);
				bs = tmp_char.getBytes("latin1");
				tmp_base64 = encoder.encode(bs);

				r = matcher.group(1) + "," + matcher.group(2) + ","
						+ matcher.group(3) + "," + matcher.group(4) + ","
						+ matcher.group(5) + "," + matcher.group(6) + ","
						+ matcher.group(7) + "," + matcher.group(8) + ","
						+ matcher.group(9) + "," + matcher.group(10) + ","
						+ matcher.group(11) + "," + matcher.group(12) + ","
						+ matcher.group(13) + "," + tmp_level + ","
						+ tmp_base64 + "\r\n";
				bw.write(r);
			} else {
				bw_err.write(r);// 打印之前的记录
				r = i + ",," + l_str + ",,"
						+ Arrays.toString(l_str.getBytes("latin1")) + "\r\n";
				bw_err.write(r);
			}

			if (i % 50000 == 0) {
				System.out.println(datetimeformat.format(new Date())
						+ "\tdealed " + i);
				bw.flush();
				bw_err.flush();
			}
		}
		br.close();
		bw_err.close();
		bw.close();
	}

	// 字节流
	public static void deal2(String src_fn) throws Exception {

		DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

		// 源文件
		File src_file = new File(src_fn);
		if (src_file.exists() == false) {
			return;
		}
		FileInputStream fis = new FileInputStream(src_file);
		InputStreamReader isr = new InputStreamReader(fis, "latin1");
		BufferedReader br = new BufferedReader(isr);

		// 目标文件
		File dest_file = new File(src_fn + ".txt");
		if (dest_file.exists() == false) {
			dest_file.createNewFile();
		}
		FileOutputStream fos = new FileOutputStream(dest_file, false);
		OutputStreamWriter osr = new OutputStreamWriter(fos, "latin1");
		BufferedWriter bw = new BufferedWriter(osr);

		// 14 个字段
		// 用户ID,注册时间,在线时长,游戏积分,
		// 竞技胜利盘数,竞技总盘数,竞技平均击杀,竞技击杀总人数,
		// 第一章成就完成率,第二章成就完成率,第三章成就完成率,第四章成就完成率,
		// 最后登录时间,角色昵称
		String pat_str = "([0-9]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([\\s\\S]*)";
		Pattern p = Pattern.compile(pat_str);

		BASE64Encoder encoder = new sun.misc.BASE64Encoder();

		// 开始读，跳过第一行
		String b_str = br.readLine();
		String n_str = "";
		int dealed_num = 0;
		int read_num = 0;
		int idx = 0;
		int read_cnt = 0;
		String tmp = "";

		String tmp_score = "";
		String tmp_level = "";
		String tmp_char = "";
		String tmp_base64 = "";
		String r = "";
		byte[] bs;

		char[] cb;
		char[] left_cb = new char[0];
		int left_cnt = 0;
		char[] read_cb = new char[1048576];
		Matcher b_match = p.matcher(b_str);
		Matcher n_match;
		boolean b_bool = false;
		boolean n_bool = false;
		String log_str;
		while (true) {

			read_cnt = br.read(read_cb);
			read_num++;
			log_str = dtf.format(new Date()) + "\tr " + read_num;
			System.out.println(log_str);

			if (read_cnt > 0) { // 没有到达文件末尾
				cb = new char[left_cnt + read_cnt];

				System.arraycopy(left_cb, 0, cb, 0, left_cnt);
				System.arraycopy(read_cb, 0, cb, left_cnt, read_cnt);

				tmp = new String(cb);
				b_str = "";
				n_str = "";

				// b_str
				idx = tmp.indexOf("\r\n");
				while (idx >= 0) {
					b_str = b_str + tmp.substring(0, idx);

					b_match = p.matcher(b_str);
					b_bool = b_match.find();

					if (b_bool) {
						break;
					} else {
						tmp = tmp.substring(idx + 2);
						idx = tmp.indexOf("\r\n");
						continue;
					}
				}
				if (idx < 0) {
					left_cb = tmp.toCharArray();
					left_cnt = tmp.length();
					break;
				}
				// 循环
				while (true) {

					// n_str
					n_str = "";
					tmp = tmp.substring(idx + 2);
					idx = tmp.indexOf("\r\n");
					while (idx >= 0) {
						n_str = tmp.substring(0, idx);

						n_match = p.matcher(n_str);
						n_bool = n_match.find();

						if (n_bool) {
							break;
						} else {
							// 把不匹配的数据追加到前一个记录
							if (idx == 0) {
								b_str = b_str + "\r\n";
							} else {
								b_str = b_str + "\r\n" + n_str;
							}
							tmp = tmp.substring(idx + 2);
							idx = tmp.indexOf("\r\n");
							continue;
						}
					}
					if (idx < 0) {
						left_cb = tmp.toCharArray();
						left_cnt = tmp.length();
						break;
					}
					//
					// if (b_str.endsWith("\r\n")) {
					// b_str = b_str.substring(0, b_str.length() - 2);
					// }

					// 重新检查前一条记录是否匹配
					b_match = p.matcher(b_str);
					b_bool = b_match.find();

					// 两条记录都匹配的时候
					// 821149 1889214
					if (b_bool && n_bool) {

						// if ("821149".equals(b_match.group(1))) {
						// tmp_iuin = "821149";
						// }
						tmp_score = b_match.group(4);
						tmp_level = userlevel(new Integer(tmp_score));
						tmp_char = b_match.group(14);
						bs = tmp_char.getBytes("latin1");
						tmp_base64 = encoder.encode(bs);

						r = b_match.group(1) + "," + b_match.group(2) + ","
								+ b_match.group(3) + "," + b_match.group(4)
								+ "," + b_match.group(5) + ","
								+ b_match.group(6) + "," + b_match.group(7)
								+ "," + b_match.group(8) + ","
								+ b_match.group(9) + "," + b_match.group(10)
								+ "," + b_match.group(11) + ","
								+ b_match.group(12) + "," + b_match.group(13)
								+ "," + tmp_level + "," + tmp_base64 + "\r\n";
						bw.write(r);

						dealed_num++;
						if (dealed_num % 50000 == 0) {
							log_str = dtf.format(new Date()) + "\td "
									+ dealed_num;
							System.out.println(log_str);
							bw.flush();
						}

						// 把下一条记录给前一个记录
						b_str = n_str;
					} else {
						log_str = dtf.format(new Date()) + "\te " + read_num
								+ " " + dealed_num + " " + b_bool + " "
								+ n_bool + "\n" + b_str + "\n" + n_str;
						System.out.println(log_str);
						System.exit(1);
					}

				}

			} else { // 到达文件末尾,退出
				break;
			}
		}
		br.close();
		bw.close();
	}

	public static String userlevel(int score) {
		String ret = "-1";

		if (score < 0) {
			ret = "-1";
		} else if (score >= 0 && score <= 999) {
			ret = "01";
		} else if (score >= 1000 && score <= 4999) {
			ret = "02";
		} else if (score >= 5000 && score <= 9999) {
			ret = "03";
		} else if (score >= 10000 && score <= 15999) {
			ret = "04";
		} else if (score >= 16000 && score <= 23499) {
			ret = "05";
		} else if (score >= 23500 && score <= 33999) {
			ret = "06";
		} else if (score >= 34000 && score <= 47499) {
			ret = "07";
		} else if (score >= 47500 && score <= 63999) {
			ret = "08";
		} else if (score >= 64000 && score <= 83499) {
			ret = "09";
		} else if (score >= 83500 && score <= 113499) {
			ret = "10";
		} else if (score >= 113500 && score <= 155499) {
			ret = "11";
		} else if (score >= 155500 && score <= 209499) {
			ret = "12";
		} else if (score >= 209500 && score <= 275499) {
			ret = "13";
		} else if (score >= 275500 && score <= 353499) {
			ret = "14";
		} else if (score >= 353500 && score <= 533499) {
			ret = "15";
		} else if (score >= 533500 && score <= 785499) {
			ret = "16";
		} else if (score >= 785500 && score <= 1109499) {
			ret = "17";
		} else if (score >= 1109500 && score <= 1505499) {
			ret = "18";
		} else if (score >= 1505500 && score <= 1973499) {
			ret = "19";
		} else if (score >= 1973500 && score <= 2333499) {
			ret = "20";
		} else if (score >= 2333500 && score <= 2837499) {
			ret = "21";
		} else if (score >= 2837500 && score <= 3485499) {
			ret = "22";
		} else if (score >= 3485500 && score <= 4277499) {
			ret = "23";
		} else if (score >= 4277500 && score <= 5213499) {
			ret = "24";
		} else if (score >= 5213500 && score <= 25447499) {
			ret = "25";
		} else if (score >= 25447500 && score <= 50894999) {
			ret = "26";
		} else if (score >= 50895000 && score <= 101789999) {
			ret = "27";
		} else if (score >= 101790000 && score <= 203579999) {
			ret = "28";
		} else if (score >= 203580000 && score <= 407159999) {
			ret = "29";
		} else if (score >= 407160000) {
			ret = "30";
		}
		return ret;
	}

}
