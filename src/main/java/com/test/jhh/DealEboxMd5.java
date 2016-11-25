package com.test.jhh;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class DealEboxMd5 {

	public static void main(String[] args) throws Exception {
		deal1("D:/installfiles/4410_dmhml_sort.txt",
				"D:/installfiles/4410-0529.txt", "D:/installfiles/4410_out.txt");
		//
//		deal2("D:/installfiles/4410_md5_file.txt",
//				"D:/installfiles/4410_dmhml.txt");

	}

	public static void deal1(String file, String photo, String res)
			throws Exception {
		BufferedWriter output = new BufferedWriter(new FileWriter(res));

		File file_md5 = new File(file);
		File photo_md5 = new File(photo);
		BufferedReader readerfile = null;
		BufferedReader readerphoto = null;
		try {
			readerfile = new BufferedReader(new FileReader(file_md5));
			readerphoto = new BufferedReader(new FileReader(photo_md5));
			String lineFile, linePhoto;

			String docidFile;
			String md5highFile;
			String md5lowFile;

			String docidPhoto;
			String md5highPhoto;
			String md5lowPhoto;
			int i = 0;
			lineFile = readerfile.readLine();
			linePhoto = readerphoto.readLine();
			do {
				if (null == lineFile || null == linePhoto)
					break;

				docidFile = lineFile.split("\t")[0];
				md5highFile = lineFile.split("\t")[1];
				md5lowFile = lineFile.split("\t")[2];

				docidPhoto = linePhoto.split("\t")[0];
				md5highPhoto = linePhoto.split("\t")[1];
				md5lowPhoto = linePhoto.split("\t")[2];

				long idfile = Long.valueOf(docidFile);
				long idphoto = Long.valueOf(docidPhoto);

				// if (i % 5000 == 0) {
				// System.out.println("read " + i + " lines.");
				// output.write("read " + i + " lines."+ "\r\n");
				// }
				if (idfile == idphoto) {
					if (!md5highFile.equalsIgnoreCase(md5highPhoto)
							|| !md5lowFile.equalsIgnoreCase(md5lowPhoto)) {
						// System.out.println(idfile + "-(" + md5highFile + ","
						// + md5lowFile + ")-(" + md5highPhoto + ","
						// + md5lowPhoto + ")");
						output.write(idfile + "- md5error(" + md5highFile + ","
								+ md5lowFile + ")-(" + md5highPhoto + ","
								+ md5lowPhoto + ")" + "\r\n");
					}
					lineFile = readerfile.readLine();
					linePhoto = readerphoto.readLine();
					i = i + 2;
					continue;
				} else if (idfile > idphoto) {
					// System.out.println(idphoto + " not exists in file.");
					output.write(idphoto + " not exists in file." + "\r\n");
					linePhoto = readerphoto.readLine();
					i = i + 1;
					continue;
				} else if (idfile < idphoto) {
					// System.out.println(idfile + " not exists in database.");
					output.write(idfile + " not exists in database." + "\r\n");
					lineFile = readerfile.readLine();
					i = i + 1;
					continue;
				}
			} while (true);

			if (null != lineFile) {
				do {
					// System.out.println(lineFile +
					// "  not exists in database.");
					output.write(lineFile + "  not exists in database."
							+ "\r\n");
				} while ((lineFile = readerfile.readLine()) != null);
			}
			if (null != linePhoto) {
				do {
					// System.out.println(linePhoto + "  not exists in file.");
					output.write(linePhoto + "  not exists in file." + "\r\n");
				} while ((linePhoto = readerphoto.readLine()) != null);
			}
			readerfile.close();
			readerphoto.close();
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (readerfile != null) {
				try {
					readerfile.close();
				} catch (IOException e1) {
				}
			}
			if (readerphoto != null) {
				try {
					readerphoto.close();
				} catch (IOException e1) {
				}
			}
		}
	}

	public static void deal2(String sn, String rn) throws Exception {
		BufferedWriter output = new BufferedWriter(new FileWriter(rn));

		File file = new File(sn);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString;
			long docid = 0;
			String path = null;
			String md5str = null;
			long md5high;
			long md5low;

			int trashcnt = 0;
			int dealcnt = 0;
			while ((tempString = reader.readLine()) != null) {
				md5str = tempString.split("  ")[0];
				path = tempString.split("  ")[1];
				if (path.contains("trash")) {
					trashcnt++;
					continue;
				}
				docid = getsdfsId(path.substring(12));

				byte[] md5 = MD5HexString2Bytes(md5str);
//				md5high = Util.bytes2long(md5, 0, 8);
//				md5low = Util.bytes2long(md5, 8, 8);
				dealcnt++;
//				System.out.println(docid + "\t" + md5high + "\t" + md5low);
//				output.write(docid + "\t" + md5high + "\t" + md5low + "\r\n");

			}
			reader.close();
			output.close();

			System.out.println("trashcnt:" + trashcnt + ",dealcnt:" + dealcnt);
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

	public static long getsdfsId(String path) {
		String[] s = path.split("/");

		if (s.length == 0 || s.length > 5) {
			System.err.println("Invalid Path " + path);
			System.exit(-1);
		}
		int bucketId = Integer.valueOf(s[0]);
		long docid = (long) bucketId << 40;

		for (int i = 1; i < s.length; ++i) {
			short v = Short.valueOf(s[i]).shortValue();
			if (v >= (1 << 10)) {
				System.err.println("Invalid Path " + path);
				System.exit(-1);
			}
			docid |= (long) v << (4 - i) * 10;
		}
		return docid;
	}

	public void toDouble(String md5str) throws Exception {
		byte[] md5 = MD5HexString2Bytes(md5str);
//		long md5high = Util.bytes2long(md5, 0, 8);
//		long md5low = Util.bytes2long(md5, 8, 8);
//		System.out.println("md5high = " + md5high + " md5low = " + md5low);
	}

	public void toMd5(long md5high, long md5low) {
//		String md5 = Util.getHex(Util.long2byte(md5high, md5low));
//		System.out.println("md5 = " + md5);
	}

	private static byte[] MD5HexString2Bytes(String src) throws Exception {
		if (src.length() != 32) {
			throw new Exception("The length of MD5 string is not 32! ");
		}
		byte[] ret = new byte[16];
		byte[] tmp = src.getBytes();
		for (int i = 0; i < 16; i++) {
			ret[i] = uniteBytes(tmp[i * 2], tmp[i * 2 + 1]);
		}
		return ret;
	}

	private static byte uniteBytes(byte src0, byte src1) {
		byte _b0 = Byte.decode("0x" + new String(new byte[] { src0 }))
				.byteValue();
		_b0 = (byte) (_b0 << 4);
		byte _b1 = Byte.decode("0x" + new String(new byte[] { src1 }))
				.byteValue();
		byte ret = (byte) (_b0 ^ _b1);

		return ret;
	}
}
