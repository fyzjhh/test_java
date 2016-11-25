package com.test.java.charset;

/**
 * UnicodeGB2312.java
 * Copyright (c) 1997-2003 by Dr. Herong Yang
 * 
 * reference 
 *   Unicode Code Point Charts, Unicode, Inc, http://www.unicode.org/charts
 *   GB2312, wikipedia, http://en.wikipedia.org/wiki/GB2312
 */
import java.io.*;
import java.nio.*;
import java.nio.charset.*;

class CharSetUnicodeGB2312 {
	static OutputStream out = null;
	static char hexDigit[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
			'9', 'A', 'B', 'C', 'D', 'E', 'F' };
	static int b_out[] = { 201, 267, 279, 293, 484, 587, 625, 657, 734, 782,
			827, 874, 901, 980, 1001, 5590, 8801 };
	static int e_out[] = { 216, 268, 280, 294, 494, 594, 632, 694, 748, 794,
			836, 894, 903, 994, 1594, 5594, 9494 };

	public static void main(String[] a) {
		try {
			out = new FileOutputStream("D:/jhhWorks/jhh/ziliao/unicode_gb2312.gb");
			writeCode();
			out.close();
		} catch (IOException e) {
			System.out.println(e.toString());
		}
	}

	public static void writeCode() throws IOException {
		CharsetEncoder gbec = Charset.forName("GBK").newEncoder();
		char[] ca = new char[1];
		CharBuffer cb = null;
		ByteBuffer gbbb = null;
		writeHeader();
		int count = 0;
		for (int i = 0; i < 0x010000; i++) {
			ca[0] = (char) i;
			cb = CharBuffer.wrap(ca);
			try {
				gbbb = gbec.encode(cb);
			} catch (CharacterCodingException e) {
				gbbb = null;
			}
			if (validGB(gbbb)) {
				count++;
				writeHex((byte) (ca[0] >>> 8));
				writeHex((byte) (ca[0] & 0xff));
				writeString(" ");
				writeByteBuffer(gbbb, 2);
				writeString(" ");
				writeByte(gbbb.get(0));
				writeByte(gbbb.get(1));
				if (count % 5 == 0)
					writeln();
				else
					writeString("   ");
			}
		}
		if (count % 5 != 0)
			writeln();
		writeFooter();
		System.out.println("Number of GB characters wrote: " + count);
	}

	public static boolean validGB(ByteBuffer gbbb) {
		if (gbbb == null)
			return false;
		else if (gbbb.limit() != 2)
			return false;
		else {
			byte hi = gbbb.get(0);
			byte lo = gbbb.get(1);
			if ((hi & 0xFF) < 0xA0)
				return false;
			if ((lo & 0xFF) < 0xA0)
				return false;
			int i = (hi & 0xFF) - 0xA0;
			int j = (lo & 0xFF) - 0xA0;
			if (i < 1 || i > 94)
				return false;
			if (j < 1 || j > 94)
				return false;
			for (int l = 0; l < b_out.length; l++) {
				if (i * 100 + j >= b_out[l] && i * 100 + j <= e_out[l])
					return false;
			}
		}
		return true;
	}

	public static void writeHeader() throws IOException {
		writeString("<pre>");
		writeln();
		writeString("Uni. GB   ");
		writeGBSpace();
		writeString("   ");
		writeString("Uni. GB   ");
		writeGBSpace();
		writeString("   ");
		writeString("Uni. GB   ");
		writeGBSpace();
		writeString("   ");
		writeString("Uni. GB   ");
		writeGBSpace();
		writeString("   ");
		writeString("Uni. GB   ");
		writeGBSpace();
		writeln();
		writeln();
	}

	public static void writeFooter() throws IOException {
		writeString("</pre>");
		writeln();
	}

	public static void writeln() throws IOException {
		out.write(0x0D);
		out.write(0x0A);
	}

	public static void writeGBSpace() throws IOException {
		out.write(0xA1);
		out.write(0xA1);
	}

	public static void writeByteBuffer(ByteBuffer b, int l) throws IOException {
		int i = 0;
		if (b == null) {
			writeString("null");
			i = 2;
		} else {
			for (i = 0; i < b.limit(); i++)
				writeHex(b.get(i));
		}
		for (int j = i; j < l; j++)
			writeString("  ");
	}

	public static void writeString(String s) throws IOException {
		if (s != null) {
			for (int i = 0; i < s.length(); i++) {
				out.write((int) (s.charAt(i) & 0xFF));
			}
		}
	}

	public static void writeHex(byte b) throws IOException {
		out.write((int) hexDigit[(b >> 4) & 0x0F]);
		out.write((int) hexDigit[b & 0x0F]);
	}

	public static void writeByte(byte b) throws IOException {
		out.write(b & 0xFF);
	}
}