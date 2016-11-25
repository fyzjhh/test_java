package com.test.java.charset;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Conversion {

	/**
	 * Translate a short to 2 bytes.
	 * 
	 * @param in
	 *            The short to translate
	 * @param out
	 *            An array of bytes
	 * @param offset
	 *            An offset into the array (where to write the bytes)
	 * @return The number of bytes written (always 2)
	 */
	public static int shortToBytes(short in, byte out[], int offset) {
		out[offset] = (byte) ((in >>> 8) & 0xFF);
		out[offset + 1] = (byte) ((in >>> 0) & 0xFF);
		return 2;
	}

	/**
	 * Translate 2 bytes to a short.
	 * 
	 * @param in
	 *            An array of bytes containing the bytes to convert
	 * @param offset
	 *            An offset into in where to start reading bytes
	 * @return The short created from the bytes in in
	 */
	public static short bytesToShort(byte in[], int offset) {
		return (short) (((in[offset] << 8) & (0xFF << 8)) | ((in[offset + 1] << 0) & (0xFF << 0)));
	}

	/**
	 * Translate an int to 4 bytes.
	 * 
	 * @param in
	 *            The integer to translate
	 * @param out
	 *            An array of bytes
	 * @param offset
	 *            An offset into the array (where to write the bytes)
	 * @return The number of bytes written (always 4!)
	 */
	public static int intToBytes(int in, byte out[], int offset) {
		out[0 + offset] = (byte) ((in >>> 24) & 0xFF);
		out[1 + offset] = (byte) ((in >>> 16) & 0xFF);
		out[2 + offset] = (byte) ((in >>> 8) & 0xFF);
		out[3 + offset] = (byte) ((in >>> 0) & 0xFF);
		return 4;
	}

	/**
	 * Translate 4 bytes to an int.
	 * 
	 * @param in
	 *            An array of bytes containing the bytes to convert
	 * @param offset
	 *            An offset into in where to start reading bytes
	 * @return The integer created from the bytes in in
	 * 
	 */
	public static int bytesToInt(byte in[], int offset) {
		return ((in[offset] << 24) & (0xFF << 24))
				| ((in[offset + 1] << 16) & (0xFF << 16))
				| ((in[offset + 2] << 8) & (0xFF << 8))
				| ((in[offset + 3] << 0) & (0xFF << 0));
	}

	/**
	 * Boris Special - Anv?nd p? egen risk!
	 * 
	 */
	public static int strbuffToBytes(StringBuffer str, byte buff[], int offset) {
		int i;

		buff[offset] = (byte) str.length();
		offset++;
		for (i = 0; i < str.length(); i++)
			buff[offset + i] = (byte) str.charAt(i);
		offset += i;
		return offset;
	}

	/**
	 * Boris Special - Anv?nd p? egen risk!
	 * 
	 */
	public static int bytesToStrbuf(StringBuffer str, byte buff[], int offset) {

		int length = 0x00ff & (int) buff[offset++];
		for (int i = 0; i < length; i++)
			str.append((char) buff[offset + i]);
		return offset;
	}

	/**
	 * Translate bytes into a String
	 * 
	 * @param in
	 *            The bytes to stranslate
	 * @param offset
	 *            An offset into the array (where to read the bytes)
	 * @param length
	 *            The length of the string
	 * @return The string
	 */
	public static String bytesToString(byte in[], int offset, int length) {
		return new String(in, offset, length);
	}

	/**
	 * Translate an long to 8 bytes.
	 * 
	 * @param in
	 *            The long to translate
	 * @param out
	 *            An array of bytes
	 * @param offset
	 *            An offset into the array (where to write the bytes)
	 * @return The number of bytes written (always 8!)
	 */
	public static int longToBytes(long in, byte out[], int offset) {
		out[offset] = (byte) ((int) (in >>> 56) & 0xFF);
		out[offset + 1] = (byte) ((int) (in >>> 48) & 0xFF);
		out[offset + 2] = (byte) ((int) (in >>> 40) & 0xFF);
		out[offset + 3] = (byte) ((int) (in >>> 32) & 0xFF);
		out[offset + 4] = (byte) ((int) (in >>> 24) & 0xFF);
		out[offset + 5] = (byte) ((int) (in >>> 16) & 0xFF);
		out[offset + 6] = (byte) ((int) (in >>> 8) & 0xFF);
		out[offset + 7] = (byte) ((int) (in >>> 0) & 0xFF);
		return 8;
	}

	/**
	 * Translate 8 bytes to an long.
	 * 
	 * @param in
	 *            An array of bytes containing the bytes to convert
	 * @param offset
	 *            An offset into in where to start reading bytes
	 * @return The long created from the bytes in in
	 */
	public static long bytesToLong(byte in[], int offset) {
		return (((long) in[offset] << 56) & ((long) 0xFF << 56))
				| (((long) in[offset + 1] << 48) & ((long) 0xFF << 48))
				| (((long) in[offset + 2] << 40) & ((long) 0xFF << 40))
				| (((long) in[offset + 3] << 32) & ((long) 0xFF << 32))
				| (((long) in[offset + 4] << 24) & ((long) 0xFF << 24))
				| (((long) in[offset + 5] << 16) & ((long) 0xFF << 16))
				| (((long) in[offset + 6] << 8) & ((long) 0xFF << 8))
				| (((long) in[offset + 7] << 0) & ((long) 0xFF << 0));
	}

	/**
	 * Translate a byte to an short containing the unsigned byte
	 * 
	 * @param s
	 *            The byte to translate
	 * @return a short
	 */
	public static short unsignByte(byte b) {
		if (b > -1)
			return (short) b;
		return (short) (b & 0xFF);
	}

	/**
	 * Translate a short to an int containing the unsigned short
	 * 
	 * @param s
	 *            The short to translate
	 * @return an int
	 */
	public static int unsignShort(short s) {
		if (s > -1)
			return (int) s;
		byte buf[] = new byte[2];
		shortToBytes(s, buf, 0);
		return (int) (((buf[0] << 8) & (0xFF << 8)) | ((buf[1]) & (0xFF)));
	}

	/**
	 * Translate an int to a long containing the unsigned int
	 * 
	 * @param i
	 *            The int to translate
	 * @return an long
	 */
	public static long unsignInt(int i) {
		if (i > -1)
			return (long) i;
		return (long) ((long) (2l << 31) + i);
	}

	/**
	 * Translate an IPnr to a String in dot-representation
	 * 
	 * @param in
	 *            IP-number in network byteorder
	 * @return String containing the IP-number in dot-form.
	 */
	public static String IPtoString(int IP) {
		int shift;
		int addr = IP;
		String cmd = "";

		for (shift = 32; (shift -= 8) >= 0;) {
			cmd = cmd + ((addr >>> shift) & 0xff);
			if (shift > 0)
				cmd = cmd + ".";
		}
		return cmd;
	}

	/**
	 * Translate an IPnr to a String in dot-representation
	 * 
	 * @param ip
	 *            IP-number in network byteorder
	 * @return String containing the IP-number in dot-form.
	 */

	public static String IPtoString(byte ip[]) {
		return IPtoString(bytesToInt(ip, 0));
	}

	/**
	 * Translate a byte into a String in "binary"-representation. <BR>
	 * 131 -> "10000011"
	 * 
	 * @param b
	 *            The byte to translate
	 * @return The string representation
	 */
	public static String byteToBit(byte b) {
		return "" + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)
				+ (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)
				+ (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
				+ (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1);
	}

	/**
	 * Translate a int into a String in "binary"-representation. <BR>
	 * 
	 * @param b
	 *            The long to translate
	 * @return The string representation
	 */
	public static String intToBit(int l) {
		return (byteToBit((byte) ((int) (l >>> 24) & 0xFF)) + "-"
				+ byteToBit((byte) ((int) (l >>> 16) & 0xFF)) + "-"
				+ byteToBit((byte) ((int) (l >>> 8) & 0xFF)) + "-" + byteToBit((byte) ((int) (l >>> 0) & 0xFF)));

	}

	/**
	 * Translate a long into a String in "binary"-representation. <BR>
	 * 
	 * @param b
	 *            The long to translate
	 * @return The string representation
	 */
	public static String longToBit(long l) {
		return (byteToBit((byte) ((int) (l >>> 56) & 0xFF)) + "-"
				+ byteToBit((byte) ((int) (l >>> 48) & 0xFF)) + "-"
				+ byteToBit((byte) ((int) (l >>> 40) & 0xFF)) + "-"
				+ byteToBit((byte) ((int) (l >>> 32) & 0xFF)) + "-"
				+ byteToBit((byte) ((int) (l >>> 24) & 0xFF)) + "-"
				+ byteToBit((byte) ((int) (l >>> 16) & 0xFF)) + "-"
				+ byteToBit((byte) ((int) (l >>> 8) & 0xFF)) + "-" + byteToBit((byte) ((int) (l >>> 0) & 0xFF)));

	}

	/**
	 * Strip an RCS tag from esthetically unnecessary characters. For example,
	 * the string "$Revision: 1.5 $" becomes "1.1". If the tag is illegal or no
	 * information exist in the tag, the empty string is returned. For example,
	 * both "$name$" and "$revision: 1.1" would return an empty string.
	 * 
	 * @param tag
	 *            The tag to strip
	 * @return The stripped tag
	 */
	public static String stripRCSTag(String tag) {
		int start = tag.indexOf(":") + 1;
		int end = tag.lastIndexOf("$");
		if (start == 0 || end <= 1)
			return "";
		return tag.substring(start, end).trim();
	}

	/**
	 * Formats a Date following "Europeen standard" and adding zeros at the
	 * right places. Ex: 960626 14:23:21
	 * 
	 * @param date
	 *            The date to format.
	 * @return The formated string.
	 */
	public static String formatDate(Date date) {
		return formatDate(date, false);
	}

	/**
	 * Formats a Date following "Europeen standard" and adding zeros at the
	 * right places. Ex: 960626 14:23:21
	 * 
	 * @param date
	 *            The date to format.
	 * @param timezone
	 *            Include timezone info?
	 * @return The formated string.
	 */
	public static String formatDate(Date date, boolean timezone) {
		TimeZone pdt = TimeZone.getDefault();
		if ((pdt.getID().compareTo("MET") == 0)
				&& !(pdt.getRawOffset() == 60 * 60 * 1000)) {
			pdt = TimeZone.getTimeZone("ECT");
		}
		SimpleDateFormat formatter = (SimpleDateFormat) SimpleDateFormat
				.getDateTimeInstance();
		if (timezone)
			formatter.applyPattern("yyMMdd HH:mm:ss zzzz");
		else
			formatter.applyPattern("yyMMdd HH:mm:ss");
		formatter.setTimeZone(pdt);
		return formatter.format(date);
	}

	/**
	 * Formats a Date-time following "Europeen standard" and adding zeros at the
	 * right places. Ex: 14:23:21
	 * 
	 * @param date
	 *            The date to format.
	 * @return The formated string.
	 */
	public static String formatTime(Date date) {
		return formatTime(date, false);
	}

	/**
	 * Formats a Date-time following "Europeen standard" and adding zeros at the
	 * right places. Ex: 14:23:21
	 * 
	 * @param date
	 *            The date to format.
	 * @param timezone
	 *            Include timezone info?
	 * @return The formated string.
	 */
	public static String formatTime(Date date, boolean timezone) {
		TimeZone pdt = TimeZone.getDefault();
		if ((pdt.getID().compareTo("MET") == 0)
				&& !(pdt.getRawOffset() == 60 * 60 * 1000)) {
			pdt = TimeZone.getTimeZone("ECT");
		}
		SimpleDateFormat formatter = (SimpleDateFormat) SimpleDateFormat
				.getDateTimeInstance();
		if (timezone)
			formatter.applyPattern("HH:mm:ss zzzz");
		else
			formatter.applyPattern("HH:mm:ss");
		formatter.setTimeZone(pdt);
		return formatter.format(date);

		// return when;
	}

	/**
	 * Check if a string is one character long and if so add a zero at the
	 * start. This methos is used by formatDate.
	 * 
	 * @param str
	 *            The string to check
	 * @return The formatet string
	 * @see #formatDate
	 */
	public static String addZero(String str) {
		if (str.length() == 1)
			return "0" + str;
		else
			return str;
	}

	/**
	 * Transform an array of strings to a single string with one space between
	 * each item.
	 * 
	 * @param in
	 *            The array containing strings
	 * @return String The resulting string.
	 */
	public static String stringArrayToString(String in[]) {
		if (in == null || in.length == 0)
			return "";
		String str = "";
		for (int i = 0; i < in.length; i++)
			str = str + " " + in[i];
		return (str.trim());
	}

	// Do NOT remove this line!!
	String Id = "$Id: Conversion.java,v 1.5 1997/09/03 16:32:16 peppar Exp $";
}