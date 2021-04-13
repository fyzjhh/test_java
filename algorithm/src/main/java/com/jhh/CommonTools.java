package com.jhh;

import java.text.DateFormat;
import java.text.SimpleDateFormat;


/**
 * Tools class
 */
public class CommonTools {

    public static String COMMA = ",";
    public static String TAB = "	";
    public static String COLON = ":";
    public static String SPACE = " ";
    public static String POINT = ".";
    public static String EMPTY = "";
    public static String UNDERLINE = "_";
    public static String NEWLINE = "\n";
    public static String WAVY = "~";
    public static String SEMI = ";";
    public static String S_QUOTE = "'";
    public static String D_QUOTE = "\"";



    DateFormat datetimeformat = new SimpleDateFormat(
            "yyyy-MM-dd_HH:mm:ss");
    DateFormat spacedatetimeformat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");
    DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");

    String str1, str2, str3 = EMPTY;


    private String repfile(String f) {
        if (f != null) {
            return f.replace("\\", "fxg").replace("/", "xg").replace("?", "wh")
                    .replace("\t", "tab").replace("<", "xyh")
                    .replace(">", "dyh").replace("\"", "syh")
                    .replace("|", "sx").replace("*", "xh").replace(":", "mh");
        } else {
            return null;
        }
    }

}



