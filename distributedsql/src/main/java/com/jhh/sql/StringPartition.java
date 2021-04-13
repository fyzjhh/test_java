package com.test.java;


public class TestString {
    public static int FLAG4MIN = -1;
    public static int FLAG4MAX = 1;

    public static int PARTITION_CHAR_LENGTH = 4;
    public static int TOTAL_CHAR_COUNT = 256;

    public static void main(String[] args) throws Exception {

        test1(args);
    }


    private static void test1(Object[] args) {
        String minStr = "";
        String maxStr = "O";

        long all_record_count = 1000000;
        long partition_record_count = 200000;


        doStringPartition(minStr, maxStr, all_record_count, partition_record_count);


    }

    private static void doStringPartition(String minStr, String maxStr, long totalRecordCount, long partitionRecordCount) {
        String str = null;

        String targetMinStr = getTargetString(minStr, FLAG4MIN);
        long minCode = getCode4String(targetMinStr);

        String targetMaxStr = getTargetString(maxStr, FLAG4MAX);
        long maxCode = getCode4String(targetMaxStr);

        long diff = (long) Math.ceil(maxCode - minCode);

        long partitionCount = (long) Math.ceil(totalRecordCount / partitionRecordCount);

        long step = (long) Math.ceil(diff / partitionCount);

        str = String.format("minStr:'%s' , targetMinStr:'%s' , minCode:'%s' ", minStr, targetMinStr, minCode);
        System.out.println(str);

        str = String.format("maxStr:'%s' , targetMaxStr:'%s' , maxCode:'%s' ", maxStr, targetMaxStr, maxCode);
        System.out.println(str);

        str = String.format("diff:'%s' , partitionCount:'%s' , step:'%s' ", diff, partitionCount, step);
        System.out.println(str);

        for (long i = 0; i < partitionCount; i++) {
            long startCode = step * i + minCode;
            long stopCode = step * i + step + minCode;

            String tmpStartStr = getString4Code(startCode);
            String tmpStopStr = getString4Code(stopCode);

            str = String.format("%d Str: '%s' '%s' ", i, tmpStartStr, tmpStopStr);
            System.out.println(str);
            str = String.format("%d Code: '%d' '%d' ", i, startCode, stopCode);
            System.out.println(str);

            System.out.println();
        }
    }

    private static String getString4Code(long code) {
        long tmpCode = code;
        char[] charArray = new char[PARTITION_CHAR_LENGTH];
        for (int i = 0; i < PARTITION_CHAR_LENGTH; i++) {
            charArray[PARTITION_CHAR_LENGTH - i - 1] = (char) (tmpCode % TOTAL_CHAR_COUNT);
            tmpCode = tmpCode / TOTAL_CHAR_COUNT;
        }
        return String.valueOf(charArray);
    }

    private static long getCode4String(String str) {
        long minCode = 0L;
        for (int i = 0; i < str.length(); i++) {
            char charCode = str.charAt(i);
            minCode = minCode * TOTAL_CHAR_COUNT + charCode;
        }
        return minCode;
    }

    private static String getTargetString(String str, int flag) {

        char[] charArray = new char[PARTITION_CHAR_LENGTH];

        if (str != null) {
            if (str.length() < PARTITION_CHAR_LENGTH) {
                for (int i = 0; i < PARTITION_CHAR_LENGTH; i++) {
                    if (i < str.length() - 1) {
                        charArray[i] = str.charAt(i);
                    } else if (i == str.length() - 1) {
                        if (flag == FLAG4MIN) {
                            charArray[i] = (char) (str.charAt(i) - (char) 1);
                        }
                        if (flag == FLAG4MAX) {
                            charArray[i] = (char) (str.charAt(i) + (char) 1);
                        }
                    } else {
                        charArray[i] = ' ';
                    }
                }
            } else {
                for (int i = 0; i < PARTITION_CHAR_LENGTH; i++) {
                    if (i < PARTITION_CHAR_LENGTH - 1) {
                        charArray[i] = str.charAt(i);
                    } else {
                        if (flag == FLAG4MIN) {
                            charArray[i] = (char) (str.charAt(i) - (char) 1);
                        }
                        if (flag == FLAG4MAX) {
                            charArray[i] = (char) (str.charAt(i) + (char) 1);
                        }
                    }
                }
            }
        }

        String targetStr = String.valueOf(charArray);
        return targetStr;
    }

}
