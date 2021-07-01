package com.flink.tan.Util;

import java.util.Base64;

public class Base64Utils {
    public static boolean isBase64Encoded(String str) {
        if (str.length() % 4 != 0) {
            return false;
        }
        char[] charArray = str.toCharArray();
        for (char c : charArray) {
            if ((c < 'A') || (c > 'Z')) {
                if ((c < 'a') || (c > 'z')) {
                    if ((c < '0') || (c > '9')) {
                        if ((c != '+') && (c != '\\') && (c != '=')) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    public static String tryUnBase64(String str) {
        try {
            return new String(Base64.getDecoder().decode(str));
        } catch (Exception e) {
        }
        return str;
    }

    public static String encode(String str) {
        return Base64.getEncoder().encodeToString(str.getBytes());
    }
}
