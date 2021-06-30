package com.project.tan.udf;


import java.util.Base64;

import org.apache.flink.table.functions.ScalarFunction;

public class UnBase64 extends ScalarFunction {
    public String toString() {
        return getClass().getName() + "@" + Integer.toHexString(hashCode());
    }

    public String filter(String content) {
        if ((content == null) || (content.isEmpty())) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < content.length(); i++) {
            char ch = content.charAt(i);
            if ((ch >= ' ') && (ch < '')) {
                stringBuilder.append(ch);
            }
        }
        return stringBuilder.toString();
    }

    public boolean isBase64Encode(String value) {
        if ((value == null) || (value.length() == 0)) {
            return false;
        }
        if (value.length() % 4 != 0) {
            return false;
        }
        char[] chrs = value.toCharArray();
        for (char chr : chrs) {
            if (((chr < 'a') || (chr > 'z')) && ((chr < 'A') || (chr > 'Z')) && ((chr < '0') || (chr > '9')) && (chr != '+') && (chr != '/') && (chr != '=')) {
                return false;
            }
        }
        return true;
    }

    public String eval(String input) {
        String result = "";
        try {
            if (isBase64Encode(input)) {
                try {
                    result = new String(Base64.getDecoder().decode(input.getBytes()));
                    result = filter(result);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            } else {
                result = input;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        UnBase64 instance = new UnBase64();

        String payload = "WADj07dtAAwpb5ljCABFAAMNTe5AAEAGzaUKQgPxCkID48iwH5EvymJJOpnJXFAYAfYfVwAAR0VUIC9EZWRlQ01TLVY1LjctVVRGOC1TUDItRnVsbC91cGxvYWRzL2RlZGUvc3lzX3ZlcmlmaWVzLnBocD9hY3Rpb249Z2V0ZmlsZXMmcmVmaWxlc1tdPVxcJTIyO3N5c3RlbSglMjdpcGNvbmZpZyUyNyk7Ly8gSFRUUC8xLjENCkhvc3Q6IDEwLjY2LjMuMjI3OjgwODENClVzZXItQWdlbnQ6IE1vemlsbGEvNS4wIChYMTE7IExpbnV4IHg4Nl82NDsgcnY6NjguMCkgR2Vja28vMjAxMDAxMDEgRmlyZWZveC82OC4wDQpBY2NlcHQ6IHRleHQvaHRtbCxhcHBsaWNhdGlvbi94aHRtbCt4bWwsYXBwbGljYXRpb24veG1sO3E9MC45LCovKjtxPTAuOA0KQWNjZXB0LUxhbmd1YWdlOiB6aC1DTix6aDtxPTAuOCx6aC1UVztxPTAuNyx6aC1ISztxPTAuNSxlbi1VUztxPTAuMyxlbjtxPTAuMg0KQWNjZXB0LUVuY29kaW5nOiBnemlwLCBkZWZsYXRlDQpDb25uZWN0aW9uOiBrZWVwLWFsaXZlDQpDb29raWU6IFBIUFNFU1NJRD1iM2E0OWU3YWY4NDQyZTY3MjhhYjlhM2U3ZmQyYjdiZTsgX2NzcmZfbmFtZV81YmZkNGRlMD02ZTE5NmUxNThkNGIxMGQzNGMwNjBiNmE1MDFhZjhiNTsgX2NzcmZfbmFtZV81YmZkNGRlMF9fY2tNZDU9NGMxZmNjZDA2YTI5MzVjZTsgRGVkZVVzZXJJRD0xOyBEZWRlVXNlcklEX19ja01kNT0zMzZlMTc0ZjE1MDc2MzdjOyBEZWRlTG9naW5UaW1lPTE1OTk1MzU4MzM7IERlZGVMb2dpblRpbWVfX2NrTWQ1PWMyMmI2NmE4YTZlOTc5ODANClVwZ3Jh";
        String valuetest = instance.eval(payload);
        System.out.println(valuetest);
    }
}