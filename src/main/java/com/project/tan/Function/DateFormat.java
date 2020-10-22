package com.project.tan.Function;

import org.apache.flink.table.functions.ScalarFunction;

import java.security.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * UDF : 实现参数为字符串的日期解析
 * @Author zhengqiang.tan
 * @Date 2020/10/20 2:40 PM
 * @Version 1.0
 */
public class DateFormat extends ScalarFunction {

    public String eval(Timestamp t, String format) {
        return new SimpleDateFormat(format).format(t);
    }

    /**
     * 默认日期格式：yyyy-MM-dd HH:mm:ss
     *
     * @param t
     * @param format
     * @return
     */
    public static String eval(String t, String format) {
        try {
            Date originDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t);
            return new SimpleDateFormat(format).format(originDate);
        } catch (ParseException e) {
            throw new RuntimeException("日期:" + t + "解析为格式" + format + "出错");
        }
    }
}
