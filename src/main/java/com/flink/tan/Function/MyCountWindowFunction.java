package com.flink.tan.Function;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Title 自定义窗口函数，封装成字符串
 * 第一个参数是上一阶段的统计
 * 第二个参数 输出
 * 第三个就是 窗口类 能获取窗口结束时间
 * @Author zhengqiang.tan
 * @Date 2020/10/27 5:58 PM
 */


public class MyCountWindowFunction implements WindowFunction<Long, String, String, TimeWindow> {

    @Override
    public void apply(String productId, TimeWindow window, Iterable<Long> input, Collector<String> out) throws Exception {
        /*商品访问统计输出*/
        /*out.collect("productId"productId,window.getEnd(),input.iterator().next()));*/
        out.collect("----------------窗口时间：" + window.getEnd());
        out.collect("商品ID: " + productId + "  浏览量: " + input.iterator().next());
    }

}

