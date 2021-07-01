package com.flink.tan.Function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 * ProcessWindowFunction 用来进行全量聚合，窗口中需要维护全部原始数据，当窗口触发计算时，则进行全量聚合。
 * ProcessWindowFunction 中有一个比较重要的对象，那就是 Context，可以用来访问事件和状态信息。
 * 但 ProcessWindowFunction 中的数据不是增量聚合，所以会使得资源消耗变大。
 *
 * 除了上述的用法，ProcessWindowFunction 还可以结合 ReduceFunction、AggregateFunction，或者 FoldFunction 来做增量计算。
 *
 * 功能：实现针对窗口的分组统计功能：
 * @Author zhengqiang.tan
 * @Date 2020/10/22 5:39 PM
 * @Version 1.0
 */
public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
        long count = 0;
        for (Tuple2<String, Long> in : input) {
            count++;
        }
        out.collect("Window: " + context.window() + "count: " + count);
    }
}