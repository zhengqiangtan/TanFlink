package com.project.tan.WaterMark;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * WindowWaterMark DMEO
 *
 * 我们使用的 AssignerWithPeriodicWatermarks 来自定义水印发射器和时间戳提取器，设置允许乱序时间为 5 秒，并且在一个 5 秒的窗口内进行聚合计算。
 * 在这个案例中，可以看到如何正确使用 Flink 提供的 API 进行水印和时间戳的设置
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/21 8:52 PM
 * @Version 1.0
 */
public class WindowWaterMark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //设置为eventtime事件类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置水印生成时间间隔100ms
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> dataStream = env
                .socketTextStream("127.0.0.1", 9000)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    private Long currentTimeStamp = 0L;

                    //设置允许乱序时间

                    private Long maxOutOfOrderness = 5000L;
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimeStamp - maxOutOfOrderness);

                    }

                    @Override

                    public long extractTimestamp(String s, long l) {

                        String[] arr = s.split(",");

                        long timeStamp = Long.parseLong(arr[1]);

                        currentTimeStamp = Math.max(timeStamp, currentTimeStamp);

                        System.err.println(s + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));

                        return timeStamp;

                    }

                });

        dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<String, Long>(split[0], Long.parseLong(split[1]));

            }

        })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .minBy(1)
                .print();
        env.execute("WaterMark Test Demo");

    }

}

