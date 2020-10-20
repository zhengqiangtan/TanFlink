package com.project.tan.API;

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
 * 水印 ： watermark
 * 需求：模拟一个实时接收 Socket 的 DataStream 程序，代码中使用 AssignerWithPeriodicWatermarks 来设置水印，
 * 将接收到的数据进行转换，分组并且在一个 5秒的窗口内获取该窗口中第二个元素最小的那条数据。
 *
 * nc -lk 9000
 * flink,1588659181000
 * flink,1588659182000
 * flink,1588659183000
 * flink,1588659184000
 * flink,1588659185000
 *
 * 核心问题是：maxOutOfOrderness 的设置会影响窗口的计算时间和水印的时间。你要理解这句话，
 * 假如正常的水印是100，那么当时设置允许乱序这个水印就会顺延。最后一个图中，有一条乱序消息，他的存在导致水印往后延迟了5秒钟。
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/20 5:40 PM
 * @Version 1.0
 */
public class WaterMarkDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //设置事件类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置水印生成时间间隔100ms
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> dataStream = env
                .socketTextStream("127.0.0.1", 9000)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

                    private Long currentTimeStamp = 0L;

                    //设置允许乱序时间（允许乱序时间 = 0L 即不处理乱序消息）
                    private Long maxOutOfOrderness = 0L;
//                  private Long maxOutOfOrderness = 5000L;

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
