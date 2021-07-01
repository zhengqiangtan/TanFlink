package com.flink.tan.TopN;


import com.alibaba.fastjson.JSON;
import com.flink.tan.entity.OrderDetail;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 求所有用户中下单金额最多的 Top 10 用户，这里就会用到 windowAll 函数
 */
public class TopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 设置事件事件
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE); // 设置检查点间隔 60s
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        //从最早开始消费
        consumer.setStartFromEarliest();

        DataStream<String> stream = env.addSource(consumer);

        DataStream<OrderDetail> orderStream = stream.map(message -> JSON.parseObject(message, OrderDetail.class));


        /**订单消息流可能存在乱序的问题，我们在这里设置允许乱序时间为 30 秒，并且设置周期性水印*/
        DataStream<OrderDetail> dataStream = orderStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<OrderDetail>() {

            private Long currentTimeStamp = 0L;
            //设置允许乱序时间
            private Long maxOutOfOrderness = 30000L;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimeStamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(OrderDetail element, long previousElementTimestamp) {
                return element.getTimeStamp();
            }
        });

        /**
         * 首先对输入流按照用户 ID 进行分组，并且自定义了一个滑动窗口，
         * 即 SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20))，
         * 表示定义一个总时间长度为 600 秒，每 20 秒向后滑动一次的滑动窗口。
         * */
        DataStream<OrderDetail> reduce = dataStream
                .keyBy((KeySelector<OrderDetail, Object>) value -> value.getUserId())
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20)))
                .reduce(new ReduceFunction<OrderDetail>() {
                    @Override
                    public OrderDetail reduce(OrderDetail value1, OrderDetail value2) throws Exception {
                        return new OrderDetail(
                                value1.getUserId(), value1.getItemId(), value1.getCiteName(), value1.getPrice() + value2.getPrice(), value1.getTimeStamp()
                        );
                    }
                });


        /**
         * 我们的订单数据会按照用户维度每隔 20 秒进行一次计算，并且通过 windowAll 函数将所有的数据汇聚到一个窗口
         * 注意，windowAll 是一个并发度为 1 的特殊操作，也就是所有元素都会进入到一个窗口内进行计算。
         *
         * 如何取得Top10 ？
         * 我们定义了一个 TreeMap，TreeMap 存储 K-V 键值对，通过红黑树（R-B tree）实现，红黑树结构天然支持排序，默认情况下通过 Key 值的自然顺序进行排序。
         * 我们设置的 TreeMap 大小是 10，如果新的数据到来后，TreeMap 的数据已经到达 10个，那么就会进行比较，将较小的删除
         *
         * */
        DataStream<Tuple2<Double, OrderDetail>> process = reduce.windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
                .process(new ProcessAllWindowFunction<OrderDetail, Tuple2<Double, OrderDetail>, TimeWindow>() {
                             @Override
                             public void process(Context context, Iterable<OrderDetail> elements, Collector<Tuple2<Double, OrderDetail>> out) throws Exception {
                                 TreeMap<Double, OrderDetail> treeMap = new TreeMap<Double, OrderDetail>(new Comparator<Double>() {
                                     @Override
                                     public int compare(Double x, Double y) {
                                         return (x < y) ? -1 : 1;
                                     }
                                 });

                                 Iterator<OrderDetail> iterator = elements.iterator();
                                 if (iterator.hasNext()) {
                                     treeMap.put(iterator.next().getPrice(), iterator.next());
                                     if (treeMap.size() > 10) {
                                         treeMap.pollLastEntry(); // TreeMap已排过序，会将最小的删除
                                     }
                                 }

                                 for (Map.Entry<Double, OrderDetail> entry : treeMap.entrySet()) {
                                     out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                                 }
                             }
                         }
                );


        /**把结果存入 Redis 中，结构为 HASH */
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
        process.addSink(new RedisSink<>(conf, new RedisMapper<Tuple2<Double, OrderDetail>>() {

            private final String TOPN_PREFIX = "TOPN:";

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, TOPN_PREFIX);
            }

            @Override
            public String getKeyFromData(Tuple2<Double, OrderDetail> data) {
                return String.valueOf(data.f0);
            }

            @Override
            public String getValueFromData(Tuple2<Double, OrderDetail> data) {
                return String.valueOf(data.f1.toString());
            }
        }));

        env.execute("TopN");

    }
}
