package com.flink.tan.PVUV;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.tan.entity.UserClick;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 *
 * 周期性水印：AssignerWithPeriodicWatermarks
 * 特定事件触发水印：PunctuatedWatermark
 *
 * 单窗口内存统计
 * 这种方法需要把一天内所有的数据进行缓存，然后在内存中遍历接收的数据，进行 PV 和 UV 的叠加统计。
 *
 *
 */
public class PVUVCount {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 检查点配置略。但后面要用到状态，所以状态后端必须预先配置，在flink-conf.yaml或者这里均可
        env.setStateBackend(new MemoryStateBackend(true));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "10");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("log_user_action", new SimpleStringSchema(), properties);
        //设置从最早的offset消费
        consumer.setStartFromEarliest();


        // 将读取到的数据反序列化为 UserClick 的 DataStream
        DataStream<UserClick> dataStream = env
                .addSource(consumer)
                .name("log_user_action")
                .map(message -> {
                    JSONObject record = JSON.parseObject(message);

                    return new UserClick(
                            record.getString("user_id"),
                            record.getLong("timestamp"),
                            record.getString("action")
                    );
                })
                .returns(TypeInformation.of(UserClick.class));


        /**
         * 水印和窗口设计
         * 由于我们的用户访问日志可能存在乱序，所以使用 BoundedOutOfOrdernessTimestampExtractor
         * 来处理乱序消息和延迟时间，我们指定消息的乱序时间 30 秒
         */
        SingleOutputStreamOperator<UserClick> userClickSingleOutputStreamOperator = dataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<UserClick>(Time.seconds(30)) {
                    @Override
                    public long extractTimestamp(UserClick element) {
                        return element.getTimestamp();
                    }
                });

        /**
         * 窗口触发器
         * 每天从 0 点开始计算并且每天都会清空数据。
         */
        userClickSingleOutputStreamOperator
                .windowAll(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(20))) //每 20 秒触发一次计算输出中间结果。
                .evictor(TimeEvictor.of(Time.seconds(0), true));


    }
}
