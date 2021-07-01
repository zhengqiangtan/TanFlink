package com.flink.tan.PVUV;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.tan.Util.DateUtil;
import com.flink.tan.entity.UserClick;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * 使用 BitMap 或者布隆过滤器进行去重
 * <p>
 * 假如用户的 ID 可以转化为 Long 型，可以使用 BitMap 进行去重计算 UV
 */
public class PVUVCountBitMap {

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


        SingleOutputStreamOperator<UserClick> userClickSingleOutputStreamOperator = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserClick>(Time.seconds(30)) {
            @Override
            public long extractTimestamp(UserClick element) {
                return element.getTimestamp();
            }
        });

        userClickSingleOutputStreamOperator
                .keyBy(new KeySelector<UserClick, String>() {
                    @Override
                    public String getKey(UserClick value) throws Exception {
                        return DateUtil.timeStampToDate(value.getTimestamp());
                    }
                })

//        我们可以使用 offset 使我们的时区以0时区为准。比如我们生活在中国，时区是
//        UTC+08:00，可以指定一个 Time.hour(-8)，使时间以0时区为准。
//                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))// 第二个参数 offset，它的作用是改变窗口的时间。
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(20))) // 触发器定义了何时会触发窗口的执行函数的计算
                .evictor(TimeEvictor.of(Time.seconds(0), true)) // 触发器触发后，执行函数执行前，移除一些元素。
                .process(new PvUvProcessWindowFunction());
    }
}
