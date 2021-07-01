package com.flink.tan.PVUV;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.tan.Util.DateUtil;
import com.flink.tan.entity.UserClick;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
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
 * kafka ->flink -> mysql 示例
 * JDBC Sink 可以保证 "at-least-once" 语义保障，可通过实现“有则更新、无则写入”来实现写入 MySQL 的幂等性来实现 "exactly-once" 语义。
 */
public class PVUVCountKeyByIdMysqlSink {

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


        /**
         * 使用MySQLSink
         */
        String driverClass = "com.mysql.jdbc.Driver";
        String dbUrl = "jdbc:mysql://127.0.0.1:3306/test";
        String userNmae = "root";
        String passWord = "root";

        userClickSingleOutputStreamOperator
                .keyBy(new KeySelector<UserClick, String>() {
                    @Override
                    public String getKey(UserClick value) throws Exception {
                        return DateUtil.timeStampToDate(value.getTimestamp());
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(20)))
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new PvUvProcessWindowFunction())
                .addSink(
                        JdbcSink.sink(
                                "replace into pvuv_result (type,value) values (?,?)",
                                (ps, value) -> {
                                    ps.setString(1, value.f1);
                                    ps.setInt(2, value.f2);

                                },
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(dbUrl)
                                        .withDriverName(driverClass)
                                        .withUsername(userNmae)
                                        .withPassword(passWord)
                                        .build())
                );
    }
}
