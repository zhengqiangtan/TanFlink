package com.project.tan.kafka;

import com.project.tan.source.MyNoParalleSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author zhengqiang.tan
 * @Date 2020/10/21 5:51 PM
 * @Version 1.0
 */
public class KafkaProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
//        new FlinkKafkaProducer<>("test",new SimpleStringSchema(),properties);

        // 2.0 配置 KafkaProducer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                "127.0.0.1:9092", //broker 列表
                "test",           //topic
                new SimpleStringSchema()); // 消息序列化

        //写入 Kafka 时附加记录的事件时间戳
        producer.setWriteTimestampToKafka(true);
        text.addSink(producer);
        env.execute();

    }


}
