package com.flink.tan.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * KakfaConsumer
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/21 7:12 PM
 * @Version 1.0
 */
public class KakfaConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // 动态分区发现: 每隔 1s 会动态获取 Topic 的元数据，对于新增的 Partition 会自动从最早的位点开始消费数据
        properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "1000");


        // 如果你是0.8版本的Kafka，需要配置
        //properties.setProperty("zookeeper.connect", "localhost:2181");

        //设置消费组
        properties.setProperty("group.id", "group_test");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        //设置从最早的offset消费
        consumer.setStartFromEarliest();

        //还可以手动指定相应的 topic, partition，offset,然后从指定好的位置开始消费
        //HashMap<KafkaTopicPartition, Long> map = new HashMap<>();
        //map.put(new KafkaTopicPartition("test", 1), 10240L);

        //假如partition有多个，可以指定每个partition的消费位置
        //map.put(new KafkaTopicPartition("test", 2), 10560L);

        //然后各个partition从指定位置消费
        //consumer.setStartFromSpecificOffsets(map);

        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                System.out.println(value);
            }

        });

        env.execute("start consumer ...");

    }

}
