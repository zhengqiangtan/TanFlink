package com.project.tan.TEST;

import com.project.tan.sink.RedisSink01;
import com.project.tan.sink.SelfRedisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

/**
 * RedisSink Test
 * 开源实现的 Redis Connector 使用非常方便，但是有些功能缺失，例如，无法使用一些 Jedis 中的高级功能如设置过期时间等。
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/22 8:15 PM
 * @Version 1.0
 */
public class RedisSinkTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, String>> stream = env.fromElements("flink", "spark", "storm").map(new MapFunction<String, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>(s, s.toUpperCase());
            }

        });

        // 1. 使用flink redis Sink 组件
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).setPassword("123456").build();
        stream.addSink(new RedisSink<>(conf, new RedisSink01()));

       // 2. 使用jedis
        // stream.addSink(new SelfRedisSink());


        env.execute("redis sink test");

    }

}


//macbook:~ tandemac$ redis-cli -h 127.0.0.1 -p 6379 -a 123456
//        Warning: Using a password with '-a' or '-u' option on the command line interface may not be safe.
//        127.0.0.1:6379> get flink
//        "FLINK"
//        127.0.0.1:6379> get spark
//        "SPARK"