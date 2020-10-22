package com.project.tan.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author zhengqiang.tan
 * @Date 2020/10/22 8:09 PM
 * @Version 1.0
 */
public class RedisSink01 implements RedisMapper<Tuple2<String, String>> {

    /**
     * 设置 Redis 数据类型
     * 其中 getCommandDescription 定义了存储到 Redis 中的数据格式，这里我们定义的 RedisCommand 为 SET，将数据以字符串的形式存储
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);

    }

    /**
     * 设置SET Key
     */
    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;

    }

    /**
     * 设置SET value
     */
    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;

    }

}
