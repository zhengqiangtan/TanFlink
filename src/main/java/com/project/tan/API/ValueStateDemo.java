package com.project.tan.API;

import com.project.tan.Function.CountWindowAverage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 状态测试
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/20 8:46 PM
 * @Version 1.0
 */
public class ValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 5L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .printToErr();
        env.execute("ValueStateDemo");
    }
}
