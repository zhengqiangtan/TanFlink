package com.project.tan.API;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 设置并行度
 * 这四种级别的配置生效优先级如下：算子级别 > 执行环境级别 > 提交任务级别 > 系统配置级别。
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/20 3:41 PM
 * @Version 1.0
 */
public class ParallelismDemo {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 执行环境级别
        env.setParallelism(10);

        StreamExecutionEnvironment.getExecutionEnvironment();


        // 算子级别
//        DataSet<Tuple2<String, Integer>> counts =
//
//                text.flatMap(new LineSplitter())
//
//                        .groupBy(0)
//
//                        .sum(1).setParallelism(1);

        // 提交任务级别
        // ./bin/flink run -p 10 WordCount.jar


        // 系统配置级别
        //  flink-conf.yaml 中的一个配置：parallelism.default
    }
}
