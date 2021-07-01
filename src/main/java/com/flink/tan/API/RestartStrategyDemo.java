package com.flink.tan.API;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 重启策略
 * 在实际生产环境中由于每个任务的负载和资源消耗不一样，我们推荐在代码中指定每个任务的重试机制和重启策略。
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/20 3:32 PM
 * @Version 1.0
 */
public class RestartStrategyDemo {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1. 无重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 2.固定延迟重启策略模式
        // flink-conf.yaml 中设置如下配置参数，来启用此策略：restart-strategy: fixed-delay
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 重启次数
                Time.of(5, TimeUnit.SECONDS) // 时间间隔
        ));

        // 3. 失败率重启策略模式
        // 假如 5 分钟内若失败了 3 次，则认为该任务失败，每次失败的重试间隔为 5 秒。

        // restart-strategy.failure-rate.max-failures-per-interval: 3
        // restart-strategy.failure-rate.failure-rate-interval: 5 min
        // restart-strategy.failure-rate.delay: 5 s

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // 每个时间间隔的最大故障次数
                Time.of(5, TimeUnit.MINUTES), // 测量故障率的时间间隔
                Time.of(5, TimeUnit.SECONDS) //  每次任务失败时间间隔
        ));


    }
}
