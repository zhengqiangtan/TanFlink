package com.project.tan.Function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 自定义的 AverageAggregate 用来计算输入数据第二个字段的平均值
 * AggregateFunction 更加通用，它有 3 个参数：输入类型（IN）、累加器类型（ACC）和输出类型（OUT）。
 * <p>
 * createAccumulator()：用来创建一个累加器，负责将输入的数据进行迭代
 * <p>
 * add()：该函数是用来将输入的每条数据和累加器进行计算的具体实现
 * <p>
 * getResult()：从累加器中获取计算结果
 * <p>
 * merge()：将两个累加器进行合并
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/22 5:33 PM
 * @Version 1.0
 */
public class AverageAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return new Tuple2<>(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
        return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        return ((double) accumulator.f0) / accumulator.f1;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}
