package com.flink.tan.Function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


/**
 * 状态：ValueState
 * <p>
 * Operator State 的实际应用场景不如 Keyed State 多，
 * 一般来说它会被用在 Source 或 Sink 等算子上，用来保存流入数据的偏移量或对输出数据做缓存，以保证 Flink 应用的 Exactly-Once 语义。
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/20 8:41 PM
 * @Version 1.0
 * 通过继承 RichFlatMapFunction 来访问 State
 * https://kaiwu.lagou.com/course/courseInfo.htm?courseId=81#/detail/pc?id=2044
 *
 * https://www.cnblogs.com/zz-ksw/p/12973639.html
 */
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
        Tuple2<Long, Long> currentSum;
        // 访问ValueState
        if (sum.value() == null) {
            currentSum = Tuple2.of(0L, 0L);
        } else {
            currentSum = sum.value();
        }
        // 更新
        currentSum.f0 += 1;
        // 第二个元素加1
        currentSum.f1 += input.f1;
        // 更新state
        sum.update(currentSum);
        // 如果count的值大于等于2，求知道并清空state
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }


    //  StateDesciptor
    //  Flink 提供了 StateDesciptor 方法专门用来访问不同的 state，StateDesciptor 同时还可以通过 setQueryable 使状态变成可以查询状态。
    //  可查询状态目前是一个 Beta 功能，暂时不推荐使用。
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // state的名字
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})
                ); // 设置默认值
        descriptor.setQueryable("average");
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        // 获取状态的句柄
        sum = getRuntimeContext().getState(descriptor);
    }
}
