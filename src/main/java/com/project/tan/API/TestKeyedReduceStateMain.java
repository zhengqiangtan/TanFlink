package com.project.tan.API;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 测试ReducingState
 */
public class TestKeyedReduceStateMain {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(16);
        //获取数据源
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(
                        Tuple2.of(1L, 3L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L),
                        Tuple2.of(1L, 5L),
                        Tuple2.of(2L, 2L),
                        Tuple2.of(2L, 6L));


        // 输出：
        //(1,5.0)
        //(2,4.0)
        dataStreamSource
                .keyBy(0)
                .flatMap(new CountAverageWithReduceState())
                .print();


        env.execute("TestStatefulApi");
    }


    /**
     * 定义平均值处理函数
     */
    static class CountAverageWithReduceState
            extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private ReducingState<Long> reducingState;

        /***状态初始化*/
        @Override
        public void open(Configuration parameters) throws Exception {

            ReducingStateDescriptor descriptor = new ReducingStateDescriptor("ReducingDescriptor", new ReduceFunction<Long>() {
                @Override
                public Long reduce(Long v1, Long v2) throws Exception {
                    return v1 + v2;
                }
            }, Long.class);

            reducingState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> collector) throws Exception {

            //将状态放入
            reducingState.add(element.f1);
            collector.collect(Tuple2.of(element.f0, reducingState.get()));

        }

    }

}