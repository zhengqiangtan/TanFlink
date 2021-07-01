package com.flink.tan.Example_01;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @DESC 实时订单统计示例
 * https://ververica.cn/developers/apache-flink-basic-zero-iii-datastream-api-programming/
 * @Author tzq
 * @Date 2020-03-19 10:33
 *
 * 总结:
 *  DataStream API 的原理进行简要的介绍。当我们调用 DataStream#map 算法时，Flink 在底层会创建一个 Transformation 对象，
 *  这一对象就代表我们计算逻辑图中的节点。它其中就记录了我们传入的 MapFunction，也就是 UDF（User Define Function）。
 *  随着我们调用更多的方法，我们创建了更多的 DataStream 对象，每个对象在内部都有一个 Transformation 对象，这些对象根据计算依赖关系组成一个图结构，就是我们的计算图。
 *  后续 Flink 将对这个图结构进行进一步的转换，从而最终生成提交作业所需要的 JobGraph。
 *
 **/
public class GroupedProcessingTimeWindowSample {
    /**
     * Source：构建 <商品类型，成交量>  流
     */
    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        // 标记和控制执行的状态
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
                String key = "类别" + (char) ('A' + random.nextInt(3));
                int value = random.nextInt(10) + 1;

                System.out.println(String.format("Emits\t(%s, %d)", key, value));
                ctx.collect(new Tuple2<>(key, value));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);

        /**
         * 在底层，Sum 算子内部会使用 State 来维护每个Key（即商品类型）对应的成交量之和。当有新记录到达时，Sum 算子内部会更新所维护的成交量之和，
         * 并输出一条<商品类型，更新后的成交量>记录。
         *
         * 使用 Fold 方法来在算子中维护每种类型商品的成交量
         * (将所有记录输出到同一个计算节点的实例上。我们可以通过 KeyBy 并且对所有记录返回同一个 Key，将所有记录分到同一个组中，从而可以全部发送到同一个实例上)
         */
        keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "";
            }

        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                accumulator.put(value.f0, value.f1);
                return accumulator;
            }
        }).addSink(new SinkFunction<HashMap<String, Integer>>() {
            @Override
            public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                // 每个类型的商品成交量
                System.out.println("每个类型的商品成交量:" + value);
                // 商品成交总量
                System.out.println("商品成交总量:" + value.values().stream().mapToInt(v -> v).sum());
            }
        });

        env.execute();
    }
}


//        Emits	(类别C, 9)
//        每个类型的商品成交量{类别C=9}
//        商品成交总量:9
//        Emits	(类别A, 6)
//        Emits	(类别A, 8)
//        每个类型的商品成交量{类别A=6, 类别C=9}
//        商品成交总量:15
//        每个类型的商品成交量{类别A=14, 类别C=9}