package com.project.tan.API;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * SideOutPut 分流 （推荐做法）
 * <p>
 * SideOutPut 是 Flink 框架为我们提供的最新的也是最为推荐的分流方法，可以多次进行拆分的，无需担心会爆出异常，在使用 SideOutPut 时，需要按照以下步骤进行：
 * <p>
 * 定义 OutputTag
 * 调用特定函数进行数据拆分
 * ProcessFunction
 * KeyedProcessFunction
 * CoProcessFunction
 * KeyedCoProcessFunction
 * ProcessWindowFunction
 * ProcessAllWindowFunction
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/20 9:00 PM
 * @Version 1.0
 * <p>
 * ProcessFunction 示例
 */
public class SplitStreamDemo_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据源
        List data = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        data.add(new Tuple3<>(0, 1, 0));
        data.add(new Tuple3<>(0, 1, 1));
        data.add(new Tuple3<>(0, 2, 2));
        data.add(new Tuple3<>(0, 1, 3));
        data.add(new Tuple3<>(1, 2, 5));
        data.add(new Tuple3<>(1, 2, 9));
        data.add(new Tuple3<>(1, 2, 11));
        data.add(new Tuple3<>(1, 2, 13));

        DataStreamSource<Tuple3<Integer, Integer, Integer>> items = env.fromCollection(data);


        OutputTag<Tuple3<Integer, Integer, Integer>> zeroStream = new OutputTag<Tuple3<Integer, Integer, Integer>>("zeroStream") {
        };
        OutputTag<Tuple3<Integer, Integer, Integer>> oneStream = new OutputTag<Tuple3<Integer, Integer, Integer>>("oneStream") {
        };

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> processStream = items.process(
                new ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public void processElement(Tuple3<Integer, Integer, Integer> value, Context ctx, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
                        if (value.f0 == 0) {
                            ctx.output(zeroStream, value);
                        } else if (value.f0 == 1) {
                            ctx.output(oneStream, value);
                        }
                    }
                });
        DataStream<Tuple3<Integer, Integer, Integer>> zeroSideOutput = processStream.getSideOutput(zeroStream);
        DataStream<Tuple3<Integer, Integer, Integer>> oneSideOutput = processStream.getSideOutput(oneStream);


        zeroSideOutput.print();
        oneSideOutput.printToErr();

        env.execute("SplitStreamDemo_3");

    }

}
