package com.project.tan.API;

import com.project.tan.entity.Item;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.ArrayList;
import java.util.List;

/**
 * KeyedStream。 在实际业务中，我们经常会需要根据数据的某种属性或者单纯某个字段进行分组，然后对不同的组进行不同的处理
 * <p>
 * https://kaiwu.lagou.com/course/courseInfo.htm?courseId=81#/detail/pc?id=2039
 * min 和 minBy 都会返回整个元素，只是 min 会根据用户指定的字段取最小值，并且把这个值保存在对应的位置，而对于其他的字段，并不能保证其数值正确。max 和 maxBy 同理。
 * <p>
 * 注意：事实上，对于 Aggregations 函数，Flink 帮助我们封装了状态数据，这些状态数据不会被清理，
 *  所以在实际生产环境中应该尽量避免在一个无限流上使用 Aggregations。
 *  而且，对于同一个 keyedStream ，只能调用一次 Aggregation 函数
 */
public class MaxApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

//        DataStreamSource<Item> items = env.fromCollection(data);
//        // 按照 Tuple3 的第一个元素进行聚合，并且按照第三个元素取最大值
//        items.keyBy(0).max(2).printToErr();
//        items.keyBy(0).maxBy(2).printToErr();


        // reduce
        DataStreamSource<Tuple3<Integer, Integer, Integer>> itemsReduce = env.fromCollection(data);
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> reduce = itemsReduce.keyBy(0)
                .reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> t1, Tuple3<Integer, Integer, Integer> t2) throws Exception {
                        Tuple3<Integer, Integer, Integer> newTuple = new Tuple3<>();
                        newTuple.setFields(0, 0, (Integer) t1.getField(2) + (Integer) t2.getField(2));
                        return newTuple;
                    }
                });

        reduce.printToErr().setParallelism(1);


        env.execute("flink");

    }
}

//        3> (0,1,0)
//        3> (0,1,1)
//        3> (0,1,2) 并不能保证其他字段正确
//        3> (0,1,3)
//        3> (1,2,5)
//        3> (1,2,9)
//        3> (1,2,11)
//        3> (1,2,13)