package com.flink.tan.API;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title minby测试
 * @Author zhengqiang.tan
 * @Date 2021/4/24 10:22 PM
 */
public class MinByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> mapped = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {

            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] flieds = value.split(",");
                String province = flieds[0];
                String city = flieds[1];
                Double money = Double.parseDouble(flieds[2]);
                return Tuple3.of(province, city, money);
            }
        });
        KeyedStream<Tuple3<String, String, Double>, String> keyByed = mapped.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> minbyed = keyByed.minBy(2, false);
        /* minBy(比较字段,false) 将会把最小的记录的那条 flieds进行返回
         * 而其他字段(city)将保留第一次出现的字段
         * 辽宁,沈阳,1000
         * 辽宁,大连,800
         * 辽宁,锦州,800
         * ==>minby(2,false) ==> 辽宁 锦州 800
         * 第二个布尔值类型的参数意思是 假设同一个分组有几条(money)都等于800的 而
         * 且是最小的 那么要返回新的那条最小的数据的flieds;
         */
        minbyed.print();

        env.execute();

    }
}
