package com.project.tan.Basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by tzq on 2019/5/17.
 */
public class FlatMap_API {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements("flink vs spark", "buffer vs shuffle");

        DataSet<String> ds1 = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) {
                out.collect(value.toUpperCase() + "--##bigdata##");
            }
        });

        ds1.print();
        System.out.println(" ---------- ");


        DataSet<String[]> ds2 = text.flatMap(new FlatMapFunction<String, String[]>() {
            @Override
            public void flatMap(String value, Collector<String[]> out) {
                out.collect(value.toUpperCase().split("\\s+"));
            }
        });
        ds2.print();

    }


}
