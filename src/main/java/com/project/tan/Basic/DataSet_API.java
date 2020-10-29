package com.project.tan.Basic;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * DataSet API TEST
 * Created by tzq on 2019/5/17.
 */
public class DataSet_API {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<Tuple3<String, String, Double>> tuples = env.fromElements(
                new Tuple3<>("lisi", "shandong", 2400.00),
                new Tuple3<>("zhangsan", "hainan", 2600.00),
                new Tuple3<>("wangwu", "shandong", 2400.00),
                new Tuple3<>("zhaoliu", "hainan", 2600.00),
                new Tuple3<>("xiaoqi", "guangdong", 2400.00),
                new Tuple3<>("xiaoba", "henan", 2600.00)
        );

        DataSet<Tuple3<String, String, Double>> ds1 = tuples.first(2);
        DataSet<Tuple3<String, String, Double>> ds2 = tuples.groupBy(1).first(2);
        DataSet<Tuple3<String, String, Double>> ds3 = tuples.groupBy(1).sortGroup(2, Order.ASCENDING).first(3);
        System.out.println("================");
        ds1.print();
        System.out.println("================");
        ds2.print();
        System.out.println("================");
        ds3.print();


    }

}
