package com.project.tan.FlinkSQL;

import com.project.tan.entity.Item;
import com.project.tan.source.MyStreamingSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * FlinkSQL 示例：
 *
 * 实际生产中双流join并不提倡，会遇到很多状态问题。一般会把其中一个流放入Hbase这样的存储中再去关联。文中的odd和even两个流是为了模拟join所需的输入。
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/20 10:46 AM
 * @Version 1.0
 */
public class StreamingSQLDemo {
    public static void main(String[] args) throws Exception {
        // 使用Blink的执行计划解析
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        SingleOutputStreamOperator<Item> source = bsEnv.addSource(new MyStreamingSource()).map(new MapFunction<Item, Item>() {
            @Override
            public Item map(Item item) throws Exception {
                return item;
            }
        });

        DataStream<Item> evenSelect = source.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("even");
        DataStream<Item> oddSelect = source.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("odd");

        // 创建临时视图
        bsTableEnv.createTemporaryView("evenTable", evenSelect, "name,id");
        bsTableEnv.createTemporaryView("oddTable", oddSelect, "name,id");

        // 执行SQL
        Table queryTable = bsTableEnv.sqlQuery("select a.id,a.name,b.id,b.name from evenTable as a join oddTable as b on a.name = b.name");
        queryTable.printSchema();
//            root
//                |-- id: INT
//                |-- name: STRING
//                |-- id0: INT
//                |-- name0: STRING

        // Flink处理数据把表转换为流的时候,可以使用toAppendStream与toRetractStream，前者适用于数据追加的场景， 后者适用于更新，删除场景
        // 使用TypeInformation来指定列的类型
        bsTableEnv.toRetractStream(queryTable, TypeInformation.of(new TypeHint<Tuple4<Integer, String, Integer, String>>() {
            //TODO Nothing
        })).print();
        bsEnv.execute("streaming sql job");

    }
}
