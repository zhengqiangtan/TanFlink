package com.project.tan.FlinkSQL;

import com.project.tan.entity.PageVisit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author zhengqiang.tan
 * @Date 2020/10/20 2:00 PM
 * @Version 1.0
 * @REF https://www.cnblogs.com/bigdata1024/p/13572466.html
 *
 */
public class RetractPvUvSQL {
    public static void main(String[] args) throws Exception {


        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new FsStateBackend("",true));
        env.enableCheckpointing(1000,CheckpointingMode.AT_LEAST_ONCE); //

//        指定At-Least-Once语义，会取消屏障对齐，即算子收到第一个输入的屏障之后不会阻塞，而是触发快照。这样一来，部分属于检查点n + 1的数据也会包括进检查点n的数据里， 当恢复时，这部分交叉的数据就会被重复处理。


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

//        ParameterTool params = ParameterTool.fromArgs(args);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<PageVisit> input = env.fromElements(
                new PageVisit("2017-09-16 09:00:00", 1001, "/page1"),
                new PageVisit("2017-09-16 09:00:00", 1001, "/page2"),

                new PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
                new PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
                new PageVisit("2017-09-16 10:30:00", 1005, "/page2"));

        // register the DataStream as table "visit_table"
        tEnv.createTemporaryView("visit_table", input, "visitTime, userId, visitPage");

        Table table = tEnv.sqlQuery(
                "SELECT " +
                        "visitTime, " +
                        "DATE_FORMAT(max(visitTime), 'HH') as ts, " +
                        "count(userId) as pv, " +
                        "count(distinct userId) as uv " +
                        "FROM visit_table " +
                        "GROUP BY visitTime");

        DataStream<Tuple2<Boolean, Row>> dataStream = tEnv.toRetractStream(table, Row.class);

        dataStream.print();

//        if (params.has("output")) {
//            String outPath = params.get("output");
//            System.out.println("Output path: " + outPath);
//            dataStream.writeAsCsv(outPath);
//        } else {
//            System.out.println("Printing result to stdout. Use --output to specify output path.");
//            dataStream.print();
//        }
        env.execute();
    }
}

//    Flink RetractStream 用true或false来标记数据的插入和撤回，返回true代表数据插入，false代表数据的撤回
//        2> (true,2017-09-16 10:30:00,10,1,1)
//        3> (true,2017-09-16 09:00:00,09,1,1)
//        2> (false,2017-09-16 10:30:00,10,1,1)
//        2> (true,2017-09-16 10:30:00,10,2,1)
//        3> (false,2017-09-16 09:00:00,09,1,1)
//        3> (true,2017-09-16 09:00:00,09,2,1)