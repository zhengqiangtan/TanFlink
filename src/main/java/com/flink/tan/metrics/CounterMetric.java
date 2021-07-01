package com.flink.tan.metrics;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.util.Random;


public class CounterMetric {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String jobName = parameterTool.get("job_name"); // -job_name test
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//		env.disableOperatorChaining();

        env.getConfig().enableForceAvro();  //强制使用avro


        SingleOutputStreamOperator<String> source = env.addSource(new customSource()).name("[自定义]");

        source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) {
                return StringUtils.isNullOrWhitespaceOnly(s) ? false : true;
            }
        }).name("[filterOperator]")
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    Counter mapDataNum;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapDataNum = getRuntimeContext()
                                .getMetricGroup().addGroup("my_count_metric")
                                .counter("mapDataNum");
                    }
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] strs = s.split("\\W+");
                        for (String w : strs) {
                            collector.collect(Tuple2.of(w, 1));
                        }
                        mapDataNum.inc();
                    }
                })
                .keyBy(0)
                .sum(1).name("[sum operator]")
                .print().name("[输出打印]");

        // execute program
        JobExecutionResult r = env.execute().getJobExecutionResult();
        System.out.println(r.getJobID());

    }

    /**
     * 自定义数据源、10s 发送一句话
     */
    static class customSource implements SourceFunction<String> {
        boolean flag = true;
        String[] str = new String[]{"我们 都是 中国人", "我们 都是 好孩子", "你好 世界"};

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (flag) {
                Thread.sleep(10000);
                sourceContext.collect(str[new Random().nextInt(3)]);
            }
        }
        @Override
        public void cancel() {
            flag = false;
        }
    }
}