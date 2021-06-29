package com.project.tan.helloword;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.util.Random;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String jobName = parameterTool.get("job_name"); // -job_name test

        System.out.println("---->" + jobName);



//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.disableOperatorChaining();

        //使用本地模式并开启WebUI
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);
//		env.disableOperatorChaining();

//		DataStreamSource<String> socketStream = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<String> source = env.addSource(new customSource()).name("[自定义]");

        source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) {
                return StringUtils.isNullOrWhitespaceOnly(s) ? false : true;
            }
        }).name("[filterOperator]")
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//
//                    @Override
//                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                        String[] split = s.split("\\W+");
//                        for (String w : split) {
//                            collector.collect(Tuple2.of(w, 1));
//                        }
//                    }
//                }).name("[flatMapOperator]")

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


///**
// * 流式计算
// *
// * nc -lk 9000 监听一个本地的 Socket 端口，并且使用 Flink 中的滚动窗口，每 3 秒打印一次计算结果
// */
//public class StreamingJob {
//
//    public static void main(String[] args) throws Exception {
//        // set up the streaming execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");
//        DataStream<WordWithCount> wordCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
//            @Override
//            public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
//                for (String word : value.split("\\s")) {
//                    collector.collect(new WordWithCount(word, 1L));
//                }
//            }
//        })
//                .keyBy("word")
//                .timeWindow(Time.seconds(3), Time.seconds(1))  // 定义了一个滑动窗口，窗口大小3秒，每1秒滑动一次
//                .reduce(new ReduceFunction<WordWithCount>() {
//                    @Override
//                    public WordWithCount reduce(WordWithCount w1, WordWithCount w2) throws Exception {
//                        return new WordWithCount(w1.word, w1.count + w2.count);
//                    }
//                });
//
//        wordCounts.print().setParallelism(1);
//
//
//        // execute program
//        env.execute("Socket window wordcount");
//    }
//
//
//    public static class WordWithCount {
//        public String word;
//        public long count;
//
//        public WordWithCount() {
//        }
//
//        public WordWithCount(String word, long count) {
//            this.word = word;
//            this.count = count;
//        }
//
//        @Override
//        public String toString() {
//            return "WordWithCount{" +
//                    "word='" + word + '\'' +
//                    ", count=" + count +
//                    '}';
//        }
//    }
//}
