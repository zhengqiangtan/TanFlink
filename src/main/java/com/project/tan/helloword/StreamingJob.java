package com.project.tan.helloword;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 流式计算
 *
 * nc -lk 9000 监听一个本地的 Socket 端口，并且使用 Flink 中的滚动窗口，每 3 秒打印一次计算结果
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * https://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */


        DataStream<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");
        DataStream<WordWithCount> wordCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
                for (String word : value.split("\\s")) {
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        })
                .keyBy("word")
                .timeWindow(Time.seconds(3), Time.seconds(1))  // 定义了一个滑动窗口，窗口大小3秒，每1秒滑动一次
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount w1, WordWithCount w2) throws Exception {
                        return new WordWithCount(w1.word, w1.count + w2.count);
                    }
                });

        wordCounts.print().setParallelism(1);


        // execute program
        env.execute("Socket window wordcount");
    }


    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
