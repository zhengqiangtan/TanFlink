package com.project.tan.CEP;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * flink的神奇分流器-sideoutput
 * <p>
 * 对单词长度小于5的单词进行wordcount操作，同时又想记录下来哪些单词的长度大于5
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/26 10:39 AM
 * @Version https://blog.csdn.net/rlnLo2pNEfx9c/article/details/86285634
 */
public class SideOutPutDemo_1 {
    public static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "Or to take arms against a sea of troubles,",
            "And by opposing end them?--To die,--to sleep,--",
            "No more; and by a sleep to say we end",
            "The heartache, and the thousand natural shocks",
            "That flesh is heir to,--'tis a consummation",
            "Devoutly to be wish'd. To die,--to sleep;--",
            "To sleep! perchance to dream:--ay, there's the rub;",
            "For in that sleep of death what dreams may come,",
            "When we have shuffled off this mortal coil,",
            "Must give us pause: there's the respect",
            "That makes calamity of so long life;",
            "For who would bear the whips and scorns of time,",
            "The oppressor's wrong, the proud man's contumely,",
            "The pangs of despis'd love, the law's delay,",
            "The insolence of office, and the spurns",
            "That patient merit of the unworthy takes,",
            "When he himself might his quietus make",
            "With a bare bodkin? who would these fardels bear,",
            "To grunt and sweat under a weary life,",
            "But that the dread of something after death,--",
            "The undiscover'd country, from whose bourn",
            "No traveller returns,--puzzles the will,",
            "And makes us rather bear those ills we have",
            "Than fly to others that we know not of?",
            "Thus conscience does make cowards of us all;",
            "And thus the native hue of resolution",
            "Is sicklied o'er with the pale cast of thought;",
            "And enterprises of great pith and moment,",
            "With this regard, their currents turn awry,",
            "And lose the name of action.--Soft you now!",
            "The fair Ophelia!--Nymph, in thy orisons",
            "Be all my sins remember'd."
    };

    // 测输出打上标签
    private static final OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {};


    /**
     * 以用户自定义FlatMapFunction函数的形式来实现分词器功能，该分词器会将分词封装为(word,1)，
     * 同时不接受单词长度大于5的，也即是侧输出都是单词长度大于5的单词。
     */
    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(
                String value,
                Context ctx,
                Collector<Tuple2<String, Integer>> out) throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 5) {
                    ctx.output(rejectedWordsTag, token);
                } else if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // 获取输入数据
        DataStream<String> text = env.fromElements(WORDS);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = text
                .keyBy(new KeySelector<String, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer getKey(String value) throws Exception {
                        return 0;
                    }
                })
                .process(new Tokenizer());

        // 获取侧输出
        DataStream<String> rejectedWords = tokenized
                .getSideOutput(rejectedWordsTag)
                .map(new MapFunction<String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public String map(String value) throws Exception {
                        return "rejected: " + value;
                    }
                });

        DataStream<Tuple2<String, Integer>> counts = tokenized
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1);

        // wordcount结果输出
        counts.print();
        // 侧输出结果输出
        rejectedWords.print();

        // execute program
        env.execute("Streaming WordCount SideOutput");
    }
}
