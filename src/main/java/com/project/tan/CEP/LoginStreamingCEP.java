package com.project.tan.CEP;

import com.project.tan.entity.AlertEvent;
import com.project.tan.entity.LogInEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * 连续登录场景
 * 在这个场景中，我们需要找出那些 5 秒钟内连续登录失败的账号，然后禁止用户，再次尝试登录需要等待 1 分钟。
 */
public class LoginStreamingCEP {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * 从登录消息 LogInEvent 可以得到用户登录是否成功，当检测到 5 秒钟内用户连续两次登录失败，
         * 则会发出告警消息，提示用户 1 分钟以后再试，或者这时候就需要前端输入验证码才能继续尝试
         */
        DataStream<LogInEvent> source = env.fromElements(
                new LogInEvent(1L, "fail", 1597905234000L),
                new LogInEvent(1L, "success", 1597905235000L),
                new LogInEvent(2L, "fail", 1597905236000L),
                new LogInEvent(2L, "fail", 1597905237000L),
                new LogInEvent(2L, "fail", 1597905238000L),
                new LogInEvent(3L, "fail", 1597905239000L),
                new LogInEvent(3L, "success", 1597905240000L)

        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .keyBy(new KeySelector<LogInEvent, Object>() {
            @Override
            public Object getKey(LogInEvent value) throws Exception {
                return value.getUserId();
            }
        });

        /**
         * 定义Pattern 规则
         */
        Pattern<LogInEvent, LogInEvent> pattern = Pattern.<LogInEvent>begin("start").where(new IterativeCondition<LogInEvent>() {
            @Override
            public boolean filter(LogInEvent value, Context<LogInEvent> ctx) throws Exception {
                return value.getIsSuccess().equals("fail");
            }
        }).next("next").where(new IterativeCondition<LogInEvent>() {
            @Override
            public boolean filter(LogInEvent value, Context<LogInEvent> ctx) throws Exception {
                return value.getIsSuccess().equals("fail");
            }
        }).within(Time.seconds(5));


        /**
         * 将 Pattern 和 Stream 结合在一起，在匹配到事件后先在控制台打印，并且向外发送。
         */
        PatternStream<LogInEvent> patternStream = CEP.pattern(source, pattern);



        SingleOutputStreamOperator<AlertEvent> process = patternStream.process(new PatternProcessFunction<LogInEvent, AlertEvent>() {
            @Override
            public void processMatch(Map<String, List<LogInEvent>> match, Context ctx, Collector<AlertEvent> out) throws Exception {

                List<LogInEvent> start = match.get("start");
                List<LogInEvent> next = match.get("next");
                System.err.println("start:" + start + ",next:" + next);


                out.collect(new AlertEvent(String.valueOf(start.get(0).getUserId()), "出现连续登陆失败"));
            }
        });

        process.printToErr();
        env.execute("execute cep");

    }


    /**
     * 自定义周期性水印 (时间戳和水印提取器)
     * https://www.cnblogs.com/asker009/p/11318290.html
     */
    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<LogInEvent> {

        private final long maxOutOfOrderness = 5000L;
        private long currentTimeStamp;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimeStamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(LogInEvent element, long previousElementTimestamp) {

            Long timeStamp = element.getTimeStamp();
            currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
            System.err.println(element.toString() + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));
            return timeStamp;
        }
    }


}
