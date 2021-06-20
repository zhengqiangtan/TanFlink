package com.project.tan.PVUV;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.project.tan.Function.MyProcessWindowFunction;
import com.project.tan.Util.DateUtil;
import com.project.tan.entity.UserClick;
import com.project.tan.sink.MyRedisSink;
import com.project.tan.sink.RedisSink01;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Properties;

/**
 *
 * 分组窗口 + 过期数据剔除 解决翻滚窗口触发器缓存一天数据进行计算的问题
 *
 * 触发器类型:
 * EventTimeTrigger：通过对比 Watermark 和窗口的 Endtime 确定是否触发窗口计算，如果 Watermark 大于 Window EndTime 则触发，否则不触发，窗口将继续等待。
 * ProcessTimeTrigger：通过对比 ProcessTime 和窗口 EndTime 确定是否触发窗口，如果 ProcessTime 大于 EndTime 则触发计算，否则窗口继续等待。
 * ContinuousEventTimeTrigger：根据间隔时间周期性触发窗口或者 Window 的结束时间小于当前 EndTime 触发窗口计算。
 * ContinuousProcessingTimeTrigger：根据间隔时间周期性触发窗口或者 Window 的结束时间小于当前 ProcessTime 触发窗口计算。
 * CountTrigger：根据接入数据量是否超过设定的阈值判断是否触发窗口计算。
 * DeltaTrigger：根据接入数据计算出来的 Delta 指标是否超过指定的 Threshold 去判断是否触发窗口计算。
 * PurgingTrigger：可以将任意触发器作为参数转换为 Purge 类型的触发器，计算完成后数据将被清理。
 *
 * 剔除器
 * Flink 中的剔除器可以在 Window Function 执行前或后使用，用来从 Window 中剔除元素。目前 Flink 支持了三种类型的剔除器，具体如下。
 *
 * CountEvictor：数量剔除器。在 Window 中保留指定数量的元素，并从窗口头部开始丢弃其余元素。
 * DeltaEvictor：阈值剔除器。计算 Window 中最后一个元素与其余每个元素之间的增量，丢弃增量大于或等于阈值的元素。
 * TimeEvictor： 时间剔除器。保留 Window 中最近一段时间内的元素，并丢弃其余元素。
 *
 *
 */
public class PVUVCountKeyById {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 检查点配置略。但后面要用到状态，所以状态后端必须预先配置，在flink-conf.yaml或者这里均可
        env.setStateBackend(new MemoryStateBackend(true));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "10");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("log_user_action", new SimpleStringSchema(), properties);
        //设置从最早的offset消费
        consumer.setStartFromEarliest();


        DataStream<UserClick> dataStream = env
                .addSource(consumer)
                .name("log_user_action")
                .map(message -> {
                    JSONObject record = JSON.parseObject(message);

                    return new UserClick(
                            record.getString("user_id"),
                            record.getLong("timestamp"),
                            record.getString("action")
                    );
                })
                .returns(TypeInformation.of(UserClick.class));


        SingleOutputStreamOperator<UserClick> userClickSingleOutputStreamOperator = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserClick>(Time.seconds(30)) {
            @Override
            public long extractTimestamp(UserClick element) {
                return element.getTimestamp();
            }
        });

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();

        //
        userClickSingleOutputStreamOperator
                // 把 DataStream 按照用户的访问时间所在天进行分组
                .keyBy(new KeySelector<UserClick, String>() {
                    @Override
                    public String getKey(UserClick value) throws Exception {
                        return DateUtil.timeStampToDate(value.getTimestamp());
                    }
                })
                // 指定在中国的 0 点开始创建窗口，然后每天计算一次输出结果即可
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                // 设置触发器，以一定的频率输出中间结果
                // 根据间隔时间周期性触发窗口或者 Window 的结束时间小于当前 ProcessTime 触发窗口计算。每 20 秒触发一次计算输出中间结果
                // 注意：这种方法代码简单清晰，但是有很严重的内存占用问题。如果我们的数据量很大，那么所定义的 TumblingProcessingTimeWindows 窗口会缓存一整天的数据，内存消耗非常大。
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(20)))
                // 调用用了 evictor 来剔除已经计算过的数据
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new PvUvProcessWindowFunction())
                .addSink(new RedisSink<>(conf,new MyRedisSink()));
    }
}




