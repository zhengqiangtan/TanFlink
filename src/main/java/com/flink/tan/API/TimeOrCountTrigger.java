package com.flink.tan.API;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;


/**
 * 参考示例：
 *
 * TimeOrCountTrigger : 达到一定时间或者数据条数后触发
 * 条数的触发：使用状态进行计数
 * 时间的触发：使用 flink 的 timerServer，注册触发器触发
 * 知乎: 码农铲屎官
 * https://mp.weixin.qq.com/s?__biz=MzkwMzE3MzI1Mw==&mid=2247483678&idx=1&sn=a3574dfb7df0385dbd676e7e37ab153b&chksm=c09b04f4f7ec8de2f0f5dcde7aea991ca44070ba8caa33585e8dbe8610690840d49f136f3267&scene=21#wechat_redirect
 *
 */
public class TimeOrCountTrigger<W extends Window> extends Trigger<Object, W> {

    // 触发的条数
    private final long size;
    // 触发的时长
    private final long interval;
    private static final long serialVersionUID = 1L;
    // 条数计数器
    private final ReducingStateDescriptor<Long> countStateDesc =
            new ReducingStateDescriptor<Long>("count", (ReduceFunction<Long>) new ReduceSum(), LongSerializer.INSTANCE);
    // 时间计数器，保存下一次触发的时间
    private final ReducingStateDescriptor<Long> timeStateDesc =
            new ReducingStateDescriptor<Long>("fire-interval", (ReduceFunction<Long>) new ReduceMin(), LongSerializer.INSTANCE);

    public TimeOrCountTrigger(long size, long interval) {
        this.size = size;
        this.interval = interval;
    }


    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, Trigger.TriggerContext ctx) throws Exception {
        // 注册窗口结束的触发器, 不需要会自动触发
        // count
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        //interval
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(timeStateDesc);
        // 每条数据 counter + 1
        count.add(1L);
        if (count.get() >= size) {
//            logger.info("countTrigger triggered, count : {}", count.get());
            // 满足条数的触发条件，先清 0 条数计数器
            count.clear();
            // 满足条数时也需要清除时间的触发器，如果不是创建结束的触发器
            if (fireTimestamp.get() != window.maxTimestamp()) {
                ctx.deleteProcessingTimeTimer(fireTimestamp.get());
            }
            fireTimestamp.clear();
            // fire 触发计算
            return TriggerResult.FIRE;
        }

        // 触发之后，下一条数据进来才设置时间计数器注册下一次触发的时间
        timestamp = ctx.getCurrentProcessingTime();
        if (fireTimestamp.get() == null) {
            long nextFireTimestamp = timestamp + interval;
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
            fireTimestamp.add(nextFireTimestamp);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, Trigger.TriggerContext ctx) throws Exception {
        // count
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        //interval
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(timeStateDesc);

        // time trigger and window end
        if (time == window.maxTimestamp()) {
//            logger.info("window close : {}", time);
            // 窗口结束，清0条数和时间的计数器
            count.clear();
            ctx.deleteProcessingTimeTimer(fireTimestamp.get());
            fireTimestamp.clear();
            return TriggerResult.FIRE_AND_PURGE;
        } else if (fireTimestamp.get() != null && fireTimestamp.get().equals(time)) {
//            logger.info("timeTrigger trigger, time : {}", time);
            // 时间计数器触发，清0条数和时间计数器
            count.clear();
            fireTimestamp.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {

    }
}