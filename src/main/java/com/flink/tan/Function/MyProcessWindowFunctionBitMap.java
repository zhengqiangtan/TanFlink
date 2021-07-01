package com.flink.tan.Function;

import com.flink.tan.entity.UserClick;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Iterator;

/**
 * BitMap精确去重函数
 * <p>
 * 假如用户的 ID 可以转化为 Long 型，可以使用 BitMap 进行去重计算 UV
 * 如果业务中的用户 ID 是字符串类型，不能被转化为 Long 型，那么你可以使用布隆过滤器进行去重。
 */
public class MyProcessWindowFunctionBitMap extends ProcessWindowFunction<UserClick, Tuple3<String, String, Integer>, String, TimeWindow> {

    private transient ValueState<Integer> uvState;
    private transient ValueState<Integer> pvState;
    private transient ValueState<Roaring64NavigableMap> bitMapState;


    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        uvState = this.getRuntimeContext().getState(new ValueStateDescriptor<Integer>("pv", Integer.class));
        pvState = this.getRuntimeContext().getState(new ValueStateDescriptor<Integer>("uv", Integer.class));
        bitMapState = this.getRuntimeContext().getState(new ValueStateDescriptor("bitMap", TypeInformation.of(new TypeHint<Roaring64NavigableMap>() {
        })));
    }

    /**
     * 下面定义了一个 Roaring64NavigableMap 用来存储用户 ID，最后只需要调用 bitMap.getIntCardinality() 即可求得去重后的 UV 值。
     *
     * @param s
     * @param context
     * @param elements
     * @param out
     * @throws Exception
     */
    @Override
    public void process(String s, Context context, Iterable<UserClick> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception {

        Integer uv = uvState.value();
        Integer pv = pvState.value();
        Roaring64NavigableMap bitMap = bitMapState.value();

        if (bitMap == null) {
            bitMap = new Roaring64NavigableMap();
            uv = 0;
            pv = 0;
        }

        Iterator<UserClick> iterator = elements.iterator();
        while (iterator.hasNext()) {
            pv = pv + 1;
            String userId = iterator.next().getUserId();
            //如果userId可以转成long
            bitMap.add(Long.valueOf(userId));
        }

        out.collect(Tuple3.of(s, "uv", bitMap.getIntCardinality()));
        out.collect(Tuple3.of(s, "pv", pv));

    }
}
