package com.project.tan.TopN;


import com.project.tan.entity.OrderDetail;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * TopNAllWindowFunction
 *
 *
 * ProcessWindowFunction获得一个包含窗口所有元素的可迭代器，以及一个具有时间和状态信息访问权的上下文对象，
 * 这使得它比其他窗口函数提供更大的灵活性。这是以性能和资源消耗为代价的，因为元素不能增量地聚合，而是需要在内部缓冲，直到认为窗口可以处理为止。
 *
 */
public class TopNAllWindowFunction extends ProcessAllWindowFunction<OrderDetail, Tuple2<Double, OrderDetail>, TimeWindow> {

    private int size = 10;

    public TopNAllWindowFunction(int size) {
        this.size = size;
    }

    @Override
    public void process(Context context, Iterable<OrderDetail> elements, Collector<Tuple2<Double, OrderDetail>> out) throws Exception {
        TreeMap<Double, OrderDetail> treeMap = new TreeMap<Double, OrderDetail>(new Comparator<Double>() {
            @Override
            public int compare(Double x, Double y) {
                return (x < y) ? -1 : 1;
            }
        });

        Iterator<OrderDetail> iterator = elements.iterator();
        if (iterator.hasNext()) {
            treeMap.put(iterator.next().getPrice(), iterator.next());
            if (treeMap.size() > 10) {
                treeMap.pollLastEntry();
            }
        }

        for (Map.Entry<Double, OrderDetail> entry : treeMap.entrySet()) {
            out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
        }

    }

}

