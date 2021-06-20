package com.project.tan.Example_01;

import com.project.tan.Function.MyCountAggreateFunction;
import com.project.tan.Function.MyCountWindowFunction;
import com.project.tan.entity.ProductViewData;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Title
 * @Author zhengqiang.tan
 * @Date 2020/10/27 8:41 PM
 */
public class CountAggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从文件读取数据
        DataStream<String> sourceData = senv.readTextFile("file:///Users/tandemac/workspace/lagou/TanFlink/src/main/resources/ProductViewData.txt");
        DataStream<ProductViewData> productViewData = sourceData.map(new MapFunction<String, ProductViewData>() {
            @Override
            public ProductViewData map(String value) throws Exception {
                String[] record = value.split(",");
                return new ProductViewData(record[0], record[1], Long.valueOf(record[2]), Long.valueOf(record[3]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ProductViewData>() {
            @Override
            public long extractAscendingTimestamp(ProductViewData element) {
                return element.getTimestamp();
            }
        });

        /*过滤操作类型为1  点击查看的操作*/
        DataStream<String> productViewCount = productViewData.filter(new FilterFunction<ProductViewData>() {
            @Override
            public boolean filter(ProductViewData value) throws Exception {
                if (value.getOperationType() == 1) {
                    return true;
                }
                return false;
            }
        }).keyBy(new KeySelector<ProductViewData, String>() {
            @Override
            public String getKey(ProductViewData value) throws Exception {
                return value.getProductId();
            }
            //时间窗口 6秒  滑动间隔3秒
        }).timeWindow(Time.milliseconds(6000), Time.milliseconds(3000))
                /*这里按照窗口进行聚合*/
         .aggregate(new MyCountAggreateFunction(), new MyCountWindowFunction());
        //聚合结果输出
        productViewCount.print();

        senv.execute("CountAggregateFunctionDemo");
    }
}
