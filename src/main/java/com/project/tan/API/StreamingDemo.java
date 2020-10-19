package com.project.tan.API;

import com.project.tan.entity.Item;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * DataStream and DataSet 区别？
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/19 4:53 PM
 * @Version 1.0
 */

public class StreamingDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        //注意：并行度设置为1,我们会在后面的课程中详细讲解并行度
        DataStreamSource<Item> text = env.addSource(new MyStreamingSource()).setParallelism(1);

//        // 1.打印全部
//        DataStream<Item> item = text.map((MapFunction<Item, Item>) value -> value);
//        //打印结果
//        item.print().setParallelism(1);

        //2. Map: 获取部分值，如获取商品名
//        SingleOutputStreamOperator<Object> mapItems = text.map(new MapFunction<Item, Object>() {
//            @Override
//            public Object map(Item item) throws Exception {
//                return item.getName();
//            }
//        });
//        //打印结果
//        mapItems.print().setParallelism(1);


        //3.重写的 map 函数中使用 lambda 表达式
//        SingleOutputStreamOperator<String> map = text.map(item -> item.getName());
//        map.print().setParallelism(1);


        // 4. 自定义map
//        SingleOutputStreamOperator<String> map4 = text.map(new MyMapFunction());
//        map4.print().setParallelism(1);


        // 5. flatMap : FlatMap 接受一个元素，返回零到多个元素
//        SingleOutputStreamOperator<Object> flatMapItems = text.flatMap(new FlatMapFunction<Item, Object>() {
//            @Override
//            public void flatMap(Item item, Collector<Object> collector) throws Exception {
//                String name = item.getName();
//                collector.collect(name);
//            }
//        });

        // 6. filter    
//        SingleOutputStreamOperator<Item> filterItems = text.filter(new FilterFunction<Item>() {
//            @Override
//            public boolean filter(Item item) throws Exception {
//                return item.getId() % 2 == 0;
//            }
//        });

        SingleOutputStreamOperator<Item> filterItems = text.filter(
                item -> item.getId() % 2 == 0
        );
        filterItems.print();


        String jobName = "user defined streaming source";
        env.execute(jobName);

    }

    /**
     * 通过重写 MapFunction 或 RichMapFunction 来自定义自己的 map 函数。
     */
    static class MyMapFunction extends RichMapFunction<Item, String> {
        @Override
        public String map(Item item) throws Exception {
            return item.getName();
        }
    }
}

/**
 * 自定义实时数据源
 */
class MyStreamingSource implements SourceFunction<Item> {

    private boolean isRunning = true;

    /**
     * 重写run方法产生一个源源不断的数据发送源
     *
     * @param ctx
     * @throws Exception
     */

    @Override

    public void run(SourceContext<Item> ctx) throws Exception {

        while (isRunning) {
            Item item = generateItem();
            ctx.collect(item);
            //每秒产生一条数据
            Thread.sleep(1000);

        }

    }

    @Override

    public void cancel() {

        isRunning = false;

    }


    //随机产生一条商品数据
    private Item generateItem() {
        int i = new Random().nextInt(100);
        Item item = new Item();
        item.setName("name" + i);
        item.setId(i);
        return item;
    }


}



