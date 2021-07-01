package com.flink.tan.source;

import com.flink.tan.entity.Item;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

/**
 * 自定义实时数据源
 * @Author zhengqiang.tan
 * @Date 2020/10/20 10:37 AM
 * @Version 1.0
 */
public class MyStreamingSource implements SourceFunction<Item> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Item> sourceContext) throws Exception {
        while (isRunning) {
            sourceContext.collect(generateItem());
            // 每秒发送一次
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
        ArrayList<String> list = new ArrayList();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }
}
