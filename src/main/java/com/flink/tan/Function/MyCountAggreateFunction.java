package com.flink.tan.Function;

import com.flink.tan.entity.ProductViewData;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Title 自定义聚合函数 AggregateFunction
 *  Flink 的AggregateFunction是一个基于中间计算结果状态进行增量计算的函数。由于是迭代计算方式，所以，在窗口处理过程中，
 *  不用缓存整个窗口的数据，所以效率执行比较高。
 *  AggregateFunction比ReduceFunction更加通用，它有三个参数：输入类型（IN），累加器类型（ACC）和输出类型（OUT）
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/27 5:51 PM
 */
public class MyCountAggreateFunction implements AggregateFunction<ProductViewData, Long, Long> {


    // 创建一个新的累加器，启动一个新的聚合,负责迭代状态的初始化
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    // 对于数据的每条数据，和迭代数据的聚合的具体实现
    @Override
    public Long add(ProductViewData productViewData, Long acc) {
        return acc +1;
    }

    // 从累加器获取聚合的结果
    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    // 合并两个累加器，返回一个具有合并状态的累加器
    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}