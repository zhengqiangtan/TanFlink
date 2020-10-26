### 一、概念
1. 什么是CEP ?
> 创建系列 Pattern，然后利用 NFACompiler 将 Pattern 进行拆分并且创建出 NFA，NFA 包含了 Pattern 中的各个状态和各个状态间转换的表达式。

> 整个过程我们可以把 Flink CEP 的使用类比为正则表达式的使用。
> CEP 中定义的 Pattern 就是正则表达式，而 DataStream 是需要进行匹配的原数据，Flink CEP 通过 DataStream 和 Pattern 进行匹配，然后生成一个经过正则过滤后的 DataStream。


### Flink 中的 Pattern 分为单个模式、组合模式、模式组 3 类



### 二、Flink CEP 源码解析
Flink CEP 被设计成 Flink 中的算子，而不是单独的引擎。那么当一条数据到来时，Flink CEP 是如何工作的呢？

Flink DataStream 和 PatternStream
我们还是用官网中的案例来进行讲解：


*我们可以看到一条数据进入 Flink CEP 中处理的逻辑大概可以分为以下几个步骤：*

DataSource 中的数据转换为 DataStream；
定义 Pattern，并将 DataStream 和 Pattern 组合转换为 PatternStream；
PatternStream 经过 select、process 等算子转换为 DataStraem；
再次转换的 DataStream 经过处理后，sink 到目标库。




参考： 
1、[拉钩-flink-CEP源码分析](https://kaiwu.lagou.com/course/courseInfo.htm?courseId=81#/detail/pc?id=2071)
2、[FLINK CEP 模式匹配](https://kaiwu.lagou.com/course/courseInfo.htm?courseId=81#/detail/pc?id=2072)



