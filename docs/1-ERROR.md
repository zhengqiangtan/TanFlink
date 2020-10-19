
#### 一 、问题记录

1. `Exception in thread "main" java.lang.RuntimeException: No new data sinks have been defined since the last execution.
 The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'.`

> 错误的API引用,应该import org.apache.flink.api.java.tuple.Tuple2，结果import scala.Tuple2


2. `Exception in thread "main" java.lang.RuntimeException: 
No new data sinks have been defined since the last execution. The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'.
`
> 解释：自上次执行以来，没有定义新的数据接收器。对于离线批处理的算子，如：“count()”、“collect()”或“print()”等既有sink功能，还有触发的功能。
>我们上面调用了print()方法，会自动触发execute，所以最后面的一行执行器没有数据可以执行。
>因此注释掉：env.execute()









