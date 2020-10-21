

### 数据倾斜原因和解决方案
Flink 任务出现数据倾斜的直观表现是任务节点频繁出现反压，但是增加并行度后并不能解决问题；
部分节点出现 OOM 异常，是因为大量的数据集中在某个节点上，导致该节点内存被爆，任务失败重启。


#### 产生数据倾斜的原因主要有 2 个方面：

业务上有严重的数据热点，比如滴滴打车的订单数据中北京、上海等几个城市的订单量远远超过其他地区；
技术上大量使用了 KeyBy、GroupBy 等操作，错误的使用了分组 Key，人为产生数据热点。

因此解决问题的思路也很清晰：

- 业务上要尽量避免热点 key 的设计，例如我们可以把北京、上海等热点城市分成不同的区域，并进行单独处理；

- 技术上出现热点时，要调整方案打散原来的 key，避免直接聚合；此外 Flink 还提供了大量的功能可以避免数据倾斜。



### Flink 任务数据倾斜场景和解决方案

1. 两阶段聚合解决 KeyBy 热点
2. GroupBy + Aggregation 分组聚合热点问题
3. Flink 消费 Kafka 上下游并行度不一致导致的数据倾斜
4. Distinct计算UV
```sql
SELECT day, SUM(cnt) total 
FROM ( 
	SELECT day, MOD(buy_id, 1024), COUNT(DISTINCT buy_id) as cnt 
	FROM T GROUP BY day, MOD(buy_id, 1024)
) GROUP BY day
```





### 参考文章
[拉钩-数据倾斜问题处理](https://kaiwu.lagou.com/course/courseInfo.htm?courseId=81#/detail/pc?id=2051)
