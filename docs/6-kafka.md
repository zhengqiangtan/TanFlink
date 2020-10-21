#### Kakfa常用操作命令
本地模式：   
启动ZK：  
`nohup bin/zookeeper-server-start.sh config/zookeeper.properties  &`

启动Kafka Server:   
`nohup bin/kafka-server-start.sh config/server.properties &`

创建一个名为 test 的 Topic：   
`bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test`

消费kafka消息： 
`bin/kafka-console-consumer.sh  --bootstrap-server localhoust:9092 --topic test --from-beginning`




### Flink 消费 Kafka 设置 offset 的方法 [跳转](https://kaiwu.lagou.com/course/courseInfo.htm?courseId=81#/detail/pc?id=2059)
Flink 消费 Kafka 需要指定消费的 offset，也就是偏移量。Flink 读取 Kafka 的消息有五种消费方式：  

- 指定 Topic 和 Partition

- 从最早位点开始消费

- 从指定时间点开始消费

- 从最新的数据开始消费

- 从上次消费位点开始消费
