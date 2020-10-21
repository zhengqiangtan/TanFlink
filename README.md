## TanFlink 综合学习项目 

 START TIME : 2020年09月23日

### 一、环境说明
- Java 1.8
- Maven 3.6.1
- Flink Version 1.10.0


### 二、相关技术
- 项目初始化
```shell script
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java      \
      -DarchetypeVersion=1.10.0
```
- 其他



### 三、问题记录
1. DateSet 和 DataStream 的区别和联系 ？
>  flink-java 这个模块中找到所有关于 DataSet 的核心类，DataStream 的核心实现类则在 flink-streaming-java 这个模块。
>  Flink 的编程模型中，对于 DataSet 而言，Source 部分来源于文件、表或者 Java 集合；而 DataStream 的 Source 部分则一般是消息中间件比如 Kafka 等。
>
2. 其他



### 四、参考学习网址
- [技术栈mstacks.com](http://mstacks.com/141/1505.html#content1505)
- [你需要的不是实时数仓 | 你需要的是一款强大的OLAP数据库(上)](https://mp.weixin.qq.com/s/9MZ9ztr8fYJTl1HchqtQqA)



---
[Flink Kafka Connector Docs](https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html)

