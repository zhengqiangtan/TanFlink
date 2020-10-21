

#### 1、几种常见的 Flink 中实时去重方案

- 基于状态后端
> 状态后端的种类之一是 RocksDBStateBackend。它会将正在运行中的状态数据保存在 RocksDB 数据库中，该数据库默认将数据存储在 TaskManager 运行节点的数据目录下。
  RocksDB 是一个 K-V 数据库，我们可以利用 MapState 进行去重

- 基于 HyperLogLog
> HyperLogLog 并不是精准的去重，如果业务场景追求 100% 正确，那么一定不要使用这种方法。
                   >
                   >
- 基于布隆过滤器（BloomFilter）
> BloomFilter（布隆过滤器）类似于一个 HashSet，用于快速判断某个元素是否存在于集合中，其典型的应用场景就是能够快速判断一个 key 是否存在于某容器，不存在就直接返回。
  需要注意的是，和 HyperLogLog 一样，布隆过滤器不能保证 100% 精确。但是它的插入和查询效率都很高。 

- 基于 BitMap
> 上面的 HyperLogLog 和 BloomFilter 虽然减少了存储但是丢失了精度， 这在某些业务场景下是无法被接受的。下面的这种方法不仅可以减少存储，而且还可以做到完全准确，那就是使用 BitMap。
> Bit-Map 的基本思想是用一个 bit 位来标记某个元素对应的 Value，而 Key 即是该元素。由于采用了 Bit 为单位来存储数据，因此可以大大节省存储空间。  

- 基于外部数据库
> 假如我们的业务场景非常复杂，并且数据量很大。为了防止无限制的状态膨胀，也不想维护庞大的 Flink 状态，我们可以采用外部存储的方式，比如可以选择使用 Redis 或者 HBase 存储数据，我们只需要设计好存储的 Key 即可。同时使用外部数据库进行存储，我们不需要关心 Flink 任务重启造成的状态丢失问题，但是有可能会出现因为重启恢复导致的数据多次发送，从而导致结果数据不准的问题。





#### 参考文章
[拉钩-Flink去重方案](https://kaiwu.lagou.com/course/courseInfo.htm?courseId=81#/detail/pc?id=2055)