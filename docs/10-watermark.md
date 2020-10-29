
## 周期性水印

### AscendingTimestampExtractor
> AscendingTimestampExtractor产生的时间戳和水印必须是单调非递减的，用户通过覆写extractAscendingTimestamp()方法抽取时间戳。
>如果产生了递减的时间戳，就要使用名为MonotonyViolationHandler的组件处理异常，
>有两种方式：打印警告日志（默认）和抛出RuntimeException。  
> 单调递增的事件时间并不太符合实际情况，所以AscendingTimestampExtractor用得不多。
  

### BoundedOutOfOrdernessTimestampExtractor
> 允许“有界乱序”的，构造它时传入的参数maxOutOfOrderness就是乱序区间的长度，而实际发射的水印为通过覆写extractTimestamp()方法提取出来的时间戳减去乱序区间，
> 相当于让水印把步调“放慢一点”。这是Flink为迟到数据提供的第一重保障。  
> 当然，乱序区间的长度要根据实际环境谨慎设定，设定得太短会丢较多的数据，设定得太长会导致窗口触发延迟，实时性减弱。
>
>


### IngestionTimeExtractor
> IngestionTimeExtractor基于当前系统时钟生成时间戳和水印，其实就是Flink三大时间特征里的摄入时间了。
  
 
 
## 打点水印
> 打点水印比周期性水印用的要少不少，并且Flink没有内置的实现，那么就写个最简单的栗子吧。

```java
    sourceStream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<UserActionRecord>() {
      @Nullable
      @Override
      public Watermark checkAndGetNextWatermark(UserActionRecord lastElement, long extractedTimestamp) {
        return lastElement.getUserId().endsWith("0") ? new Watermark(extractedTimestamp - 1) : null;
      }

      @Override
      public long extractTimestamp(UserActionRecord element, long previousElementTimestamp) {
        return element.getTimestamp();
      }
    });
```

> AssignerWithPunctuatedWatermarks适用于需要依赖于事件本身的某些属性决定是否发射水印的情况。我们实现checkAndGetNextWatermark()方法来产生水印，
>产生的时机完全由用户控制。上面例子中是收取到用户ID末位为0的数据时才发射。
  


==**三点需要提醒**：==

不管使用哪种方式产生水印，都不能过于频繁。因为Watermark对象是会全部流向下游的，也会实打实地占用内存，水印过多会造成系统性能下降。
水印的生成要尽量早，一般是在接入Source之后就产生，或者在Source经过简单的变换（map、filter等）之后产生。
如果需求方对事件时间carry的业务意义并不关心，可以直接使用处理时间，简单方便。





`参考`  
[https://www.jianshu.com/p/c612e95a5028](https://www.jianshu.com/p/c612e95a5028)