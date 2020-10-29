package com.test;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Title AscendingTimestampExtractor
 * 适用于elements的时间在每个parallel task里头是单调递增的，对于违反timestamp monotony的，这里调用MonotonyViolationHandler的handleViolation方法进行处理；
 * MonotonyViolationHandler继承了Serializable，它定义了handleViolation方法，这个接口内置有三个实现类，分别是IgnoringHandler、FailingHandler、FailingHandler
 * <p>
 * https://blog.csdn.net/weixin_33859844/article/details/88681479
 * @Author zhengqiang.tan
 * @Date 2020/10/27 5:37 PM
 */
public class Test_extractAscendingTimestamp {
    @Test
    public void testWithFailingHandler() {
        AscendingTimestampExtractor<Long> extractor = (new Test_extractAscendingTimestamp.LongExtractor()).withViolationHandler(new AscendingTimestampExtractor.FailingHandler());
        this.runValidTests(extractor);

        try {
            this.runInvalidTest(extractor);
            this.runValidTests(extractor);
            Assert.fail("should fail with an exception");
        } catch (Exception var3) {
            ;
        }

    }

    private void runValidTests(AscendingTimestampExtractor<Long> extractor) {
        Assert.assertEquals(13L, extractor.extractTimestamp(13L, -1L));
        Assert.assertEquals(13L, extractor.extractTimestamp(13L, 0L));
        Assert.assertEquals(14L, extractor.extractTimestamp(14L, 0L));
        Assert.assertEquals(20L, extractor.extractTimestamp(20L, 0L));
        Assert.assertEquals(20L, extractor.extractTimestamp(20L, 0L));
        Assert.assertEquals(20L, extractor.extractTimestamp(20L, 0L));
        Assert.assertEquals(500L, extractor.extractTimestamp(500L, 0L));
        Assert.assertEquals(9223372036854775806L, extractor.extractTimestamp(9223372036854775806L, 99999L));
    }

    private void runInvalidTest(AscendingTimestampExtractor<Long> extractor) {
        Assert.assertEquals(1000L, extractor.extractTimestamp(1000L, 100L));
        Assert.assertEquals(1000L, extractor.extractTimestamp(1000L, 100L));
        Assert.assertEquals(999L, extractor.extractTimestamp(999L, 100L));
    }

    private static class LongExtractor extends AscendingTimestampExtractor<Long> {
        private static final long serialVersionUID = 1L;

        private LongExtractor() {
        }

        public long extractAscendingTimestamp(Long element) {
            return element;
        }
    }
}
