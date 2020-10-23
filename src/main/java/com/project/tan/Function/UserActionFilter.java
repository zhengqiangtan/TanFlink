package com.project.tan.Function;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * 过滤数据
 * 获取那些用户事件为“点击”的消息，并且需要满足 JSON 格式数据的基本要求：以“{" 开头，以 "}”结尾。
 * @Author zhengqiang.tan
 * @Date 2020/10/23 11:42 AM
 * @Version 1.0
 */
public class UserActionFilter implements FilterFunction<String> {

    @Override
    public boolean filter(String input) throws Exception {
        return input.contains("CLICK") && input.startsWith("{") && input.endsWith("}");
    }
}
