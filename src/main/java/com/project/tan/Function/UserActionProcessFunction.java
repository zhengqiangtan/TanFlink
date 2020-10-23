package com.project.tan.Function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.project.tan.entity.UserClick;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 可以在 ProcessFunction 中将所有的过滤转化逻辑放在一起进行处理
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/23 11:46 AM
 * @Version 1.0
 */
public class UserActionProcessFunction extends ProcessFunction<String, String> {

    @Override
    public void processElement(String input, Context ctx, Collector<String> out) throws Exception {
        if (!input.contains("CLICK") || input.startsWith("{") || input.endsWith("}")) {
            return;
        }

        JSONObject jsonObject = JSON.parseObject(input);

        String user_id = jsonObject.getString("user_id");
        String action = jsonObject.getString("action");
        Long timestamp = jsonObject.getLong("timestamp");

        if (!StringUtils.isEmpty(user_id) || !StringUtils.isEmpty(action)) {

            UserClick userClick = new UserClick();
            userClick.setUserId(user_id);
            userClick.setTimestamp(timestamp);
            userClick.setAction(action);

            out.collect(JSON.toJSONString(userClick));

        }

    }

}
