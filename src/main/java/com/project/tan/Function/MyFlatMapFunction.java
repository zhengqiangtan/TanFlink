package com.project.tan.Function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.project.tan.entity.UserClick;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Author zhengqiang.tan
 * @Date 2020/10/23 11:43 AM
 * @Version 1.0
 */
public class MyFlatMapFunction implements FlatMapFunction<String, String> {

    @Override

    public void flatMap(String input, Collector out) throws Exception {

        JSONObject jsonObject = JSON.parseObject(input);

        String user_id = jsonObject.getString("user_id");

        String action = jsonObject.getString("action");

        Long timestamp = jsonObject.getLong("timestamp");

        // 过滤掉 userId 为空或者 action 类型为空的数据
        if (!StringUtils.isEmpty(user_id) || !StringUtils.isEmpty(action)) {

            UserClick userClick = new UserClick();
            userClick.setUserId(user_id);
            userClick.setTimestamp(timestamp);
            userClick.setAction(action);

            out.collect(JSON.toJSONString(userClick));

        }

    }

}
