package com.project.tan.udf;

import java.util.Map;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

public class CEPUpper extends FunctionExecutor {
    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
    }

    // 只转换第一个能转换的元素
    protected Object execute(Object[] objects) {
        return execute(objects[0]);
    }

    protected Object execute(Object o) {
        if (!(o instanceof String)) {
            return null;
        }
        return ((String) o).toUpperCase();
    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }

    public Map<String, Object> currentState() {
        return null;
    }

    public void restoreState(Map<String, Object> map) {
    }

    public static void main(String[] args) {
        System.out.println(new CEPUpper().execute(new Object[]{"test"}));
        System.out.println(new CEPUpper().execute(new Object[]{null})); // null
        System.out.println(new CEPUpper().execute(new Object[]{Integer.valueOf(123)})); // null
        System.out.println(new CEPUpper().execute(new Object[]{"test", "te"}));
        System.out.println(new CEPUpper().execute(new Object[]{"test", Integer.valueOf(123)}));
    }
}