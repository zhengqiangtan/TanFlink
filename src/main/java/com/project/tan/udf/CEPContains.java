package com.project.tan.udf;

import java.util.Map;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

public class CEPContains extends FunctionExecutor {
    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
    }
    protected Object execute(Object[] objects) {
        if (objects.length != 2) {
            return Integer.valueOf(-1);
        }
        if ((!(objects[0] instanceof String)) || (!(objects[1] instanceof String))) {
            return Integer.valueOf(-1);
        }
        String str = (String) objects[0];
        String target = (String) objects[1];
        return Integer.valueOf(str.contains(target) ? 1 : 0);
    }

    protected Object execute(Object o) {
        return Integer.valueOf(-1);
    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.INT;
    }

    public Map<String, Object> currentState() {
        return null;
    }

    public void restoreState(Map<String, Object> map) {
    }

    public static void main(String[] args) {
        System.out.println(new CEPContains().execute(new Object[]{"123456", "45"})); // 1
        System.out.println(new CEPContains().execute(new Object[]{"123", "45"})); // 0
        System.out.println(new CEPContains().execute(new Object[]{null, "45"})); // -1
        System.out.println(new CEPContains().execute(new Object[]{"123"})); // -1
    }
}
