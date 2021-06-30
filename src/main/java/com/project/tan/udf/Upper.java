package com.project.tan.udf;

import org.apache.flink.table.functions.ScalarFunction;
public class Upper extends ScalarFunction {

    public String eval(String str)
    {
        return str.toUpperCase();
    }
}
