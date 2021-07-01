package com.flink.tan.compiler;

public abstract class AbstractChinese implements Person {
    private static final long serialVersionUID = 1L;
    public abstract String getName();
    public abstract void setName(String name);
    public abstract int getAge();
    public abstract void setAge(int age);
}
