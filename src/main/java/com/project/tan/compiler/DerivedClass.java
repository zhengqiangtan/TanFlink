package com.project.tan.compiler;

public class DerivedClass extends BaseClass {

    private String name;

    public DerivedClass(String baseId, String name) {
        super(baseId);
        this.name = name;
    }

    @Override
    public String toString() {
        return super.toString() + "DerivedClass [name=" + name + "]";
    }

}