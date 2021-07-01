package com.flink.tan.entity;

/**
 * 使用idea innerbuilder插件完成
 */
public class TestBuiler {
    private int id;
    private String name;
    private Boolean flag;

    private TestBuiler(Builder builder) {
        id = builder.id;
        name = builder.name;
        flag = builder.flag;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int id;
        private String name;
        private Boolean flag;

        private Builder() {
        }

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder flag(Boolean flag) {
            this.flag = flag;
            return this;
        }

        public TestBuiler build() {
            return new TestBuiler(this);
        }
    }

    public static void main(String[] args) {
        TestBuiler test = TestBuiler.builder().id(1).name("test").flag(false).build();
        System.out.println(test.toString());

    }
}
