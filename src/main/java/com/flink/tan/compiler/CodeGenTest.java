package com.flink.tan.compiler;

import org.codehaus.janino.SimpleCompiler;

/**
 * Flink中大量使用了CodeGen技术，这里做个demo
 *
 * Flink自身支持对原生系统函数进行校验，但是 自定义UDF 由用户业务决定，无法全量支持，因此 打算采用 CodeGen 编译 自定义UDF 后，进行校验。
 * (Flink 内部，在 由 Sql -> DataStream 时，也使用了 CodeGen 代码生成器功能)
 *
 * https://blog.csdn.net/super_wj0820/article/details/96024779
 */
public class CodeGenTest {
    public static void main(String[] args) throws Exception {
        String code = "public class Student extends com.flink.tan.compiler.AbstractChinese { " +
                " " +
                "    private String name; " +
                " " +
                "    private int age; " +
                " " +
                "    public Student() { " +
                "    } " +
                " " +
                "    @Override " +
                "    public String getName() { " +
                "        return name; " +
                "    } " +
                " " +
                "    @Override " +
                "    public void setName(String name) { " +
                "        this.name = name; " +
                "    } " +
                " " +
                "    @Override " +
                "    public int getAge() { " +
                "        return age; " +
                "    } " +
                " " +
                "    @Override " +
                "    public void setAge(int age) { " +
                "        this.age = age; " +
                "    } " +
                " " +
                "    @Override " +
                "    public String print() { " +
                "        return \"Student{\" + " +
                "                \"name='\" + name + '\\'' + " +
                "                \", age=\" + age + " +
                "                '}'; " +
                "    } " +
                " " +
                "}";

        Class<AbstractChinese> clazz = getClazz(code);
        AbstractChinese instance = clazz.newInstance();

        instance.setName("TT");
        instance.setAge(22);

        System.out.println(instance.print()); // Student{name='TT', age=22}
        // 启发：可以考虑动态生成对应的实现类来实现某些插件或者函数处理逻辑等

    }

    private static Class<AbstractChinese> getClazz(String code) throws Exception {
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(CodeGenTest.class.getClassLoader());
        compiler.cook(code);
        return (Class<AbstractChinese>) compiler.getClassLoader().loadClass("Student");
    }
}



