package com.flink.tan.compiler;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.ScriptEvaluator;
import org.codehaus.janino.SimpleCompiler;

import java.io.StringReader;

/**
 * https://janino-compiler.github.io/janino/2014-02-18_SWM-JAK.pdf
 */
public class Test01 {
//    public static void main(String[] args) throws Exception {
//        String className = "Test";
//        String classBody =
//                "public class Test {\n" +
//                        "    public void getTime(){\n" +
//                        "        System.out.println(System.currentTimeMillis());\n" +
//                        "    }\n" +
//                        "}";
//        SimpleCompiler compiler = new SimpleCompiler();
//        compiler.setParentClassLoader(Thread.currentThread().getClass().getClassLoader());
//        compiler.cook(classBody);
//        Class<?> clazz = compiler.getClassLoader().loadClass(className);
//        Object o = clazz.newInstance();
//        clazz.getMethod("getTime").invoke(o); // 1625016063423
//    }



        // 不带参的函数调用
//        public static void main(String[] args) throws Exception {
//        ScriptEvaluator se = new ScriptEvaluator();
//        se.cook(
//                ""
//                        + "static void method1() {\n"
//                        + "    System.out.println(\"run in method1()\");\n"
//                        + "}\n"
//                        + "\n"
//                        + "static void method2() {\n"
//                        + "    System.out.println(\"run in method2()\");\n"
//                        + "}\n"
//                        + "\n"
//                        + "method1();\n"
//                        + "method2();\n"
//                        + "\n"
//
//        );
//        se.evaluate(null); // 调用方法1和方法2
//    }



//
///** 执行带参数的表达式计算*/
//    public static void main(String[] args) throws Exception {
//        // 首先定义一个表达式模拟器ExpressionEvaluator对象
//        ExpressionEvaluator ee = new ExpressionEvaluator();
//
//        // 定义一个算术表达式，表达式中需要有2个int类型的参数a和b
//        String expression = "2 * (a + b)";
//        ee.setParameters(new String[]{"a", "b"}, new Class[]{int.class, int.class});
//
//        // 设置表达式的返回结果也为int类型
//        ee.setExpressionType(int.class);
//
//        // 这里处理（扫描，解析，编译和加载）上面定义的算数表达式.
//        ee.cook(expression);
//
//        // 根据输入的a和b参数执行实际的表达式计算过程
//        int result = (Integer) ee.evaluate(new Object[]{19, 23});
//        System.out.println(expression + " = " + result); // 2 * (a + b) = 84
//    }


        public static void main(String[] args) throws Exception {
        String sampleClass =
                "public class Test {" +
                        "public void sampleMethod() {\n" +
                        "        JsonObject obj = new JsonObject();\n" +
                        "        obj.add(\"p1\", new JsonPrimitive(2));\n" +
                        "        System.out.println(obj.get(\"p1\"));\n" +
                        "}" +
                        "}";

        ClassBodyEvaluator classBodyEvaluator = new ClassBodyEvaluator();
        classBodyEvaluator.setParentClassLoader(Thread.currentThread().getContextClassLoader());
        classBodyEvaluator.setDefaultImports(new String[] {  "com.google.gson.JsonObject", "com.google.gson.JsonPrimitive"});
        StringReader sr = new StringReader(sampleClass);
        classBodyEvaluator.cook(null, sr);
        Class<?> clazz = classBodyEvaluator.getClazz();
        Object o = clazz.newInstance();
        System.out.println(o.getClass().getMethod("sampleMethod").invoke(o));
    }


}
