package com.project.tan.compiler;

import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.janino.ScriptEvaluator;

public class JaninoTester07 {
    public static void main(String[] args) {
        try {
            IScriptEvaluator se = new ScriptEvaluator();
            se.setReturnType(String.class);
            se.cook("import com.project.tan.compiler.BaseClass;\n"
                    + "import com.project.tan.compiler.DerivedClass;\n"
                    + "BaseClass o=new DerivedClass(\"1\",\"tom\");\n"
                    + "return o.toString();\n");
            Object res = se.evaluate(new Object[0]);
            System.out.println(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}