package com.project.tan.API;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 分布式缓存示例
 *
 * 使用分布式缓存有两个步骤：
 * 第一步：首先需要在 env 环境中注册一个文件，该文件可以来源于本地，也可以来源于 HDFS ，并且为该文件取一个名字。
 * 第二步：在使用分布式缓存时，可根据注册的名字直接获取。
 *
 * 注意：
 * 在使用分布式缓存时也需要注意一些问题，需要我们缓存的文件在任务运行期间最好是只读状态，否则会造成数据的一致性问题。
 * 另外，缓存的文件和数据不宜过大，否则会影响 Task 的执行速度，在极端情况下会造成 OOM。
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/20 3:01 PM
 * @Version 1.0
 */
public class DistributedCacheDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("/Users/tandemac/workspace/lagou/TanFlink/data/distributedcache.txt", "distributedCache");


        //1：注册一个文件,可以使用hdfs上的文件 也可以是本地文件进行测试
        DataSource<String> data = env.fromElements("Linea", "Lineb", "Linec", "Lined");
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2：使用该缓存文件
                File myFile = getRuntimeContext().getDistributedCache().getFile("distributedCache");
                List<String> lines = FileUtils.readLines(myFile);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.err.println("分布式缓存为:" + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                //在这里就可以使用dataList
                System.err.println("使用datalist：" + dataList + "-------" + value);
                //业务逻辑
                return dataList + " : " + value;
            }
        });

        result.printToErr();
    }
}

//        分布式缓存为:Test1
//        分布式缓存为:Test2
//        分布式缓存为:Test3
//        使用datalist：[Test1, Test2, Test3]-------Linea
//        使用datalist：[Test1, Test2, Test3]-------Lineb
//        使用datalist：[Test1, Test2, Test3]-------Linec
//        使用datalist：[Test1, Test2, Test3]-------Lined







