package com.flink.tan.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 重写 open、invoke、close 方法，数据写入 MySQL 时会首先调用 open 方法新建连接，然后调用 invoke 方法写入 MySQL，
 * 最后执行 close 方法关闭当前连接
 *
 * @Author zhengqiang.tan
 * @Date 2020/10/24 4:04 PM
 * @Version 1.0
 */
public class MyMysqlSink extends RichSinkFunction<Tuple3<String, String, Integer>> {

    private PreparedStatement ps = null;
    private Connection connection = null;

    String driver = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://127.0.0.1:3306/test";
    String username = "root";
    String password = "root";

    // 初始化方法

    @Override

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取连接
        connection = getConn();
        connection.setAutoCommit(false);

    }

    private Connection getConn() {
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    //每一个元素的插入，都会调用一次
    @Override
    public void invoke(Tuple3<String, String, Integer> data, Context context) throws Exception {
        connection.prepareStatement("replace into pvuv_result (type,value) values (?,?)");
        ps.setString(1, data.f1);
        ps.setInt(2, data.f2);
        ps.execute();

        connection.commit();

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }

    }

}
