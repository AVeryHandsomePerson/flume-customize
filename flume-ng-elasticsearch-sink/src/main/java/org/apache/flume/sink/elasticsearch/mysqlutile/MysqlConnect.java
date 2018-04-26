package org.apache.flume.sink.elasticsearch.mysqlutile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by ljh on 2017/5/15.
 *数据库连接
 */
public class MysqlConnect {

    public static String url = "jdbc:mysql://192.168.1.100/webtest";
    public static String name = "com.mysql.jdbc.Driver";
    public static String user = "root";
    public static String password = "123456";

    public MysqlConnect() {
        try {
            File file = new File("../apache-flume-1.7.0-bin/conf/flumemysql.properties");
            FileInputStream fileInputStream = new FileInputStream(file);
            Properties properties = new Properties();
            properties.load(fileInputStream);
            url = properties.getProperty("mysql.url");
            name = properties.getProperty("mysql.name");
            user = properties.getProperty("mysql.user");
            password = properties.getProperty("mysql.password");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Connection conn = null;
    public PreparedStatement pst = null;

    public MysqlConnect(String sql) {
        try {
            Class.forName(name);//指定连接类型
            conn = DriverManager.getConnection(url, user, password);//获取连接
            pst = conn.prepareStatement(sql);//准备执行语句
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            this.conn.close();
            this.pst.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
