package org.apache.flume.sink.gp.client;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;

/**
 * Created by ljh on 2017/10/10.
 */
public class Test {
    public static void main(String[] args) {
        String url ="jdbc:mysql://192.168.1.100/flume?Unicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true";
        String userName ="root";
        String password ="123456";
        DruidDataSource dataSource =  GpConnect.dataSource(url,userName,password);
        Connection con = GpConnect.getConnection(dataSource,url,userName,password);
        System.out.println(con);


    }
}
