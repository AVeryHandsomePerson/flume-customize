package org.apache.flume.sink.oracle.client;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.log4j.Logger;

import java.sql.Connection;

/**
 * Created by ljh on 2017/10/9.
 */
public class OracleConnect {
    private static Logger log = Logger.getLogger(OracleConnect.class);
    /**
     * 创建数据源
     *
     * @return
     */
    public static DruidDataSource dataSource(String url, String userName, String password) {
        DruidDataSource dataSource = null;
        if (dataSource == null) {
            dataSource = new DruidDataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(userName);
            dataSource.setPassword(password);
            dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
            dataSource.setMaxActive(15);// 设置最大并发数
            dataSource.setInitialSize(2);// 数据库初始化时，创建的连接个数
            dataSource.setMaxWait(60000);
            dataSource.setMinIdle(1);// 最小空闲连接数
            dataSource.setTimeBetweenEvictionRunsMillis(5 * 60 * 1000);// 5分钟检测一次是否有死掉的线程
            dataSource.setMinEvictableIdleTimeMillis(300000);// 空闲连接60秒中后释放
            dataSource.setTestWhileIdle(true);
            // 检测连接有效性
            dataSource.setTestOnBorrow(true);
            dataSource.setValidationQuery("select 1 from dual");
            dataSource.setPoolPreparedStatements(true);
            dataSource.setMaxOpenPreparedStatements(15);
        }
        return dataSource;
    }
    /**
     * 释放数据源
     */
    public static void shutDownDataSource(DruidDataSource dataSource) {
        if (dataSource != null) {
            dataSource.close();
        }
    }
    /**
     * 获取数据库连接
     *
     * @return
     */
    public static Connection getConnection(DruidDataSource dataSource,String url, String userName, String password) {
        Connection con = null;
        try {
            if (dataSource != null) {
                con = dataSource.getConnection();
            } else {
                con = dataSource(url,userName,password).getConnection();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return con;
    }
    /**
     * 关闭连接
     */
    public static void closeCon( Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (Exception e) {
                log.error("关闭连接对象Connection异常！" + e.getMessage(), e);
            }
        }
    }
}
