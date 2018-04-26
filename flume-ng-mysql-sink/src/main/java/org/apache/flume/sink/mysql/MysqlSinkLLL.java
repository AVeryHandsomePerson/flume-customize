package org.apache.flume.sink.mysql;


import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.mysql.client.MysqlConnect;
import org.apache.flume.sink.mysql.configuration.MysqlSinkConfigurationConstants;
import org.apache.flume.sink.mysql.utile.MysqlTypeUtile;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/5/23.
 */
public class MysqlSinkLLL extends AbstractSink implements Configurable {
    private static DruidDataSource dataSource = null;
    private static JdbcTemplate tt;
    MysqlConnect mc = new MysqlConnect();
    private Connection cn = null;
    private Statement stmt = null;
    private String tableName;
    private String fildname;
    private String type;
    private String separator;
    private String leng;
    private String sql;
    private int batchSize;
    private String url ="jdbc:mysql://192.168.1.100/flume?Unicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true";
    private String userName ="root";
    private String password ="123456";

    public void configure(Context context) {
        url = context.getString(MysqlSinkConfigurationConstants.URL);
//        userName = context.getString(MysqlSinkConfigurationConstants.USER_NAME);//用户名
//        password = context.getString(MysqlSinkConfigurationConstants.PASSWORD);//密码
        System.out.println("----------------------"+url);
        //jdbc:mysql://192.168.1.100:3306/flume?characterEncoding=utf8&tinyInt1isBit=false
        tableName = context.getString("indexName");//表名称
        fildname = context.getString("fildname");//字段名称
        type = context.getString("types");//数据类型
        separator = context.getString("separator");//分隔符
        leng = context.getString("filds_length");//字段长度
        batchSize = context.getInteger("batchSize", 100);
        Preconditions.checkNotNull(batchSize > 0, "batchSize must be a positive number!!");
//        url = context.getString("url","jdbc:mysql://192.168.1.100/flume?Unicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true");
//        userName = context.getString("userName","root");
//        password = context.getString("password","123456");
        if (dataSource == null) {
            dataSource = new DruidDataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(userName);
            dataSource.setPassword(password);
            dataSource.setMaxActive(15);// 设置最大并发数
            dataSource.setInitialSize(2);// 数据库初始化时，创建的连接个数
            dataSource.setMaxWait(60000);
            dataSource.setMinIdle(1);// 最小空闲连接数
            dataSource.setTimeBetweenEvictionRunsMillis(5 * 60 * 1000);// 5分钟检测一次是否有死掉的线程
            dataSource.setMinEvictableIdleTimeMillis(300000);// 空闲连接60秒中后释放
            dataSource.setTestWhileIdle(true);
            // 检测连接有效性
            dataSource.setTestOnBorrow(true);
            dataSource.setValidationQuery("select 1");
            dataSource.setPoolPreparedStatements(true);
            dataSource.setMaxOpenPreparedStatements(15);
            tt = new JdbcTemplate(dataSource);
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        String checkTable = "show tables like \"" + fildname + "\"";
        String deleterTable = "DROP TABLE " + fildname;
        String[] finame = fildname.split(",");
        String[] types = type.split(",");
        MysqlTypeUtile mysqlUtile = new MysqlTypeUtile();
        StringBuffer ssb = mysqlUtile.transition(types);
        String[] typee = ssb.toString().split(",");
        String[] splitlength = StringUtils.splitPreserveAllTokens(leng,",");
        StringBuffer st = new StringBuffer();
        for (int i = 0; i < splitlength.length; i++) {
            if (i == typee.length - 1) {
                if(typee[i].equals("datetime")){
                    st.append(finame[i]).append(" ").append(typee[i]);
                }else {
                    st.append(finame[i]).append(" ").append(typee[i]).append("(").append(splitlength[i]).append(")");
                }
            } else {
                if(typee[i].equals("datetime")){
                    st.append(finame[i]).append(" ").append(typee[i]).append(",");
                }else {
                    st.append(finame[i]).append(" ").append(typee[i]).append("(").append(splitlength[i]).append("),");
                }
            }
        }
        sql = "create table " + tableName + "(" + st.toString() + ")  DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;";
        try {
            cn = MysqlConnect.getConnection(dataSource, url, userName, password);
            stmt = cn.createStatement();
            ResultSet resultSet = stmt.executeQuery(checkTable);
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        StringBuffer sbb = new StringBuffer();
        sbb.append("insert into ");
        sbb.append(tableName);
        sbb.append(" values(");
        for (int i = 0; i < finame.length; i++) {
            if (i == finame.length - 1) {
                sbb.append("?)");
            } else {
                sbb.append("?,");
            }
        }
        sql = sbb.toString();
    }

    @Override
    public synchronized void stop() {
        super.stop();

    }

    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        StringBuffer sb = new StringBuffer();
        Transaction txn = channel.getTransaction();
        List<Object[]> list = new ArrayList<Object[]>();
        Event event;
        txn.begin();
        // This try clause includes whatever Channel operations you want to do
        try {
            list = new ArrayList<Object[]>();
            for (int i = 0; i < batchSize; i++) {
                event = channel.take();
                if (event != null) {
                    String aa = new String(event.getBody());
                    String[] data = StringUtils.splitPreserveAllTokens(aa, separator);//对数据进行划分
                    list.add(data);
                } else {
                    status = Status.BACKOFF;
                    break;
                }
            }
            try {
                if (list.size() > 0) {
                    tt.batchUpdate(sql, list);
                }
            }catch (Exception e){
                System.out.println(e.toString());
            }

            txn.commit();
        } catch (Throwable t) {
            txn.rollback();
        } finally {
            txn.close();
        }
        return status;
    }
}
