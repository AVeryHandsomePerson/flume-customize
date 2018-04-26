package org.apache.flume.sink.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flume.sink.mysql.client.MysqlConnect;
import org.apache.flume.sink.mysql.utile.MysqlUtile;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/10/10.
 */
public class MysqlOperationData {
    public String startCraeterBase(Connection cn,String tablename, String type, String field, String leng){
        MysqlUtile mysqlUtile =new MysqlUtile();
        String  str = mysqlUtile.creater(cn,tablename,type, field,leng);
        return str;
    }

    public String sqlCreaterDataSentence(String tablename, String field){
        MysqlUtile mysqlUtile =new MysqlUtile();
        String sql = mysqlUtile.insert(tablename,field);
        return sql;
    }

    public String [] startOperationData(String tmp){
        MysqlUtile mysqlUtile =new MysqlUtile();
        String[] str = mysqlUtile.dataOperation(tmp);
        return str;
    }




    public static void main(String[] args) {
        String url ="jdbc:mysql://192.168.1.100/flume?Unicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true";
        String userName ="root";
        String password ="123456";
        MysqlOperationData mysqlOperationData =new MysqlOperationData();
        DruidDataSource source = MysqlConnect.dataSource(url, userName, password);
        Connection cn = MysqlConnect.getConnection(source, url, userName, password);//获取连接
//        mysqlOperationData.startCraeterBase(cn,"chongxie","0,0","a,b","50,50");
        JdbcTemplate tt  = new JdbcTemplate(source);
        String sql =mysqlOperationData.sqlCreaterDataSentence("chongxie","a,b");
//        System.out.println(sql);
        List<Object []> list =new ArrayList();
        String[] xiao = "1,2".split(",");
        list.add(xiao);

        tt.batchUpdate(sql,list);

    }
}
