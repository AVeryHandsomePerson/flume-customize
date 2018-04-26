package org.apache.flume.sink.gp;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flume.sink.gp.client.GpConnect;
import org.apache.flume.sink.gp.utile.GpUtile;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/10/10.
 */
public class GpOperationData {
    public String startCraeterBase(Connection cn,String tablename, String type, String field, String leng){
        GpUtile mysqlUtile =new GpUtile();
        String  str = mysqlUtile.creater(cn,tablename,type, field,leng);
        return str;
    }

    public String sqlCreaterDataSentence(String tablename, String field){
        GpUtile mysqlUtile =new GpUtile();
        String sql = mysqlUtile.insert(tablename,field);
        return sql;
    }

    public String [] startOperationData(String tmp){
        GpUtile mysqlUtile =new GpUtile();
        String[] str = mysqlUtile.dataOperation(tmp);
        return str;
    }




    public static void main(String[] args) {
        String url ="jdbc:pivotal:greenplum://192.168.1.101:5432;DatabaseName=gptestdb";
        //jdbc:postgresql://192.168.1.101:5432/gptestdb
        String userName ="gpadmin";
        String password ="123456";
        GpOperationData mysqlOperationData =new GpOperationData();
        DruidDataSource source = GpConnect.dataSource(url, userName, password);
        Connection cn = GpConnect.getConnection(source, url, userName, password);//获取连接
        mysqlOperationData.startCraeterBase(cn,"chongxie","0,0","a,b","50,50");

        JdbcTemplate tt  = new JdbcTemplate(source);
        String sql =mysqlOperationData.sqlCreaterDataSentence("chongxie","a,b");
//        System.out.println(sql);
        List<Object []> list =new ArrayList();
        String[] xiao = "1,2".split(",");
        list.add(xiao);

        tt.batchUpdate(sql,list);

    }
}
