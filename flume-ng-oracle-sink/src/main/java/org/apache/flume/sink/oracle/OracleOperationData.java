package org.apache.flume.sink.oracle;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flume.sink.oracle.client.OracleConnect;
import org.apache.flume.sink.oracle.utile.OracleUtile;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/10/10.
 */
public class OracleOperationData {
    public String startCraeterBase(Connection cn,String tablename, String type, String field, String leng){
        OracleUtile mysqlUtile =new OracleUtile();
        String  str = mysqlUtile.creater(cn,tablename,type, field,leng);
        return str;
    }

    public String sqlCreaterDataSentence(String tablename, String field){
        OracleUtile mysqlUtile =new OracleUtile();
        String sql = mysqlUtile.insert(tablename,field);
        return sql;
    }

    public String [] startOperationData(String tmp){
        OracleUtile mysqlUtile =new OracleUtile();
        String[] str = mysqlUtile.dataOperation(tmp);
        return str;
    }

    public static void main(String[] args) {
        String url ="jdbc:oracle:thin:@192.168.1.104:1521:orcl11g";
        String userName ="test";
        String password ="123456";
        OracleOperationData mysqlOperationData =new OracleOperationData();
        DruidDataSource source = OracleConnect.dataSource(url, userName, password);
        Connection cn = OracleConnect.getConnection(source, url, userName, password);//获取连接
        mysqlOperationData.startCraeterBase(cn,"chongxie","0,0","a,b","50,50");
        JdbcTemplate tt  = new JdbcTemplate(source);
        String sql =mysqlOperationData.sqlCreaterDataSentence("chongxie","a,b");
        List<Object []> list =new ArrayList();
        String[] xiao = "1,2".split(",");
        list.add(xiao);

        tt.batchUpdate(sql,list);

    }
}
