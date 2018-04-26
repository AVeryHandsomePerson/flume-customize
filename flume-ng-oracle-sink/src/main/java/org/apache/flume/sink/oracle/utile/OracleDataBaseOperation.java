package org.apache.flume.sink.oracle.utile;

import java.sql.Connection;

/**
 * Created by ljh on 2017/10/10.
 */
public interface OracleDataBaseOperation {
    /***
     * 创建表
     *   表结构
     */
    public String  creater (Connection cn,String tablename, String type, String field, String leng);

   /**
    * 创建数据插入SQL
    *
    * */
   public String insert(String tablename,String field );

    /***
     *
     *  数据处理
     */
    public  String[] dataOperation(String tmp);

}
