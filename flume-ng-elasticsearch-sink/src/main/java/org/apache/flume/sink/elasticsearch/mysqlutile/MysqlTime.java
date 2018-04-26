package org.apache.flume.sink.elasticsearch.mysqlutile;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by ljh on 2017/10/31.
 */
public class MysqlTime {
   public static String start_Time(){
       SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
       Date da= new Date();
       String  str = format.format(da);
       return str;
   }
    public static String stop_Time(){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date da= new Date();
        String  str = format.format(da);
        return str;
    }
    public static void main(String[] args) {


    }
}
