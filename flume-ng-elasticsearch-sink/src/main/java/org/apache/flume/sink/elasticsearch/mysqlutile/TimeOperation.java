package org.apache.flume.sink.elasticsearch.mysqlutile;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by ljh on 2017/11/1.
 */
public class TimeOperation {
    public String insert(String id,String status){
        MysqlConnect myc = null;
    int i = 0 ;
        String sql = "insert into data_acquisition_time (start_time,acquisition_id,status) values (?,?,?)";
        PreparedStatement pstmt;
        myc=new MysqlConnect(sql);
        try {
            pstmt = myc.conn.prepareStatement(sql);
            pstmt.setString(1,MysqlTime.start_Time());
            pstmt.setString(2,id);
            pstmt.setString(3,status);
            i = pstmt.executeUpdate();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        return "";
    }
    public String update(String id,String port){
        int stat;
        MysqlConnect myc=null;
        String sql=null;
        sql="update data_acquisition_time set status="+port+",end_time='"+MysqlTime.stop_Time()+"' where acquisition_id="+id;
        myc=new MysqlConnect(sql);
        try {
            stat = myc.pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            return "500";
        }finally {
            myc.close();
        }
        return "200";
    }

//
//    public String insert_End(String id,String status){
//        int stat;
//        MysqlConnect myc=null;
//        String sql=null;
//        sql="insert into data_acquisition_time (3.WHEN .start_time) values (?) WHEN acquisition_id ="+"id";
//        PreparedStatement pstmt;
//        myc=new MysqlConnect(sql);
//        try {
//            pstmt = myc.conn.prepareStatement(sql);
//            pstmt.setString(1,MysqlTime.start_Time());
////            pstmt.setString(2,id);
////            pstmt.setString(2,status);
//            stat = pstmt.executeUpdate();
//            pstmt.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        return "200";
//    }

    public static void main(String[] args) {
        TimeOperation tm = new TimeOperation();
        tm.update("199",MysqlEvent.END_STATUS_FAIL);
    }
}
