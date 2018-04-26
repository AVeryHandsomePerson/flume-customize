package org.apache.flume.sink.gp.utile;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by ljh on 2017/10/10.
 */
public class GpUtile implements GpDataBaseOperation {
    /***
     *
     * 表创建方法
     *
     * */
    @Override
    public String creater(Connection cn, String tablename, String type, String field, String leng) {
        StringBuffer sb = new StringBuffer();
        String[] finame = field.split(",");
        String[] types = type.split(",");
        GpTypeUtile utile = new GpTypeUtile();
        StringBuffer ssb = utile.transition(types);
        String[] typee = ssb.toString().split(",");
        String[] splitlength = StringUtils.splitPreserveAllTokens(leng, ",");
        StringBuffer st = new StringBuffer();
        for (int i = 0; i <typee.length ; i++) {
            if(typee[i].equals("int4")||typee[i].equals("timestamp")) {
                if(i==typee.length-1){
                    st.append(finame[i]).append(" ").append(typee[i]);
                }else {
                    st.append(finame[i]).append(" ").append(typee[i]).append(",");
                }
            }else {
                if(i==typee.length-1){
                    st.append(finame[i]).append(" ").append(typee[i]).append("(").append(splitlength[i]).append(")");
                }else {
                    st.append(finame[i]).append(" ").append(typee[i]).append("(").append(splitlength[i]).append("),");
                }
            }
        }
        String sql = "create table " + tablename + "(" + st.toString() + ");";
        Statement stmt;
        try {
            stmt =  cn.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "创建成功";
    }
    /***
     * 创建插入数据sql
     * */
    @Override
    public String insert(String tablename, String field) {
        StringBuffer sbb = new StringBuffer();
        String[] finame = field.split(",");
        sbb.append("insert into ");
        sbb.append(tablename);
        sbb.append(" values(");
        for (int i = 0; i < finame.length; i++) {
            if (i == finame.length - 1) {
                sbb.append("?)");
            } else {
                sbb.append("?,");
            }
        }
        return sbb.toString();
    }

    @Override
    public String[] dataOperation(String tmp) {
        String[] sre =  tmp.split(",");
        return sre;
    }
}
