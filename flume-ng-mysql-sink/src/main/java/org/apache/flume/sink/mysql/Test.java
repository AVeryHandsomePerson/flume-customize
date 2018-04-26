package org.apache.flume.sink.mysql;


import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.sink.mysql.client.MysqlConnect;
import org.apache.flume.sink.mysql.utile.DataRule;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/9/22.
 */
public class Test {
    public static void main(String[] args) {
        DataRule da = new DataRule();
        String[] by = "14130676822,2017-05-01T00:01:32.000,113.63,34.77,0.00,1,1,1,58679.00,2017-05-01T00:01:38.000,87.33,\n".split(",");
        String[] ruleType = "-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1".split(",");
        String[] fildname = "DevID,Time,Lng,Lat,Speed,GpsFlag,AccFlag,GpsDistance,dads,createdatetime,mainoilper,residualoil".split(",");
        String[] fileVals = ",,,,,,,,,,,".split(",");
//        String de = "";
        String[] fieldsContact = ",,,,,,,,,,,".split(",");
        StringBuffer sb = new StringBuffer();
        List<String> list = new ArrayList<>();
        for (int i = 0; i < by.length; i++) {
            list.add(by[i]);
        }
//        if (StringUtils.isEmpty(de)) {
//            list.remove(Integer.parseInt(de) - 1);
//        }
        int j = 0;
        for (String srm : list) {
            if (fieldsContact.length == 0) {
                if (j == list.size() - 1) {
                    sb.append(srm);
                } else {
                    sb.append(srm).append(",");
                }
            } else {
                sb.append(srm).append(",");
            }
            j++;
        }
        for (int i = 0; i < fieldsContact.length; i++) {
            if (!StringUtils.isEmpty(fieldsContact[i])) {
                switch (ruleType[i]) {
                    case "0":
                        if (i == ruleType.length - 1) {
                            sb.append(da.phoneRule(by[Integer.parseInt(fieldsContact[i]) - 1]));
                        } else {
                            sb.append(da.phoneRule(by[Integer.parseInt(fieldsContact[i]) - 1])).append(",");
                        }
                        continue;
                    case "1":
                        if (i == ruleType.length - 1) {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fieldsContact[i]) - 1], by[Integer.parseInt(fieldsContact[i]) - 1]));
                        } else {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fieldsContact[i]) - 1], by[Integer.parseInt(fieldsContact[i]) - 1])).append(",");
                        }
                        continue;
                    case "2":
                        if (i == ruleType.length - 1) {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fieldsContact[i]) - 1], by[Integer.parseInt(fieldsContact[i]) - 1]));
                        } else {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fieldsContact[i]) - 1], by[Integer.parseInt(fieldsContact[i]) - 1])).append(",");
                        }
                        continue;
                        //replaceRule
                    case "3":
                        if (i == ruleType.length - 1) {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fieldsContact[i]) - 1]));
                        } else {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fieldsContact[i]) - 1])).append(",");
                        }
                        continue;
                    case "4":
                }
            }
        }
        System.out.println(sb.toString());
//        JSONObject jobj = new JSONObject();
//        String[] sp = sb.toString().split(",");
//        for (int i = 0; i < fildname.length; i++) {
//            jobj.put(fildname[i], sp[i]);
//        }
//        System.out.println(jobj.toString());
        String url ="jdbc:mysql://192.168.1.100/flume?Unicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true";
        String userName ="root";
        String password ="123456";
        JdbcTemplate tt;
        List<Object[]> lists = new ArrayList<Object[]>();
        DruidDataSource source;
        MysqlOperationData mysqlOperationData = new MysqlOperationData();
        String [] str = mysqlOperationData.startOperationData(sb.toString());
        System.out.println(str.length);
        lists.add(str);
        String sql = "insert into yace values(?,?,?,?,?,?,?,?,?,?,?,?)";
        source = MysqlConnect.dataSource(url, userName, password);
        tt =new JdbcTemplate(source);
        tt.batchUpdate(sql, lists);


    }
}
