package org.apache.flume.sink.mysql.utile;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.sink.mysql.MysqlOperationData;
import org.apache.flume.sink.mysql.client.MysqlConnect;
import org.apache.flume.sink.mysql.configuration.MysqlInterceptorConfigurationConstants;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/10/16.
 */
public class EventInterceptor implements FilterDataInterceptor {
    @Override
    public StringBuffer DataHandle(String[] by, String ruleType, String deleteFild, String fieldsContact, String[] fileVals, String fieldtype, String timerule) {
        DataRule da = new DataRule();
//        String[] type = fieldtype.split(MysqlInterceptorConfigurationConstants.FH);
        String[] ruleData = ruleType.split(MysqlInterceptorConfigurationConstants.FH);//取出划分规则
        List<String> list = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < by.length; i++) {
            if(StringUtils.isEmpty(by[i])){
                list.add("null");
            }else {
                list.add(by[i]);
            }
        }
        if (!deleteFild.equals("nodelete")) {
            String[] delete = deleteFild.split(MysqlInterceptorConfigurationConstants.FH);
            for (int i = 0; i < delete.length; i++) {
                list.remove(Integer.parseInt(delete[i]) - 1);
            }
        }
        String[] fields = fieldsContact.split(MysqlInterceptorConfigurationConstants.FH);//取出数据条件关联源数据下标
        String[] times = timerule.split(MysqlInterceptorConfigurationConstants.FH);
        int j = 0;
        for (String srm : list) {
            if (fields.length == 0) {
                if (j == list.size() - 1) {
                    sb.append(srm);
                } else {
                    sb.append(srm).append(MysqlInterceptorConfigurationConstants.FH);
                }
            } else {
                sb.append(srm).append(MysqlInterceptorConfigurationConstants.FH);
            }
            j++;
        }
        for (int i = 0; i < fields.length; i++) {
            if (!StringUtils.isEmpty(fields[i])) {
                switch (ruleData[i]) {
                    case "0":
                        if (i == ruleData.length - 1) {
                            sb.append(da.phoneRule(by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.phoneRule(by[Integer.parseInt(fields[i]) - 1])).append(MysqlInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "1":
                        if (i == ruleData.length - 1) {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1])).append(MysqlInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "2":
                        if (i == ruleData.length - 1) {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1])).append(MysqlInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "3":
                        if (i == ruleData.length - 1) {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fields[i]) - 1])).append(MysqlInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "4":
                        continue;
                }
            }
        }
        return sb;
    }

    @Override
    public String DataJson(StringBuffer buffer, String name) {
        JSONObject jobj = new JSONObject();
        String[] fildname = name.split(MysqlInterceptorConfigurationConstants.FH);
        String[] sp = buffer.toString().split(MysqlInterceptorConfigurationConstants.FH);
        for (int i = 0; i < fildname.length; i++) {
            jobj.put(fildname[i], sp[i]);
        }
        return jobj.toJSONString();
    }

    @Override
    public String DateTransforMation(String fieldtype, String timerule, String data) {
        String[] type = fieldtype.split(MysqlInterceptorConfigurationConstants.FH);


        return null;
    }


    public static void main(String[] args) {
//        ElasticSearch elasticSearch = new ElasticSearch();
        String[] by =StringUtils.splitPreserveAllTokens(",2017-05-01T00:01:32.000,113.63,34.77,0.00,1,1,1,58679.00,2017-05-01T00:01:38.000,87.33,",",");
        String ruleType = "-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1";
        String fildname = "DevID,Time,Lng,Lat,Speed,GpsFlag,AccFlag,GpsDistance,dads,createdatetime,mainoilper,residualoil";
        String[] fileVals = ",,,,,,,,,,,".split(",");
        String de = "nodelete";
        String fieldsContact = ",,,,,,,,,,,";
        EventInterceptor eventInterceptor = new EventInterceptor();
        StringBuffer sv = eventInterceptor.DataHandle(by, ruleType, de, fieldsContact, fileVals, "0,3,0,0,0,0,0,0,0,3,0,0", "yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd");
//        String s = eventInterceptor.DataJson(sv, fildname);
//        Client client = elasticSearch.addClient(clusterName, "192.168.1.101", "9300");
//        IndexResponse response = client.prepareIndex("abcdesf", "log").setSource(s).get();
        String url = "jdbc:mysql://192.168.1.100/flume?Unicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true";
        String userName = "root";
        String password = "123456";
        JdbcTemplate tt;
        List<Object[]> lists = new ArrayList<Object[]>();
        DruidDataSource source;
        MysqlOperationData mysqlOperationData = new MysqlOperationData();
        String[] str = mysqlOperationData.startOperationData(sv.toString());
        lists.add(str);
        String sql = "insert into yace values(?,?,?,?,?,?,?,?,?,?,?,?)";
        source = MysqlConnect.dataSource(url, userName, password);
        tt = new JdbcTemplate(source);
        tt.batchUpdate(sql, lists);
        System.out.println(sv);
    }
}
