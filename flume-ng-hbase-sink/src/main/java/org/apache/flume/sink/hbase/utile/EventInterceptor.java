package org.apache.flume.sink.hbase.utile;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.sink.hbase.configuration.HbaseInterceptorConfigurationConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/10/16.
 */
public class EventInterceptor implements FilterDataInterceptor {
    @Override
    public StringBuffer DataHandle(String[] by, String ruleType, String deleteFild, String fieldsContact, String[] fileVals, String rowkey) {
        DataRule da = new DataRule();
        String[] ruleData = ruleType.split(HbaseInterceptorConfigurationConstants.FH);//取出划分规则
        List<String> list = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < by.length; i++) {
            if (StringUtils.isEmpty(by[i])) {
                list.add("null");
            } else {
                list.add(by[i]);
            }
        }
        if (StringUtils.isEmpty(deleteFild)) {
            String[] delete = deleteFild.split(HbaseInterceptorConfigurationConstants.FH);
            for (int i = 0; i < delete.length; i++) {
                list.remove(delete[i]);
            }
        }
        String[] fields = fieldsContact.split(HbaseInterceptorConfigurationConstants.FH);//取出数据条件关联源数据下标
        int j = 0;
        for (String srm : list) {
            if (fields.length == 0) {
                if (j == list.size() - 1) {
                    sb.append(srm);
                } else {
                    if (j == 0) {
                        sb.append(rowkey).append(HbaseInterceptorConfigurationConstants.FH);
                    }
                    sb.append(srm).append(HbaseInterceptorConfigurationConstants.FH);
                }
            } else {
                if (j == 0) {
                    sb.append(rowkey).append(HbaseInterceptorConfigurationConstants.FH);
                }
                sb.append(srm).append(HbaseInterceptorConfigurationConstants.FH);
            }
            j++;
        }
//        System.out.println("============"+sb.toString());
        for (int i = 0; i < fields.length; i++) {
            if (!StringUtils.isEmpty(fields[i])) {
                switch (ruleData[i]) {
                    case "0":
                        if (i == ruleData.length - 1) {
                            sb.append(da.phoneRule(by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.phoneRule(by[Integer.parseInt(fields[i]) - 1])).append(HbaseInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "1":
                        if (i == ruleData.length - 1) {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1])).append(HbaseInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "2":
                        if (i == ruleData.length - 1) {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1])).append(HbaseInterceptorConfigurationConstants.FH);
                        }
                        continue;
                        //replaceRule
                    case "3":
                        if (i == ruleData.length - 1) {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fields[i]) - 1])).append(HbaseInterceptorConfigurationConstants.FH);
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
        String[] fildname = name.split(HbaseInterceptorConfigurationConstants.FH);
        String[] sp = buffer.toString().split(HbaseInterceptorConfigurationConstants.FH);
        for (int i = 0; i < fildname.length; i++) {
            jobj.put(fildname[i], sp[i]);
        }
        return jobj.toJSONString();
    }

    @Override
    public String DateTransforMation(String fieldtype, String timerule, String data) {
        String[] type = fieldtype.split(HbaseInterceptorConfigurationConstants.FH);


        return null;
    }


    public static void main(String[] args) throws IOException {
//        ElasticSearch elasticSearch = new ElasticSearch();
        List<Put> list = new ArrayList<>();
        String[] by = StringUtils.splitPreserveAllTokens("14130677068,2017-05-01T00:00:10.000,113.62,34.68,61.00,1,1,1,34879.00,2017-05-01T00:00:18.000,54.55,", ",");
        String ruleType = "-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1";
        String fildname = "DevID,Time,Lng,Lat,Speed,GpsFlag,AccFlag,GpsDistance,dads,createdatetime,mainoilper,residualoil";
        String[] fileVals = ",,,,,,,,,,,".split(",");
        String de = "nodelete";
        String fieldsContact = ",,,,,,,,,,,";
        Utile utile = new Utile();
        String rowkey = utile.rowKeyLeng("32", "1,3", by);
        EventInterceptor eventInterceptor = new EventInterceptor();
        StringBuffer sv = eventInterceptor.DataHandle(by, ruleType, de, fieldsContact, fileVals, rowkey);
//        String[] ss = StringUtils.splitPreserveAllTokens(sv.toString(), ",");
//        System.out.println(sv);
//        Configuration configuration = utile.cfg("192.168.1.104", "2181");
////            System.out.println(sv);
//        List<Put> lists = new ArrayList<>();
//        Table table;
////        try {
//        utile.found(configuration, "yace", "log");
//        lists = utile.putEvent(ss, fildname, "yace", list);
//        table = new HTable(configuration, Bytes.toBytes("yace"));
//        table.put(list);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }
}
