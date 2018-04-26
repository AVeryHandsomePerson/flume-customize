package org.apache.flume.sink.gp.utile;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.sink.gp.configuration.GpInterceptorConfigurationConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/10/16.
 */
public class EventInterceptor implements FilterDataInterceptor {
    @Override
    public StringBuffer DataHandle(String[] by, String ruleType, String deleteFild, String fieldsContact, String[] fileVals,String fieldtype, String timerule) {
        DataRule da = new DataRule();
        String[] type = fieldtype.split(GpInterceptorConfigurationConstants.FH);
        String[] ruleData = ruleType.split(GpInterceptorConfigurationConstants.FH);//取出划分规则
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
            String[] delete = deleteFild.split(GpInterceptorConfigurationConstants.FH);
            for (int i = 0; i < delete.length; i++) {
                list.remove(Integer.parseInt(delete[i]) - 1);
            }
        }
        String[] fields = fieldsContact.split(GpInterceptorConfigurationConstants.FH);//取出数据条件关联源数据下标
        String[] times = timerule.split(GpInterceptorConfigurationConstants.FH);
        int j = 0;
        for (String srm : list) {
            if (fields.length == 0 ) {
                if (j == list.size() - 1 ) {
//                    if(type[j].equals("3")){
//                        SimpleDateFormat format = new SimpleDateFormat(times[j]);
//                        Date time = null;
//                        try {
//                            time = format.parse(srm);
//                            String str = format.format(time);
//                            sb.append(str);
//                        } catch (ParseException e) {
//                            e.printStackTrace();
//                        }
//                    }else {
                        sb.append(srm);
//                    }
                } else {
//                    if(type[j].equals("3")){
//                        SimpleDateFormat format = new SimpleDateFormat(times[j]);
//                        Date time = null;
//                        try {
//                            time = format.parse(srm);
//                            String str = format.format(time);
//                            sb.append(str).append(GpInterceptorConfigurationConstants.FH);
//                        } catch (ParseException e) {
//                            e.printStackTrace();
//                        }
//                    }else {
                        sb.append(srm).append(GpInterceptorConfigurationConstants.FH);
//                    }
                }
            } else{
                sb.append(srm).append(GpInterceptorConfigurationConstants.FH);
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
                            sb.append(da.phoneRule(by[Integer.parseInt(fields[i]) - 1])).append(GpInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "1":
                        if (i == ruleData.length - 1) {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1])).append(GpInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "2":
                        if (i == ruleData.length - 1) {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1])).append(GpInterceptorConfigurationConstants.FH);
                        }
                        continue;
                        //replaceRule
                    case "3":
                        if (i == ruleData.length - 1) {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fields[i]) - 1])).append(GpInterceptorConfigurationConstants.FH);
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
        String[] fildname = name.split(GpInterceptorConfigurationConstants.FH);
        String[] sp = buffer.toString().split(GpInterceptorConfigurationConstants.FH);
        for (int i = 0; i < fildname.length; i++) {
            jobj.put(fildname[i], sp[i]);
        }
        return jobj.toJSONString();
    }

    @Override
    public String DateTransforMation(String fieldtype, String timerule, String data) {
        String[] type = fieldtype.split(GpInterceptorConfigurationConstants.FH);


        return null;
    }


    public static void main(String[] args) {
        String[] by =StringUtils.splitPreserveAllTokens(",2017-05-01T00:01:32.000,113.63,34.77,0.00,1,1,1,58679.00,2017-05-01T00:01:38.000,87.33,",",");
        String ruleType = "-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1";
        String fildname = "DevID,Time,Lng,Lat,Speed,GpsFlag,AccFlag,GpsDistance,dads,createdatetime,mainoilper,residualoil";
        String[] fileVals = ",,,,,,,,,,,".split(",");
        String de = "nodelete";
        String fieldsContact = ",,,,,,,,,,,";
        EventInterceptor eventInterceptor = new EventInterceptor();
        StringBuffer sv = eventInterceptor.DataHandle(by, ruleType, de, fieldsContact, fileVals, "0,0,0,0,0,0,0,0,0,0,0,0", "yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd");

        System.out.println(sv);
    }
}
