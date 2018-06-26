package org.apache.flume.sink.elasticsearch.utile;

import com.alibaba.fastjson.JSONObject;
import com.sun.org.apache.bcel.internal.generic.NOP;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.sink.elasticsearch.client.ElasticSearch;
import org.apache.flume.sink.elasticsearch.configuration.ElasticInterceptorConfigurationConstants;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/10/16.
 */
public class EventInterceptor implements FilterDataInterceptor {
    @Override
    public StringBuffer DataHandle(String[] by, String ruleType, String deleteFild, String fieldsContact, String[] fileVals,String fiedstype,String timerule) {
        DataRule da = new DataRule();
        String[] ruleData = ruleType.split(ElasticInterceptorConfigurationConstants.FH);//取出划分规则
        String[] type = fiedstype.split(ElasticInterceptorConfigurationConstants.FH);
        List<String> list = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < by.length; i++) {
            list.add(by[i]);
        }
        if (!deleteFild.equals("nodelete")) {
            String[] delete = deleteFild.split(ElasticInterceptorConfigurationConstants.FH);
            for (int i = 0; i < delete.length; i++) {
                list.remove(Integer.parseInt(delete[i])-1);
            }
        }
        String[] fields = fieldsContact.split(ElasticInterceptorConfigurationConstants.FH);//取出数据条件关联源数据下标
        String[] times = timerule.split(ElasticInterceptorConfigurationConstants.FH);
        int j = 0;
        for (String srm : list) {
            if (fields.length == 0) {
                if (j == list.size() - 1) {
                    sb.append(srm);
                } else {
                    sb.append(srm).append(ElasticInterceptorConfigurationConstants.FH);
                }
            } else {
                sb.append(srm).append(ElasticInterceptorConfigurationConstants.FH);
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
                            sb.append(da.phoneRule(by[Integer.parseInt(fields[i]) - 1])).append(ElasticInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "1":
                        if (i == ruleData.length - 1) {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.stringRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1])).append(ElasticInterceptorConfigurationConstants.FH);
                        }
                        continue;
                    case "2":
                        if (i == ruleData.length - 1) {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.enumRule(fileVals[Integer.parseInt(fields[i]) - 1], by[Integer.parseInt(fields[i]) - 1])).append(ElasticInterceptorConfigurationConstants.FH);
                        }
                        continue;
                        //replaceRule
                    case "3":
                        if (i == ruleData.length - 1) {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fields[i]) - 1]));
                        } else {
                            sb.append(da.replaceRule("3", by[Integer.parseInt(fields[i]) - 1])).append(ElasticInterceptorConfigurationConstants.FH);
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
        String[] fildname = name.split(ElasticInterceptorConfigurationConstants.FH);
        String[] sp = buffer.toString().split(ElasticInterceptorConfigurationConstants.FH);
        for (int i = 0; i < fildname.length; i++) {
            if(i<sp.length){
                jobj.put(fildname[i], sp[i]);
            }
        }
        return jobj.toJSONString();
    }

    @Override
    public String DateTransforMation(String fieldtype, String timerule, String data) {
        String [] type= fieldtype.split(ElasticInterceptorConfigurationConstants.FH);


        return null;
    }


    public static void main(String[] args) throws ParseException {
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//        Date time = format.parse("2017-05-01T23:56:31.000");
//        SimpleDateFormat formats = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String str = formats.format(time);
//        System.out.println(str);


        String clusterName = "elasticsearch";
        ElasticSearch elasticSearch = new ElasticSearch();
//        String[] by = "85145,6137630,任骞骞 任兆真,86,,,,15839313972,,,万科三期采薇苑1号楼1单元703(18)号,2017-01-06 00:00:00,,,,,,85235,,1".split(",");
        String[] by = ",".split(",");
        //        String[] by = StringUtils.splitPreserveAllTokens("18/02/2001 05:27:22 71.123.116.173 - ficticious_website big-fred 123.45.678.901 GET register.asp randomgobuldygook&address=70691-Boris%2Road-Doris%2Town&country=USA&gender=M&age=21-30&buy_for=Myself&favourites=Adventure&username=user_558673 200 0 218 324 67 80 Browser_5 Cookie_112465 www.now_thats_what_i_call_a_search_engine7.com".replace("\"", ""), " - ");//Cleaning event data
        String ruleType = "-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1";
        String fildname = "iCustID,tCustSN,tCustName,iAreaID,tBuildingNum,tUnitNum,tDoorplate,tPhone,tMobile,tEMail,tAddress,dCustAddDate,dOpenAccountDate,dClosingAccountDate,dInfoModifyDate,tCustPassWord,tRemark,iMeterID,iWorkerID,iFlag";
        String[] fileVals = ",,,,,,,,,,,,,,".split(",");
        String de = "nodelete";
        String fieldsContact = ",,,,,,,,,,,,,,";
        EventInterceptor eventInterceptor = new EventInterceptor();
//        StringBuffer sv =new StringBuffer();
//        sv.append("18/02/2001 00:01:20 218.236.234.118 - ficticious_website big-fred 123.45.678.901 GET splash.htm - 200 0 429 359 66 80 Browser_4 - www.hunter15bargain.com");
        StringBuffer sv = eventInterceptor.DataHandle(by, ruleType, de, fieldsContact, fileVals,"0,0,0,0,0,0,0,0,0,0,0,0,0,0,0","yyyy/MM/dd, HH:mm:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss");
        System.out.println(sv.toString());
        Client client = elasticSearch.addClient(clusterName, "192.168.1.101", "9300");
//        String s = eventInterceptor.DataJson(sv, fildname);//dzh_jctxttoes0227 miiscomplete
//        System.out.println(s);
        String s ="{\"coordinates\": \"\"}";
        IndexResponse response = client.prepareIndex("zhilian", "log").setSource(s).get();
        System.out.println(s);


    }
}
