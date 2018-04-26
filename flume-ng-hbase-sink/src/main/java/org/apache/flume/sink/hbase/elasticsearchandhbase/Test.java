package org.apache.flume.sink.hbase.elasticsearchandhbase;


import org.apache.commons.lang.StringUtils;
import org.apache.flume.sink.hbase.utile.DataRule;
import org.apache.flume.sink.hbase.utile.Utile;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/9/22.
 */
public class Test {
    public static void main(String[] args) {
        DataRule da = new DataRule();
        String[] by = "aaa,ccc,ADC,13949895783,50".split(",");
        String[] ruleType = "-1,-1,-1,-1,-1,1,0".split(",");
        String[] fildname = "phone01,phone02,phone03,phone04,phone05,phone06,phone07".split(",");
        String rok ="1,4";
        String[] fileVals = ",,3,,,".split(",");
        String de = "4";
        String[] fieldsContact = ",,,,,3,4".split(",");
        StringBuffer sb = new StringBuffer();
        List<String> list = new ArrayList<>();
        for (int i = 0; i < by.length; i++) {
            list.add(by[i]);
        }
        if (StringUtils.isEmpty(de)) {
            list.remove(Integer.parseInt(de) - 1);
        }
        Utile utile =new Utile();
        int j = 0;
        for (String srm : list) {
            if (fieldsContact.length == 0) {
                if (j == list.size() - 1) {
                    sb.append(srm);
                } else {
                    sb.append(srm).append(",");
                }
            } else {
                if(j == 0){
                    sb.append(utile.rowKeyLeng("13","1,4",by)).append(",");
                }
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
    }
}
