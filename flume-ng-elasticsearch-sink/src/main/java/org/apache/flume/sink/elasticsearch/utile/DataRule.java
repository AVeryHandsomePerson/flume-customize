package org.apache.flume.sink.elasticsearch.utile;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by ljh on 2017/8/16.
 */
public class DataRule {
    //手机号处理 **** 0
    public String phoneRule(String str) {
        return str.replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2");
    }

    //字符串长度限制定义 1
    public String stringRule(String length, String str) {
        if (str.length() > Integer.parseInt(length)) {
            str = "";
        }
        return str;
    }

    //枚举匹配字符 2
    public String enumRule(String rule, String str) {
        String[] tmp = rule.split(",");
        for (int i = 0; i < tmp.length; i++) {
            if (tmp[i].equals(str)) {
                return str;
            }
        }
        return "";
    }

    //数据替换
    public String replaceRule(String rule, String str) {
        String tmp = null;
        String[] tms = StringUtils.splitPreserveAllTokens(rule, "|");
        for (int i = 0; i < tms.length; i++) {
            String[] aa = StringUtils.splitPreserveAllTokens(tms[i], ":");
            if(aa.length == 2 && aa[0].equals(str)){
                tmp = aa[1];
                return tmp;
            }else if(aa.length == 1){
                tmp = aa[0];
                return tmp;
            }else if(aa.length == 0){
                tmp = str;
                return tmp;
            }

        }
        return tmp;
    }
    public String timeRule(String rule) {
        SimpleDateFormat sdf=new SimpleDateFormat(rule);
        Date date=new Date();
        String stry = null;
        try {
            stry=sdf.format(date);
            Date da=sdf.parse(stry);
            stry=sdf.format(da);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return stry;
    }



    public String DispatchRule(int it, String str, String rule) {
        DataRule da = new DataRule();
        switch (it) {
            case 1:
                String tmp = da.phoneRule(str);
                return tmp;
            case 2:
                String tp = da.stringRule(rule, str);
                return tp;
            case 3:
                String tm = da.enumRule(rule, str);
                return tm;
        }
        return "";
    }
}
