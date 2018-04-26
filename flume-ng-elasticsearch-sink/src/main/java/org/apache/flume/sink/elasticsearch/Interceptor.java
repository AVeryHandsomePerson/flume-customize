package org.apache.flume.sink.elasticsearch;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.elasticsearch.configuration.ElasticInterceptorConfigurationConstants;
import org.apache.flume.sink.elasticsearch.utile.DataRule;
import org.apache.flume.sink.elasticsearch.utile.EventInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by ljh on 2017/9/22.
 */
public class Interceptor implements org.apache.flume.interceptor.Interceptor {
    private static final Logger logger = LoggerFactory
            .getLogger(Interceptor.class);
    ImmutableMap<String, String> cc = context2.getParameters();
    private EventInterceptor interceptor =new EventInterceptor();
    private static Context context2;
    private String deleteFild;
    private String breaks;
    private String ruleType;
    private String fileVal;
    private DataRule da;
    private String[] fileVals;
    private String fieldsContact;
    private String fieldsDateType;
    private String fiedstype;
    private String fildnames;
//    private String id;
//    TimeOperation ti =new TimeOperation();
    public static void setContext2(Context context) {
        context2 = context;
    }//Mapping data

    @Override
    public void initialize() {
        if(cc.get(ElasticInterceptorConfigurationConstants.SEPARATOR).equals("space")){
            breaks=" ";
        }else {
            breaks = cc.get(ElasticInterceptorConfigurationConstants.SEPARATOR);
        }
        deleteFild = cc.get(ElasticInterceptorConfigurationConstants.DELETE_FILD);//不想要的字段
        ruleType = cc.get(ElasticInterceptorConfigurationConstants.RULE_TYPE);//规则类型
        fileVal = cc.get(ElasticInterceptorConfigurationConstants.FILE_VAL);//数据条件//-1,0,1,2
        fileVals = StringUtils.splitPreserveAllTokens(fileVal, ",");
        fieldsContact = cc.get(ElasticInterceptorConfigurationConstants.FIELDS_CONTACT);//关联原字段索引
        fieldsDateType = cc.get(ElasticInterceptorConfigurationConstants.FIELDS_DATE_TYPE);//定义时间字段
        fildnames = cc.get(ElasticInterceptorConfigurationConstants.FILD_NAME);
        fiedstype = cc.get(ElasticInterceptorConfigurationConstants.FIELDS_TYPE);
        da = new DataRule();
  }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charsets.UTF_8);//get event data
        String[] by = body.replace("\"", "").split(breaks) ;//Cleaning event data
//        System.out.println("------"+breaks);
//        System.out.println("------------------"+by.length);
        StringBuffer sb = interceptor.DataHandle(by,ruleType,deleteFild,fieldsContact,fileVals,fiedstype, fieldsDateType);
        String str = interceptor.DataJson(sb,fildnames);
        System.out.println(str);
//        logger.info("-------------"+str);
        event.setBody(str.getBytes());
        return event;
    }

    @Override
    public void close() {

    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> intercepted = Lists.newArrayListWithCapacity(list.size());
        for (Event event : list) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {
        @Override
        public org.apache.flume.interceptor.Interceptor build() {
            return new Interceptor();
        }

        @Override
        public void configure(Context context) {
            Interceptor.setContext2(context);
        }
    }

}
