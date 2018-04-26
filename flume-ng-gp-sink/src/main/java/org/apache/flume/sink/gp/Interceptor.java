package org.apache.flume.sink.gp;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.gp.configuration.GpInterceptorConfigurationConstants;
import org.apache.flume.sink.gp.utile.DataRule;
import org.apache.flume.sink.gp.utile.EventInterceptor;
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
    private static Context context2;
    private EventInterceptor interceptor =new EventInterceptor();
    private String deleteFild;
    private String breaks;
    private String ruleType;
    private String fileVal;
    private DataRule da;
    private String[] fileVals;
    private String fieldsContact;
    private String fieldsDateType;
    private String fildnames;
    private String fiedstype;

    public static void setContext2(Context context) {
        context2 = context;
    }//Mapping data

    @Override
    public void initialize() {
        breaks = cc.get(GpInterceptorConfigurationConstants.SEPARATOR);
//        if (StringUtils.isEmpty(breaks)) {
//            breaks = GpInterceptorConfigurationConstants.FH;
//        }
        deleteFild = cc.get(GpInterceptorConfigurationConstants.DELETE_FILD);//不想要的字段
        ruleType = cc.get(GpInterceptorConfigurationConstants.RULE_TYPE);//规则类型
        fileVal = cc.get(GpInterceptorConfigurationConstants.FILE_VAL);//数据条件//-1,0,1,2
        fileVals = StringUtils.splitPreserveAllTokens(fileVal, ",");
        fieldsContact = cc.get(GpInterceptorConfigurationConstants.FIELDS_CONTACT);//关联原字段索引
        fieldsDateType = cc.get(GpInterceptorConfigurationConstants.FIELDS_DATE_TYPE);//定义时间字段
        fildnames = cc.get(GpInterceptorConfigurationConstants.FILD_NAME);
        fiedstype = cc.get(GpInterceptorConfigurationConstants.FIELDS_TYPE);
        da = new DataRule();
//        logger.info(deleteFild);logger.info(ruleType);logger.info(fileVal);logger.info(fieldsDateType);logger.info(fieldsContact);logger.info(fildnames);
    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charsets.UTF_8);//get event data
        String[] by = StringUtils.splitPreserveAllTokens(body.replace("\"", ""), breaks);//Cleaning event data
        StringBuffer sb = interceptor.DataHandle(by,ruleType,deleteFild,fieldsContact,fileVals,fiedstype,fieldsDateType);
        event.setBody(sb.toString().getBytes());
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
