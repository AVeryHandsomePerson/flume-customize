package org.apache.flume.sink.mysql.utile;

/**
 * Created by ljh on 2017/10/16.
 */
public interface FilterDataInterceptor {
    /**
     *
     * @param by 数据
     * @param ruleType 规则类型
     * @param deleteFild 不想要的数据
     * @param fieldsContact 关联原字段索引
     * @param fileVals 数据条件
     * @return 返回处理后的数据结果
     */
    StringBuffer DataHandle(String[] by, String ruleType, String deleteFild, String fieldsContact, String[] fileVals,String fieldtype, String timerule);

    /**
     *
     * @param buffer 规则处理后的数据
     * @param name 字段名称
     * @return 返回入库结果
     */
    String DataJson(StringBuffer buffer, String name);
    /**
     *时间
     */
    String DateTransforMation(String fieldtype, String timerule, String data);

}
