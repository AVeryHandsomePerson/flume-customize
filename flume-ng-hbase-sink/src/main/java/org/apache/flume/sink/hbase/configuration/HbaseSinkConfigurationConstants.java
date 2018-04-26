package org.apache.flume.sink.hbase.configuration;

/**
 * Created by ljh on 2017/10/16.
 */
public class HbaseSinkConfigurationConstants {
    /***
     *连接数据地址
     */
    public static final String Host_Names = "hostNames";
    /***
     * 索引名称
     */
    public static final String Index_Name = "indexName";
    /***
     *类型名称
     */
    public static final String Index_Type = "indexType";
    /***
     *字段名
     */
    public static final String Fild_Name = "fildname";
    /***
     * 分隔符
     */
    public static final String SEPARATOR = "separator";
    /***
     * 批量处理数据
     */
    public static final String BATCH_SIZE = "batchSize";
    /***
     * 连接分割符
     */
    public static final String MH = ":";
    /**
     * es映射
     */
    public static final String MAPPING = "mapping";
    /**
     * es字段类型
     */
    public static final String FIELDS_TYPE = "fields_type";
    /**
     * 字段名
     */
    public static final String FILD_NAME = "fildname";
    /**
     * 外索引库字段编号
     */
    public static final String EXTERNAL_INDEX="external_index";
    /**
     * 定义时间字段
     */
    public static final String FIELDS_DATE_TYPE = "fields_date_type";
}
