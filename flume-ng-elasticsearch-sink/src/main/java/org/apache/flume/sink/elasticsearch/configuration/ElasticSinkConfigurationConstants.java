package org.apache.flume.sink.elasticsearch.configuration;

/**
 * Created by ljh on 2017/10/16.
 */
public class ElasticSinkConfigurationConstants {
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
     * elasticsearch 集群名称
     */
    public static final String Cluster_Name = "clusterName";
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
     * 定义时间字段
     */
    public static final String FIELDS_DATE_TYPE = "fields_date_type";
    /***
     * 获得
     *
     */
    public static final String ACQUISITION_ID = "acquisition_id";


}
