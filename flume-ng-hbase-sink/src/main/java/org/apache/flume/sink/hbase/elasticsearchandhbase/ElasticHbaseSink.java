package org.apache.flume.sink.hbase.elasticsearchandhbase;

import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.hbase.client.ElasticSearch;
import org.apache.flume.sink.hbase.configuration.HbaseSinkConfigurationConstants;
import org.apache.flume.sink.hbase.mapping.ElasticsearchMapping;
import org.apache.flume.sink.hbase.utile.Utile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ljh on 2017/9/21.
 */
public class ElasticHbaseSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory
            .getLogger(ElasticHbaseSink.class);
    private final String clusterName = "elasticsearch";
    private static final int size = 100;
    private final CounterGroup counterGroup = new CounterGroup();
    private final ElasticSearch elasticSearch = new ElasticSearch();
    private final ElasticsearchMapping mapping =new ElasticsearchMapping();
    private String hostNames;
    private String post;
    private String index;
    private String indextype;
    private String fildname;
    private String separator;
    private String names;
    private String externalindex;
    private String mappings;
    private String fields_type;
    private String zd_name;
    private String fieldsDateType;
    //    private String espost;
    private int batchSize;
    private Client client;
    private Configuration configuration;
    private SinkCounter sinkCounter;
    private boolean isLocal = false;
    Utiles utiles = new Utiles();
    Utile utile = new Utile();

    @Override
    public void configure(Context context) {
        //预留 mysql更新配置
        String[] bb = context.getString(HbaseSinkConfigurationConstants.Host_Names).split(":");//待定 es 和 hbase 在相同机器中存在
        hostNames = bb[0];
        post = bb[1];
        index = context.getString(HbaseSinkConfigurationConstants.Index_Name);//表名
        index = elasticSearch.convert(index);//索引大小写转换
        indextype = context.getString(HbaseSinkConfigurationConstants.Index_Type);//列簇名
        fildname = context.getString(HbaseSinkConfigurationConstants.Fild_Name);//所有字段名称
        separator = context.getString(HbaseSinkConfigurationConstants.SEPARATOR);//分隔符
        batchSize = context.getInteger(HbaseSinkConfigurationConstants.BATCH_SIZE, size);
        names = context.getString(HbaseSinkConfigurationConstants.BATCH_SIZE, clusterName);//集群名称
        externalindex = context.getString(HbaseSinkConfigurationConstants.EXTERNAL_INDEX);//外索引库字段
        mappings = context.getString(HbaseSinkConfigurationConstants.MAPPING);
        fields_type = context.getString(HbaseSinkConfigurationConstants.FIELDS_TYPE);
        zd_name = context.getString(HbaseSinkConfigurationConstants.FILD_NAME);
        fieldsDateType = context.getString(HbaseSinkConfigurationConstants.FIELDS_DATE_TYPE);//定义时间字段
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        logger.info("ElasticStarch Hbase sink {} started");
        sinkCounter.start();
        try {
            client = elasticSearch.addClient(names, hostNames, post);
            logger.info("ElasticStarch Connect  Success");
            isLocal = mapping.indexExists(client,index);
            if(isLocal){
                logger.info("ElasticSearch Index Existence");
            }else {
                String rng = mapping.mapping(client,fields_type,zd_name,index,indextype,mappings,fieldsDateType);
                logger.info(rng);
            }
            configuration = utile.cfg(hostNames, post);
            logger.info("Hbase Connect  Success");
            utile.found(configuration, index, indextype);
        } catch (Exception ex) {
            ex.printStackTrace();
            sinkCounter.incrementConnectionFailedCount();
            if (configuration != null) {
                sinkCounter.incrementConnectionClosedCount();
            }
        }
        logger.info("-------link successful-------------");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        List<Put> list = new ArrayList<>();
        BulkRequestBuilder bulkRequestBuilder = null;
        try {
            txn.begin();
            HTable table = new HTable(configuration, Bytes.toBytes(index));
            int count;
            for (count = 0; count < batchSize; ++count) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                bulkRequestBuilder = utiles.addElasticEvent(event, client, fildname, externalindex, index, indextype);
                list = utiles.putEvent(event, separator, fildname, indextype, list);
                if (list != null) {
                    bulkRequestBuilder.execute().actionGet();
                    table.put(list);
                }
            }


            if (count <= 0) {
                sinkCounter.incrementBatchEmptyCount();
                counterGroup.incrementAndGet("channel.underflow");
                status = Status.BACKOFF;
            } else {
                if (count < batchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                    status = Status.BACKOFF;
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }
                sinkCounter.addToEventDrainAttemptCount(count);
            }
            txn.commit();
            sinkCounter.addToEventDrainSuccessCount(count);
            counterGroup.incrementAndGet("transaction.success");
        } catch (Throwable ex) {
            try {
                txn.rollback();
                counterGroup.incrementAndGet("transaction.rollback");//事务回滚
            } catch (Exception ex2) {
                logger.error(
                        "Exception in rollback. Rollback might not have been successful.",
                        ex2);//回滚异常
            }
            if (ex instanceof Error || ex instanceof RuntimeException) {
                logger.error("Failed to commit transaction. Transaction rolled back.",
                        ex);//无法提交事务回滚
                Throwables.propagate(ex);
            } else {
                logger.error("Failed to commit transaction. Transaction rolled back.",
                        ex);
                throw new EventDeliveryException(
                        "Failed to commit transaction. Transaction rolled back.", ex);//无法提交事务回滚
            }
        } finally {
            txn.close();
        }
        return status;
    }

    @Override
    public synchronized void stop() {
        super.stop();

        sinkCounter.stop();
    }
}
