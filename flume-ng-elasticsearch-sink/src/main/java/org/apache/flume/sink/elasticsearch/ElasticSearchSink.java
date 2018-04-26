package org.apache.flume.sink.elasticsearch;

import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.elasticsearch.client.ElasticSearch;
import org.apache.flume.sink.elasticsearch.configuration.ElasticSinkConfigurationConstants;
import org.apache.flume.sink.elasticsearch.mapping.ElasticsearchMapping;
import org.apache.flume.sink.elasticsearch.utile.Utiles;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ljh on 2017/9/21.
 */
public class ElasticSearchSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory
            .getLogger(ElasticSearchSink.class);
    private static final int size = 100;
    private final ElasticsearchMapping mapping = new ElasticsearchMapping();
    private final CounterGroup counterGroup = new CounterGroup();
    private final String clusterName = "elasticsearch";
    private final ElasticSearch elasticSearch = new ElasticSearch();
    private Client client;
    private String hostNames;
    private String names;
    private String post;
    private String index;
    private String indextype;
    private String mappings;
    private String fields_type;
    private String zd_name;
    private String fieldsDateType;
    private int batchSize;
    private Utiles utiles = new Utiles();
    private SinkCounter sinkCounter;
    private boolean isLocal;

    @Override
    public void configure(Context context) {
        //预留 mysql更新配置
        String[] bb = context.getString(ElasticSinkConfigurationConstants.Host_Names).split(ElasticSinkConfigurationConstants.MH);
        hostNames = bb[0];
        post = bb[1];
        index = context.getString(ElasticSinkConfigurationConstants.Index_Name);//索引名称
        index = elasticSearch.convert(index);
        indextype = context.getString(ElasticSinkConfigurationConstants.Index_Type);//索引类型
        mappings = context.getString(ElasticSinkConfigurationConstants.MAPPING);
        fields_type = context.getString(ElasticSinkConfigurationConstants.FIELDS_TYPE);
        names = context.getString(ElasticSinkConfigurationConstants.Cluster_Name, clusterName);
        zd_name = context.getString(ElasticSinkConfigurationConstants.FILD_NAME);
        batchSize = context.getInteger(ElasticSinkConfigurationConstants.BATCH_SIZE, size);
        fieldsDateType = context.getString(ElasticSinkConfigurationConstants.FIELDS_DATE_TYPE);//定义时间字段
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        logger.info("ElasticSearch sink {} started");
        sinkCounter.start();
        try {
            client = elasticSearch.addClient(names, hostNames, post);
            isLocal = mapping.indexExists(client, index);
            if (isLocal) {
                logger.info("ElasticSearch Index Existence");
            } else {
                String rng = mapping.mapping(client, fields_type, zd_name, index, indextype, mappings,fieldsDateType);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            sinkCounter.incrementConnectionFailedCount();
            if (client != null) {
                client.close();
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
        try {
            txn.begin();
            int count;
            for (count = 0; count < batchSize; ++count) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                String aa = new String(event.getBody());
                IndexResponse response = client.prepareIndex(index, indextype).setSource(aa).get();
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
        if (client != null)
            client.close();

        sinkCounter.stop();
    }
}
