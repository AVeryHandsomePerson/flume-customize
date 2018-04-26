package org.apache.flume.sink.elasticsearch;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.elasticsearch.client.ElasticSearch;
import org.apache.flume.sink.elasticsearch.configuration.ElasticSinkConfigurationConstants;
import org.apache.flume.sink.elasticsearch.mapping.ElasticsearchMapping;
import org.apache.flume.sink.elasticsearch.utile.Utiles;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ljh on 2017/5/26.
 */
public class ElasticSink extends AbstractSink implements Configurable {

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
//    private String id;
    private Utiles utiles = new Utiles();
    private SinkCounter sinkCounter;
    private boolean isLocal;
//    TimeOperation ti =new TimeOperation();
    @Override
    public void configure(Context context) {
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
//        id = context.getString(ElasticSinkConfigurationConstants.ACQUISITION_ID);
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
        Event event;
        try {
            txn.begin();
            BulkRequestBuilder bulkRequest = client.prepareBulk();//创建索引并添加数据
        for (int i= 0; i <batchSize ; i++) {
            event = channel.take();
            if (event != null) {
                String aa = new String(event.getBody());
                bulkRequest.add(client.prepareIndex(index, indextype).setSource(aa));
            }else {
                status = Status.BACKOFF;
                break;
            }
        }
        bulkRequest.execute().actionGet();
        txn.commit();
        } catch (Throwable t) {
                txn.rollback();
        } finally {
            txn.close();
        }
        return status;
    }
    @Override
    public synchronized void stop() {
        super.stop();
//        ti.update(id,MysqlEvent.END_STATUS_SUCCESS);
//        logger.info("-------Record end time-------------");
        if(client!=null)
        client.close();

    }
}
