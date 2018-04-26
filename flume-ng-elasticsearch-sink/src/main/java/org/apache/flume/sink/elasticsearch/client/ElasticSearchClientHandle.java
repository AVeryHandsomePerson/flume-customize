package org.apache.flume.sink.elasticsearch.client;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_PORT;

/**
 * Created by dell on 2017/9/11.
 */
public class ElasticSearchClientHandle implements  ElasticSearchClient{
    public static  String IndexName = "flume";
    public static  String IndexType = "log";
    @Override
    public void close(TransportClient client) {
        if(client !=null ){
            client.close();
        }
    }

    @Override
    public TransportClient connectClient(String hostName,String clusterName) {
        TransportClient client = null;
        try {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.6.2"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.6.113"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.6.4"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    @Override
    public void configure(Context context) {
    }

    public static void main(String[] args) {
        ElasticSearchClientHandle elasticSearchClientHandle =new ElasticSearchClientHandle();
        elasticSearchClientHandle.connectClient("192.168.1.101","");
    }
}
