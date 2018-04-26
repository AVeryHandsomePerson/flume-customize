package org.apache.flume.sink.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by dell on 2017/9/14.
 */
public class Test001 {
    public static void main(String[] args) {
//        Logger logger=Logger
        TransportClient client = null;
        try {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.6.2"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.6.113"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.6.4"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();

        }
        System.out.println(1);
        JSONObject object=new JSONObject();
        object.put("aaa",111);
//        IndexResponse response = client.prepareIndex("blog", "article").setSource(object.toString()).get();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.add(client.prepareIndex("blog","article").setSource(object.toString()));
        bulkRequest.execute().actionGet();
    }
}
