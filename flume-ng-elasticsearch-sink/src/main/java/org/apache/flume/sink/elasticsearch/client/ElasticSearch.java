package org.apache.flume.sink.elasticsearch.client;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetSocketAddress;

/**
 * Created by ljh on 2017/9/21.
 */
public class ElasticSearch implements ElasticSearchClient {

    @Override
    public Client addClient(String clusterName, String ip, String post) {
        Client client;
        Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName) // 设置集群名
                .put("client.transport.sniff", true) // 开启嗅探 , 开启后会一直连接不上, 原因未知
                .put("client.transport.ignore_cluster_name", true) // 忽略集群名字验证, 打开后集群名字不对也能连接上
                .build();
        client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(ip, Integer.parseInt(post))));
        return client;
    }

    @Override
    public String convert(String index) {
        String str = index.toLowerCase();
        return str;
    }

    @Override
    public boolean judge(Client client, String index) {
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = (IndicesExistsResponse) client.admin().indices().exists(request).actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        ElasticSearch elasticSearch = new ElasticSearch();
        Client client = elasticSearch.addClient("elasticsearch", "192.168.1.101", "9300");
        SearchResponse searchResponse = client.prepareSearch("lysgk").setTypes("default").execute().actionGet();
        System.out.println(searchResponse);
    }

}
