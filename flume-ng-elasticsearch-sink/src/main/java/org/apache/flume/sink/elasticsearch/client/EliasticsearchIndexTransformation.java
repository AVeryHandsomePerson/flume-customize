package org.apache.flume.sink.elasticsearch.client;

import com.sun.org.apache.bcel.internal.generic.FADD;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;

/**
 * Created by dell on 2017/9/11.
 */
public class EliasticsearchIndexTransformation {
    private  final boolean SIICCESS = true;
    private  final boolean FAIL = false;

    public String IndexTransformation( String indexName){
//        if(indexExists(client,indexName)){
//            return indexName;
//        }else {
          indexName =  indexName.toLowerCase();
//        }
        return indexName;
    }

    public boolean indexExists(Client client ,String indexName){
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response =client.admin().indices().exists(request).actionGet();
        if(response.isExists()){
            return SIICCESS;
        }
        return FAIL;
    }
}