package org.apache.flume.sink.elasticsearch.utile;

import org.apache.flume.Event;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

/**
 * Created by ljh on 2017/10/9.
 */
public class Utiles {
    private final String breakss = ",";
    public BulkRequestBuilder addElasticEvent(Event event, Client client,String index, String indextype) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        String rng = new String(event.getBody());
        bulkRequest.add(client.prepareIndex(index, indextype).setSource(rng));
        return bulkRequest;
    }

}
