package org.apache.flume.sink.hbase.elasticsearchandhbase;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

import java.util.List;

/**
 * Created by ljh on 2017/10/9.
 */
public class Utiles {
    private final String breakss = ",";
    /**
     * 数据处理
     * */
    public List<Put> putEvent(Event event, String breaks, String fildname, String indextype, List<Put> list) {
        String rng = new String(event.getBody());
        String[] we = StringUtils.splitPreserveAllTokens(rng, breaks);
        String[] name = fildname.split(breakss);
        for (int j = 0; j < name.length; j++) {
            Put put = new Put(Bytes.toBytes(we[0]));
            put.add(Bytes.toBytes(indextype), Bytes.toBytes(name[j]), Bytes.toBytes(we[j + 1]));
            list.add(put);
        }
        return list;
    }

    public BulkRequestBuilder addElasticEvent(Event event, Client client, String fildname, String externalindex, String index, String indextype) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        String rng = new String(event.getBody());
//        System.out.println("a,b,c,d,e"+rng);
        String[] rnggood = rng.split(breakss);
        String[] skt = fildname.split(breakss);
        String[] we = externalindex.split(breakss);
        JSONObject jsonObject =new JSONObject();
        for (int i = 0; i < we.length; i++) {
            jsonObject.put(skt[Integer.parseInt(we[i])-1],rnggood[Integer.parseInt(we[i])]);
        }
        bulkRequest.add(client.prepareIndex(index, indextype,rnggood[0]).setSource(jsonObject.toString()));
        return bulkRequest;
    }

}
