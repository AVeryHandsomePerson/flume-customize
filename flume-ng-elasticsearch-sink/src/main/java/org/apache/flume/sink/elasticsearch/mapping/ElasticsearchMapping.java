package org.apache.flume.sink.elasticsearch.mapping;

import org.apache.flume.sink.elasticsearch.client.ElasticSearch;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ljh on 2017/7/13.
 */
public class ElasticsearchMapping {
    private final Boolean  SUCCESS = true;
    private final Boolean  FAIL = false;

    public Boolean indexExists(Client client,String indexname) {
        IndicesExistsRequest request =new IndicesExistsRequest(indexname);
        IndicesExistsResponse response =client.admin().indices().exists(request).actionGet();
        if(response.isExists()){
            return SUCCESS;
        }else {
            client.admin().indices().prepareCreate(indexname).execute().actionGet();
        }
        return FAIL;
    }

    public String mapping(Client client,String types,String fildName,String indexname,String indextype,String getMapping,String fieldsDateType) {
        String []type = types.split(",");
//        String [] stre = timerule.split(",");
        String []stre =  fieldsDateType.split(",");
        ElasticsearchType elasticsearchType = new ElasticsearchType();
        StringBuffer sb = elasticsearchType.transition(type);
        String[] aa = sb.toString().split(",");
        String[] bb = fildName.split(",");
        XContentBuilder mapping = null;
        Map<String, String> map = new HashMap<String, String>();
//        map.put("format", "yyyy-MM-dd'T'HH:mm:ss.SSS");
        map.put("index", "not_analyzed");
        map.put("analyzer", "ik");
        String lw = null;
        String pw = null;
        String[] cc = null;
        try {
            for (int i = 0; i < aa.length; i++) {
                if (aa[i].equals("date")) {
                    lw = "format";
                    pw = stre[i];
                }else if(!getMapping.equals("nomapping")){
                    cc =getMapping.split(",");
                    for (int j = 0; j < cc.length; j++) {
                        if(i == Integer.parseInt(cc[j])-1){
                            lw = "analyzer";
                            pw = map.get(lw);
                            break;
                        } else {
                            lw = "index";
                            pw = map.get(lw);
                        }
                    }
                }else {
                    lw = "index";
                    pw = map.get(lw);
                }
                mapping = XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject(indextype)
                        .startObject("properties")
                        .startObject(bb[i]).field("type", aa[i]).field( lw, pw).endObject()
                        .endObject()
                        .endObject()
                        .endObject();
                IndicesAdminClient indicesAdminClient = client.admin().indices();
                PutMappingRequestBuilder putMappingRequest = indicesAdminClient.preparePutMapping(indexname);
                putMappingRequest.setType(indextype);
                putMappingRequest.setSource(mapping);
                PutMappingResponse request = putMappingRequest.get();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Successful Execution";
    }

    private Boolean ArrayIndexOutOfBoundsException(String [] by,String leng){

        try {
            for (int i = 0; i < leng.length(); i++) {

            }
        }catch (ArrayIndexOutOfBoundsException e){

        }


        return  true;
    }

    public static void main(String[] args) {
        ElasticsearchMapping mapping=new ElasticsearchMapping();
        ElasticSearch search =new ElasticSearch();
//        boolean a=  mapping.indexExists("elasticsearch","192.168.1.101","9300","yarresourcemanagerq1");
        Client client =   search.addClient("elasticsearch","192.168.1.101","9300");
        String le = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0";//
        String tm = "yyyy/MM/dd, HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss,yyyy-MM-dd HH\\:mm\\:ss";
        String name = "date,time,c-ip,cs-username,s-sitename,s-computername,s-ip,cs-method,cs-uri-stem,cs-uri-query,sc-status,sc-win32-status,sc-bytes,cs-bytes,time-taken,s-port,cs(User-Agent),cs(Cookie),cs(Referer)";//
        mapping.mapping(client,le,name,"miis_complete","log","nomapping",tm);//
//        System.out.println(a);
    }
}