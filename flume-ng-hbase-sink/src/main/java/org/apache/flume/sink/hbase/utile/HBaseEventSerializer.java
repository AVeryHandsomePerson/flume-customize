package org.apache.flume.sink.hbase.utile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * Created by ljh on 2017/9/25.
 */
public interface HBaseEventSerializer {
    public String rowKeyLeng(String leng,String rowkey,String[] event);

    public List<Put> putEvent(String[] by,String fildname,String indextype,List<Put> list);

    public Configuration cfg(String ip,String prot);

    public void found(Configuration configuration,String index, String indextype);
}
