package org.apache.flume.sink.hbase.utile;

import org.apache.flume.sink.hbase.configuration.HbaseInterceptorConfigurationConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by ljh on 2017/9/25.
 */
public class Utile implements HBaseEventSerializer {
    private final String breakss = ",";
    private Connection conn;
    @Override
    public String rowKeyLeng(String leng, String rowkey, String[] event) {
        String[] row = rowkey.split(breakss);
        String tmp = "";
        for (int i = 0; i < row.length; i++) {
            if (i == row.length - 1) {
                tmp += event[Integer.parseInt(row[i]) - 1];
                if (tmp.length() == Integer.parseInt(leng)) {
                    tmp = tmp;
                } else if (tmp.length() < Integer.parseInt(leng)) {
                    tmp += "-";
                    i = tmp.length();
                    while (true) {
                        if (i == Integer.parseInt(leng)) {
                            break;
                        }
                        tmp += "0";
                        i++;
                    }
                } else if (tmp.length() > Integer.parseInt(leng)) {
                    String stm ="";
                    for (int j = 0; j < Integer.parseInt(leng); j++) {
                         stm +=String.valueOf(tmp.charAt(j));
                    }
                    tmp = stm;
                }
            } else {
                tmp += event[Integer.parseInt(row[i]) - 1] + "+";
            }
        }
        return tmp;
    }

    @Override
    public List<Put> putEvent(String[] by,String fildname,String indextype,List<Put> list) {
        String[] name = fildname.split(HbaseInterceptorConfigurationConstants.FH);
        for (int j = 0; j <name.length ; j++) {
            Put put = new Put(Bytes.toBytes(by[0]));
            put.add(Bytes.toBytes(indextype), Bytes.toBytes(name[j]), Bytes.toBytes(by[j+1]));
            list.add(put);
        }
        return list;
    }

    @Override
    public Configuration cfg(String ip,String prot) {
        Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", ip);
        cfg.set("hbase.zookeeper.property.clientPort", prot);
        cfg.set("zookeeper.znode.parent","/hbase-unsecure");
        return  cfg;
    }

    @Override
    public void found(Configuration configuration, String index, String indextype) {
        HBaseAdmin hBaseAdmin = null;
        try {
            System.out.println("==================="+index);
            System.out.println("==================="+indextype);
            Admin admin;
            conn = ConnectionFactory.createConnection(configuration);
            admin = conn.getAdmin();
//            hBaseAdmin = new HBaseAdmin(configuration);
//            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(index));//创建表
//            tableDescriptor.addFamily(new HColumnDescriptor(indextype));//创建类簇
            admin.addColumn(TableName.valueOf(index), new HColumnDescriptor(indextype));
//            hBaseAdmin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Utile utile = new Utile();
//        String[] by = "aaa,ccc,ADC,13949895783,50".split(",");
//        System.out.println(utile.rowKeyLeng("19", "1,2,4", by));
        Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "192.168.1.102");
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("zookeeper.znode.parent","/hbase-unsecure");
        utile.found(cfg,"flume_tsesss","delse");
    }
}
