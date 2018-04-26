package org.apache.flume.sink.hbase.mapping;

/**
 * Created by ljh on 2017/7/13.
 */
public class ElasticsearchType {

    public StringBuffer transition(String[] aa) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < aa.length; i++) {
            if (aa[i].equals("0")) {
                if (i == aa.length - 1) {
                    sb.append("string");
                } else {
                    sb.append("string").append(",");
                }
            } else if (aa[i].equals("1")) {
                if (i == aa.length - 1) {
                    sb.append("long");
                } else {
                    sb.append("long").append(",");
                }
            } else if (aa[i].equals("2")) {
                if (i == aa.length - 1) {
                    sb.append("double");
                } else {
                    sb.append("double").append(",");
                }
            } else if (aa[i].equals("3")) {
                if (i == aa.length - 1) {
                    sb.append("date");
                } else {
                    sb.append("date").append(",");
                }
            } else if (aa[i].equals("4")) {
                if (i == aa.length - 1) {
                    sb.append("tinyint");
                } else {
                    sb.append("tinyint").append(",");
                }
            }
        }
        return sb;
    }
}
