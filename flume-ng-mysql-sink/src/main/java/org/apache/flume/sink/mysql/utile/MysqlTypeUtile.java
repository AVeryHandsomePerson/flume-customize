package org.apache.flume.sink.mysql.utile;

/**
 * Created by ljh on 2017/5/24.
 */
public class MysqlTypeUtile {
    public StringBuffer transition(String[] aa) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < aa.length; i++) {
            if (aa[i].equals("0")) {
                if (i == aa.length - 1) {
                    sb.append("varchar");
                } else {
                    sb.append("varchar").append(",");
                }
            } else if (aa[i].equals("1")) {
                if (i == aa.length - 1) {
                    sb.append("int");
                } else {
                    sb.append("int").append(",");
                }

            } else if (aa[i].equals("2")) {
                if (i == aa.length - 1) {
                    sb.append("float");
                } else {
                    sb.append("float").append(",");
                }
            } else if (aa[i].equals("3")) {

                if (i == aa.length - 1) {
                    sb.append("datetime");
                } else {
                    sb.append("datetime").append(",");
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
