package org.apache.flume.sink.gp.utile;

/**
 * Created by ljh on 2017/5/24.
 */
public class GpTypeUtile {
    public StringBuffer transition(String[] type) {
        StringBuffer sb=new StringBuffer();
        for (int i = 0; i < type.length; i++) {
            if (type[i].equals("0")) {
                if (i == type.length - 1) {
                    sb.append("varchar");
                } else {
                    sb.append("varchar").append(",");
                }
            } else if (type[i].equals("1")) {
                if (i == type.length - 1) {
                    sb.append("int4");
                } else {
                    sb.append("int4").append(",");
                }

            } else if (type[i].equals("2")) {
                if (i == type.length - 1) {
                    sb.append("float");
                } else {
                    sb.append("float").append(",");
                }
            } else if (type[i].equals("3")) {
                if (i == type.length - 1) {
                    sb.append("timestamp");
                } else {
                    sb.append("timestamp").append(",");
                }
            } else if (type[i].equals("4")) {
                if (i == type.length - 1) {
                    sb.append("bool");
                } else {
                    sb.append("bool").append(",");
                }
            }
        }
        return sb;
    }
}
