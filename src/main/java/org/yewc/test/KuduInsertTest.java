package org.yewc.test;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.kudu.KuduSink;
import org.apache.flink.streaming.connectors.kudu.connector.KuduColumnInfo;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.types.Row;
import org.apache.kudu.Type;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedHashMap;
import java.util.Map;

public class KuduInsertTest {


    public static void main(String[] args) throws Exception {
        String value = "2019-02-20<|>262079853<|>哦，爸爸怎么了，<|>2019-02-20 20:51:44<|>501<|>7<|>1248165<|>900407294<|>Hyan海燕呐<|>17<|>1203234543<|>1203234543<|>HappyBoss明哥<|>18<|>1<|>1.193.169.0<|>1<|>0<|><|>";

        KuduTableInfo.Builder builder = KuduTableInfo.Builder
                .create("fx_user_chat_ext");

        final Map<String, KuduColumnInfo> cols = getColumns();
        for (String col : cols.keySet()) {
            builder.addColumn(cols.get(col));
        }
        final KuduTableInfo tableInfo = builder.build();

        KuduSink kuduSink = new KuduSink("10.17.4.11", tableInfo);

        // ======================================================

        kuduSink.open(null);

//        kuduSink.invoke((KuduRow) getRowLine(value, "<\\|>", cols));
        FileReader reader = new FileReader("C:\\Users\\barneyye\\Downloads\\data.log");
        BufferedReader br = new BufferedReader(reader);
        int counter = 0;
        String line;
        while ((line = br.readLine()) != null) {
            long s = System.currentTimeMillis();
            kuduSink.invoke((KuduRow) getRowLine(line, "<\\|>", cols));
            long e = System.currentTimeMillis();
            System.out.println("counter: "+ (counter++) + ", use time: " + (e - s));
        }

        kuduSink.close();
    }

    public static Row getRowLine(String value, String split, Map<String, KuduColumnInfo> cols) throws Exception {
        if (StringUtils.isBlank(value)) {
            return null;
        }

        try {
            String[] data = value.split(split, cols.size());
            KuduRow row = new KuduRow(data.length);
            for (int i = 0; i < data.length; i++) {
                row.setField(i, data[i]);
            }

            return row;
        } catch (Exception e) {
            throw e;
        }
    }

    public static Map<String, KuduColumnInfo> getColumns() {
        Map<String, KuduColumnInfo> cols = new LinkedHashMap<>();

        cols.put("dt", KuduColumnInfo.Builder.create("dt", Type.STRING).key(true).nullable(false).build());
        cols.put("sfid", KuduColumnInfo.Builder.create("sfid", Type.STRING).key(true).nullable(false).build());
        cols.put("msg", KuduColumnInfo.Builder.create("msg", Type.STRING).key(true).nullable(false).build());
        cols.put("time", KuduColumnInfo.Builder.create("time", Type.STRING).key(true).nullable(false).build());
        cols.put("cmd", KuduColumnInfo.Builder.create("cmd", Type.STRING).nullable(false).build());
        cols.put("pid", KuduColumnInfo.Builder.create("pid", Type.STRING).nullable(false).build());
        cols.put("rid", KuduColumnInfo.Builder.create("rid", Type.STRING).nullable(false).build());
        cols.put("skid", KuduColumnInfo.Builder.create("skid", Type.STRING).nullable(false).build());
        cols.put("sn", KuduColumnInfo.Builder.create("sn", Type.STRING).nullable(false).build());
        cols.put("srl", KuduColumnInfo.Builder.create("srl", Type.STRING).nullable(false).build());
        cols.put("rfid", KuduColumnInfo.Builder.create("rfid", Type.STRING).nullable(false).build());
        cols.put("rkid", KuduColumnInfo.Builder.create("rkid", Type.STRING).nullable(false).build());
        cols.put("rn", KuduColumnInfo.Builder.create("rn", Type.STRING).nullable(false).build());
        cols.put("rrl", KuduColumnInfo.Builder.create("rrl", Type.STRING).nullable(false).build());
        cols.put("isse", KuduColumnInfo.Builder.create("isse", Type.STRING).nullable(false).build());
        cols.put("ip", KuduColumnInfo.Builder.create("ip", Type.STRING).nullable(false).build());
        cols.put("ver", KuduColumnInfo.Builder.create("ver", Type.STRING).nullable(false).build());
        cols.put("issu", KuduColumnInfo.Builder.create("issu", Type.STRING).nullable(false).build());
        cols.put("p1", KuduColumnInfo.Builder.create("p1", Type.STRING).nullable(false).build());
        cols.put("p2", KuduColumnInfo.Builder.create("p2", Type.STRING).nullable(false).build());
        return cols;
    }
}
