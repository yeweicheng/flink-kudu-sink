package org.yewc.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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

public class KuduUpsertTest {


    public static void main(String[] args) throws Exception {
//        String value = "{\"user_id\": 1, \"first_name\": \"yewc\", \"last_name\": \"cheng\"}";
//        String value = "{\"user_id\": 1, \"last_name\": \"cheng\"}";
        String value = "{\"user_id\": 1, \"first_name\": \"yewc\"}";

        KuduTableInfo.Builder builder = KuduTableInfo.Builder
                .create("users");

        final Map<String, KuduColumnInfo> cols = getColumns();
        for (String col : cols.keySet()) {
            builder.addColumn(cols.get(col));
        }
        final KuduTableInfo tableInfo = builder.build();

        KuduSink kuduSink = new KuduSink("10.17.4.11", tableInfo)
                .withEventualConsistency()
                .withUpsertWriteMode()
                .setMutationBufferSpace(20000);

        // ======================================================

        long startAll = System.currentTimeMillis();
        kuduSink.open(null);

//        kuduSink.invoke((KuduRow) getRowLine(value, "<\\|>", cols));
        kuduSink.invoke((KuduRow) getRowLine(value, cols));

        kuduSink.close();
        long endAll = System.currentTimeMillis();
        System.out.println("all use time: " + (endAll - startAll));
    }

    public static Row getRowLine(String value, Map<String, KuduColumnInfo> cols) throws Exception {
        if (StringUtils.isBlank(value)) {
            return null;
        }

        try {
            Map<Integer, String> index = getColumnIndex();
            JSONObject data = JSON.parseObject(value);
            KuduRow row = new KuduRow(cols.size());
            for (int i = 0; i < cols.size(); i++) {
                String key = index.get(i);
                if (data.containsKey(key)) {
                    row.setField(i, data.get(key));
                }
            }

            return row;
        } catch (Exception e) {
            throw e;
        }
    }

    public static Row getRowLine(String value, String split, Map<String, KuduColumnInfo> cols) throws Exception {
        if (StringUtils.isBlank(value)) {
            return null;
        }

        try {
            Map<Integer, String> index = getColumnIndex();
            String[] data = value.split(split, cols.size());
            KuduRow row = new KuduRow(data.length);
            for (int i = 0; i < data.length; i++) {
                if (cols.get(index.get(i)).getType().getName().equals(Type.INT16.getName())) {
                    row.setField(i, Integer.valueOf(data[i]));
                } else {
                    row.setField(i, data[i]);
                }
            }

            return row;
        } catch (Exception e) {
            throw e;
        }
    }

    public static Map<Integer, String> getColumnIndex() {
        Map<Integer, String> cols = new LinkedHashMap<>();
        cols.put(0, "user_id");
        cols.put(1, "first_name");
        cols.put(2, "last_name");

        return cols;
    }

    public static Map<String, KuduColumnInfo> getColumns() {
        Map<String, KuduColumnInfo> cols = new LinkedHashMap<>();

        cols.put("user_id", KuduColumnInfo.Builder.create("user_id", Type.INT16).key(true).nullable(false).build());
        cols.put("first_name", KuduColumnInfo.Builder.create("first_name", Type.STRING).nullable(true).build());
        cols.put("last_name", KuduColumnInfo.Builder.create("last_name", Type.STRING).nullable(true).build());

        return cols;
    }
}
