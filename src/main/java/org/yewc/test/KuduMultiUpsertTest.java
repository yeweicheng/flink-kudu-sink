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

import java.util.LinkedHashMap;
import java.util.Map;

public class KuduMultiUpsertTest {


    public static void main(String[] args) throws Exception {
        String value = "{\"user_id\": 1, \"first_name\": \"yewc\"}";

        KuduTableInfo.Builder builder = KuduTableInfo.Builder
                .create("users");

        Map<String, KuduColumnInfo> cols = getColumns1();
        for (String col : cols.keySet()) {
            builder.addColumn(cols.get(col));
        }
        KuduTableInfo tableInfo = builder.build();

        KuduSink kuduSink = new KuduSink("10.17.4.11", tableInfo)
                .withEventualConsistency()
                .withUpsertWriteMode()
                .setMutationBufferSpace(1)
                .setColumnMapping("0=0;1=1", 3);

        kuduSink.open(null);
        kuduSink.invoke((KuduRow) getRowLine(value, cols, getColumnIndex1()));

        kuduSink.close();

        // ======================================================

        value = "{\"user_id\": 1, \"last_name\": \"barneyye\"}";

        builder = KuduTableInfo.Builder
                .create("users");

        cols = getColumns2();
        for (String col : cols.keySet()) {
            builder.addColumn(cols.get(col));
        }
        tableInfo = builder.build();

        kuduSink = new KuduSink("10.17.4.11", tableInfo)
                .withEventualConsistency()
                .withUpsertWriteMode()
                .setMutationBufferSpace(1)
                .setColumnMapping("0=0;1=2", 3);

        kuduSink.open(null);
        kuduSink.invoke((KuduRow) getRowLine(value, cols, getColumnIndex2()));

        kuduSink.close();
    }

    public static Row getRowLine(String value, Map<String, KuduColumnInfo> cols, Map<Integer, String> index) throws Exception {
        if (StringUtils.isBlank(value)) {
            return null;
        }

        try {
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

    public static Map<Integer, String> getColumnIndex1() {
        Map<Integer, String> cols = new LinkedHashMap<>();
        cols.put(0, "user_id");
        cols.put(1, "first_name");

        return cols;
    }

    public static Map<Integer, String> getColumnIndex2() {
        Map<Integer, String> cols = new LinkedHashMap<>();
        cols.put(0, "user_id");
        cols.put(1, "last_name");

        return cols;
    }

    public static Map<String, KuduColumnInfo> getColumns1() {
        Map<String, KuduColumnInfo> cols = new LinkedHashMap<>();

        cols.put("user_id", KuduColumnInfo.Builder.create("user_id", Type.INT16).key(true).nullable(false).build());
        cols.put("first_name", KuduColumnInfo.Builder.create("first_name", Type.STRING).nullable(true).build());

        return cols;
    }

    public static Map<String, KuduColumnInfo> getColumns2() {
        Map<String, KuduColumnInfo> cols = new LinkedHashMap<>();

        cols.put("user_id", KuduColumnInfo.Builder.create("user_id", Type.INT16).key(true).nullable(false).build());
        cols.put("last_name", KuduColumnInfo.Builder.create("last_name", Type.STRING).nullable(true).build());

        return cols;
    }
}
