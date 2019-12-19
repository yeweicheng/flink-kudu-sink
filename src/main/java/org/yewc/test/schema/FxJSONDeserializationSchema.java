package org.yewc.test.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kudu.connector.KuduColumnInfo;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

public class FxJSONDeserializationSchema extends Schema implements KeyedDeserializationSchema<Row> {

    public final SimpleDateFormat sdf;

    private boolean includeMetadata;
    private String type;
    private String split;
    private Map<String, KuduColumnInfo> cols;

    public FxJSONDeserializationSchema(boolean includeMetadata, String type,
                                       String split, String dataFormat, Map<String, KuduColumnInfo> cols) {
        this.includeMetadata = includeMetadata;
        this.type = type;
        this.split = split;
        this.sdf = new SimpleDateFormat(dataFormat);
        this.cols = cols;
    }

    @Override
    public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        JSONObject node = new JSONObject();
        if (messageKey != null) {
            node.put("key", JSON.parse(new String(messageKey)));
        }
        if (message != null) {
            switch (this.type) {
                case "bzid":
                    node.put("value", formatBZID(new String(message)));
                    break;
                case "logweb":
                    node.put("value", formatLOGWEB(new String(message)));
                    break;
            }
        }
        if (includeMetadata) {
            JSONObject meta = new JSONObject();
            meta.put("offset", offset);
            meta.put("topic", topic);
            meta.put("partition", partition);
            node.put("metadata", meta);

            node.getJSONObject("value").put("system_default_id", partition + "-" + offset);
        }
        return map(node);
    }

    public JSONObject formatBZID(String value) {
        String[] data = value.split("\\{", 2);
        JSONObject jo = JSONObject.parseObject("{" + data[1]);
        String dt = sdf.format(new Date(Long.parseLong(data[0].split(split)[0])*1000));
        jo.put("dt", dt);
        return jo;
    }

    public JSONObject formatLOGWEB(String value) {
        //有的数据中包含特殊字符,需要替换掉
        value = value.replaceAll("[\\x00-\\x09\\x10\\x11\\x12\\x14-\\x1F\\x7F]", "");
        String timestamp = value.substring(0, 16);
        JSONObject jo = JSON.parseObject(value.substring(16));
        long lst = Long.parseLong(timestamp, 16) / 1000000;
        jo.put("dt", sdf.format(new Date(lst)));
        return jo;
    }

    public Row map(JSONObject data) throws IOException {
        if (data == null || data.isEmpty()) {
            return null;
        }

        try {

            JSONObject jo = data.getJSONObject("value");
            final Set<String> keys = cols.keySet();
            KuduRow row = new KuduRow(keys.size());

            int i = 0;
            Object colValue;
            for (Iterator<String> iter = keys.iterator(); iter.hasNext();) {
                String key = iter.next();

                if (jo.containsKey(key)) {
                    colValue = jo.get(key);
                } else {
                    colValue = null;
                }

                row.setField(i, colValue);
                i++;
            }

            return row;
        } catch (Exception e) {
            IOException customException = new IOException("error msg: " + data.getJSONObject("value").toString());
            customException.addSuppressed(e);
            throw customException;
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return getForClass(Row.class);
    }
}
