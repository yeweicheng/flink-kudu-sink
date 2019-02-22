package org.apache.flink.yewc;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kudu.KuduSink;
import org.apache.flink.streaming.connectors.kudu.connector.KuduColumnInfo;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.types.Row;
import org.apache.kudu.Type;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class KuduSinkTest {

	private static final Logger LOGGER = LoggerFactory.getLogger("KuduSinkTest");

	public static final String BROKERS = "10.1.170.14:9092,10.1.170.163:9092,10.1.170.165:9092";
	public static final String TOPIC = "fx.user.chat.ext";
	public static final String KUDU_MASTER = "10.1.174.232,10.1.174.241,10.1.174.242";
	public static final String TABLE = "fx_user_chat_ext";
	public static final String HEAD_SPLIT = "\t";

//	public static final String BROKERS = "10.16.6.191:9092";
//	public static final String TOPIC = "test2";
//	public static final String KUDU_MASTER = "10.17.4.11";
//	public static final String TABLE = "fx_user_chat_ext";
//	public static final String HEAD_SPLIT = " ";

	public static void main(String[] args) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KuduTableInfo.Builder builder = KuduTableInfo.Builder
			.create(TABLE)
			.createIfNotExist(false)
			.replicas(1);

		final Map<String, Type> cols = getColumns();
		boolean keyCol = false;
		for (String col : cols.keySet()) {
			if (!keyCol) {
				builder.addColumn(KuduColumnInfo.Builder.create(col, cols.get(col)).key(true).hashKey(true).build());
				keyCol = true;
			} else {
				builder.addColumn(KuduColumnInfo.Builder.create(col, cols.get(col)).build());
			}
		}

		KuduTableInfo tableInfo = builder.build();

		final ObjectMapper objectMapper = new ObjectMapper();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", BROKERS);
		properties.setProperty("group.id", "kudu-test");
		FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(TOPIC, new SimpleStringSchema(), properties);
		myConsumer.setStartFromGroupOffsets();

		DataStream<String> stream = env
			.addSource(myConsumer);
		stream.map(new MapFunction<String, Row>() {
			@Override
			public Row map(String value) throws Exception {
				if (StringUtils.isBlank(value) || !value.contains("{")) {
					return null;
				}

				String data[] = value.split("\\{", 2);
				String dt = sdf.format(new Date(Long.parseLong(data[0].split(HEAD_SPLIT)[0])*1000));

				try {
					JSONObject jo = new JSONObject("{" + data[1]);
					final Set<String> keys = cols.keySet();
					KuduRow row = new KuduRow(keys.size() + 1);
					row.setField(0, dt);

					int i = 1;
					Object colValue;
					for (Iterator<String> iter = keys.iterator();iter.hasNext();) {
						String key = iter.next();
						if (jo.has(key)) {
							colValue = jo.get(key);
							if (colValue instanceof String) {
								colValue = colValue.toString().replaceAll("(\r\n|\r|\n|\n\r)", "<br>");
							}
						} else {
							colValue = "";
						}

						row.setField(i, colValue);
						i++;
					}

					return row;
				} catch (Exception e) {
					Exception customException = new RuntimeException("error msg: {" + data[1]);
					customException.addSuppressed(e);
					throw customException;
				}
			}
		}).addSink(new KuduSink<>(KUDU_MASTER, tableInfo));

		env.execute(TABLE);
	}

	public static Map<String, Type> getColumns() {
		Map<String, Type> cols = new LinkedHashMap<>();
//		cols.put("sessionId", Type.STRING);
//		cols.put("appChannel", Type.INT32);
//		cols.put("clientId", Type.INT32);
//		cols.put("deviceNo", Type.STRING);
//		cols.put("imei", Type.STRING);
//		cols.put("ip", Type.STRING);
//		cols.put("kugouId", Type.INT32);
//		cols.put("platId", Type.INT32);
//		cols.put("roomId", Type.INT32);
//		cols.put("source", Type.INT32);
//		cols.put("sid", Type.STRING);
//		cols.put("systemVersion", Type.STRING);
//		cols.put("timestamp", Type.INT64);
//		cols.put("uuid", Type.STRING);
//		cols.put("version", Type.STRING);

		cols.put("sfid", Type.STRING);
		cols.put("msg", Type.STRING);
		cols.put("time", Type.STRING);
		cols.put("cmd", Type.STRING);
		cols.put("pid", Type.STRING);
		cols.put("rid", Type.STRING);
		cols.put("skid", Type.STRING);
		cols.put("sn", Type.STRING);
		cols.put("srl", Type.STRING);
		cols.put("rfid", Type.STRING);
		cols.put("rkid", Type.STRING);
		cols.put("rn", Type.STRING);
		cols.put("rrl", Type.STRING);
		cols.put("isse", Type.STRING);
		cols.put("ip", Type.STRING);
		cols.put("ver", Type.STRING);
		cols.put("issu", Type.STRING);
		cols.put("p1", Type.STRING);
		cols.put("p2", Type.STRING);
		return cols;
	}

	public static Object getColValue(Type type, JsonNode data) {
		Object result = null;
		switch (type) {
			case STRING:
				result = data.textValue();
				break;
			case INT64:
				result = data.longValue();
				break;
			case INT32:
				result = data.intValue();
				break;
			case INT16:
				result = data.intValue();
				break;
			case INT8:
				result = data.intValue();
				break;
			default:
				result = data.textValue();
				break;
		}
		return result;
	}
}
