package org.yewc.test;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KuduSinkTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KuduSinkTest.class);

//	public static final String BROKERS = "10.1.170.14:9092,10.1.170.163:9092,10.1.170.165:9092";
//	public static final String TOPIC = "fx.user.chat.ext";
//	public static final String KUDU_MASTER = "10.1.174.232,10.1.174.241,10.1.174.242";
//	public static final String TABLE = "fx_user_chat_ext";
//	public static final String HEAD_SPLIT = "\t";

	public static final String BROKERS = "10.16.6.191:9092";
	public static final String TOPIC = "test2";
	public static final String KUDU_MASTER = "10.17.4.11";
	public static final String TABLE = "fx_user_chat_ext";
	public static final String HEAD_SPLIT = " ";

	public static void main(String[] args) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KuduTableInfo.Builder builder = KuduTableInfo.Builder
				.create(TABLE)
				.createIfNotExist(false)
//				.errorBreak(false)
				.replicas(1);

		final Map<String, KuduColumnInfo> cols = getColumns();
		for (String col : cols.keySet()) {
			builder.addColumn(cols.get(col));
		}

		final KuduTableInfo tableInfo = builder.build();

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

				String[] data = null;
				try {
					data = value.split("\\{", 2);
					JSONObject jo = new JSONObject("{" + data[1]);
					String dt = sdf.format(new Date(Long.parseLong(data[0].split(HEAD_SPLIT)[0])*1000));
					jo.put("dt", dt);

					final Set<String> keys = cols.keySet();
					KuduRow row = new KuduRow(keys.size());

					int i = 0;
					Object colValue;
					for (Iterator<String> iter = keys.iterator();iter.hasNext();) {
						String key = iter.next();

						if (jo.has(key)) {
							colValue = jo.get(key);
							if (colValue instanceof String) {
								colValue = colValue.toString().replaceAll("(\r\n|\r|\n|\n\r)", "<br>");
							}
						} else {
							colValue = null;
						}

						row.setField(i, colValue);
						i++;
					}

					return row;
				} catch (Exception e) {
					Exception customException = new RuntimeException("error msg: {" + data[1]);
					if (tableInfo.isErrorBreak()) {
						customException.addSuppressed(e);
						throw customException;
					} else {
						LOGGER.warn(customException.getMessage());
					}
				}
				return null;
			}
		}).addSink(new KuduSink<>(KUDU_MASTER, tableInfo));

		env.execute(TABLE);
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
