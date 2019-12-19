package org.yewc.test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kudu.KuduUpsertTableSink;
import org.apache.flink.streaming.connectors.kudu.connector.KuduColumnInfo;
import org.apache.flink.streaming.connectors.kudu.connector.KuduConnector;
import org.apache.flink.streaming.connectors.kudu.connector.KuduEntity;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yewc.test.schema.FxJSONDeserializationSchema;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class KuduSqlTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KuduSqlTest.class);

//	public static final String BROKERS = "10.1.170.14:9092,10.1.170.163:9092,10.1.170.165:9092";
//	public static final String BROKERS = "10.1.175.202:9092,10.1.175.205:9092,10.1.175.216:9092,10.1.175.210:9092,10.1.175.213:9092";
//	public static final String TOPIC = "kg.postevent.new";
//	public static final String KUDU_MASTER = "10.1.174.232,10.1.174.241,10.1.174.242";
//	public static final String TABLE = "kg_postevent_new";
//	public static final String HEAD_SPLIT = "\t";
//	public static final String DATE_FORMAT = "yyyy-MM-dd HH";

	public static final String BROKERS = "10.16.6.191:9092";
	public static final String TOPIC = "test2";
	public static final String KUDU_MASTER = "10.17.4.11";
	public static final String TABLE = "kg_postevent_new";
	public static final String HEAD_SPLIT = " ";
	public static final String DATE_FORMAT = "yyyy-MM-dd HH";

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(10000);

		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

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
		properties.setProperty("group.id", "kudu-org.yewc.test");

		String[] fieldNames = toFieldNames(cols);
		TypeInformation[] fieldTypes = toFieldTypes(cols);

		FlinkKafkaConsumer010<Row> myConsumer =
				new FlinkKafkaConsumer010<>(TOPIC,
						new FxJSONDeserializationSchema( true, "logweb", HEAD_SPLIT, DATE_FORMAT, cols),
						properties);
		myConsumer.setStartFromEarliest();
		DataStream<Row> stream = env
				.addSource(myConsumer).returns(Types.ROW(fieldTypes));

		tableEnv.registerDataStream("my_kudu_source", stream, "dt,system_default_id,productid,deviceid,imei,imsi,time,appkey,channelid,platformid,version,osversion,eventname,eventidentifier,statistic,server_time,mid,uuid,deviceid2,kugouid,fanxid,p1,p2,actorid,roomid,isfollower,livetype,plugin,p3");

		TableSchema.Builder schameBuilder = TableSchema.builder();
		for (int i = 0; i < fieldNames.length; i++) {
			schameBuilder.field(fieldNames[i], fieldTypes[i]);
		}

		KuduEntity entity = new KuduEntity();
		entity.setKuduMasters(KUDU_MASTER);
		entity.setSchema(schameBuilder.build());
		entity.setTableInfo(tableInfo);
		entity.setFlushInterval(1000);
		entity.setMutationBufferSpace(1000);
		entity.setWriteMode(KuduConnector.WriteMode.UPSERT);

		KuduUpsertTableSink sink = new KuduUpsertTableSink(entity);

		tableEnv.registerTableSink("my_kudu_sink", fieldNames, fieldTypes, sink);

		tableEnv.sqlUpdate("insert into my_kudu_sink select * from my_kudu_source");

		env.execute(TABLE);
	}

	public static Map<String, KuduColumnInfo> getColumns() {
		Map<String, KuduColumnInfo> cols = new LinkedHashMap<>();

		cols.put("dt", KuduColumnInfo.Builder.create("dt", Type.STRING).key(true) .nullable(false).build());
		cols.put("system_default_id", KuduColumnInfo.Builder.create("system_defult_id", Type.STRING).key(true) .nullable(false).build());
//		cols.put("sfid", KuduColumnInfo.Builder.create("sfid", Type.STRING) .nullable(true).build());
//		cols.put("msg", KuduColumnInfo.Builder.create("msg", Type.STRING) .nullable(true).build());
//		cols.put("time", KuduColumnInfo.Builder.create("time", Type.STRING) .nullable(true).build());
//		cols.put("cmd", KuduColumnInfo.Builder.create("cmd", Type.STRING) .nullable(true).build());
//		cols.put("pid", KuduColumnInfo.Builder.create("pid", Type.STRING) .nullable(true).build());
//		cols.put("rid", KuduColumnInfo.Builder.create("rid", Type.STRING) .nullable(true).build());
//		cols.put("skid", KuduColumnInfo.Builder.create("skid", Type.STRING) .nullable(true).build());
//		cols.put("sn", KuduColumnInfo.Builder.create("sn", Type.STRING) .nullable(true).build());
//		cols.put("srl", KuduColumnInfo.Builder.create("srl", Type.STRING) .nullable(true).build());
//		cols.put("rfid", KuduColumnInfo.Builder.create("rfid", Type.STRING) .nullable(true).build());
//		cols.put("rkid", KuduColumnInfo.Builder.create("rkid", Type.STRING) .nullable(true).build());
//		cols.put("rn", KuduColumnInfo.Builder.create("rn", Type.STRING) .nullable(true).build());
//		cols.put("rrl", KuduColumnInfo.Builder.create("rrl", Type.STRING) .nullable(true).build());
//		cols.put("isse", KuduColumnInfo.Builder.create("isse", Type.STRING) .nullable(true).build());
//		cols.put("ip", KuduColumnInfo.Builder.create("ip", Type.STRING) .nullable(true).build());
//		cols.put("ver", KuduColumnInfo.Builder.create("ver", Type.STRING) .nullable(true).build());
//		cols.put("issu", KuduColumnInfo.Builder.create("issu", Type.STRING) .nullable(true).build());
//		cols.put("p1", KuduColumnInfo.Builder.create("p1", Type.STRING) .nullable(true).build());
//		cols.put("p2", KuduColumnInfo.Builder.create("p2", Type.STRING) .nullable(true).build());

		cols.put("productid", KuduColumnInfo.Builder.create("productid", Type.STRING) .nullable(true).build());
		cols.put("deviceid", KuduColumnInfo.Builder.create("deviceid", Type.STRING) .nullable(true).build());
		cols.put("imei", KuduColumnInfo.Builder.create("imei", Type.STRING) .nullable(true).build());
		cols.put("imsi", KuduColumnInfo.Builder.create("imsi", Type.STRING) .nullable(true).build());
		cols.put("time", KuduColumnInfo.Builder.create("time", Type.STRING) .nullable(true).build());
		cols.put("appkey", KuduColumnInfo.Builder.create("appkey", Type.STRING) .nullable(true).build());
		cols.put("channelid", KuduColumnInfo.Builder.create("channelid", Type.STRING) .nullable(true).build());
		cols.put("platformid", KuduColumnInfo.Builder.create("platformid", Type.STRING) .nullable(true).build());
		cols.put("version", KuduColumnInfo.Builder.create("version", Type.STRING) .nullable(true).build());
		cols.put("osversion", KuduColumnInfo.Builder.create("osversion", Type.STRING) .nullable(true).build());
		cols.put("eventname", KuduColumnInfo.Builder.create("eventname", Type.STRING) .nullable(true).build());
		cols.put("eventidentifier", KuduColumnInfo.Builder.create("eventidentifier", Type.STRING) .nullable(true).build());
		cols.put("statistic", KuduColumnInfo.Builder.create("statistic", Type.STRING) .nullable(true).build());
		cols.put("server_time", KuduColumnInfo.Builder.create("server_time", Type.STRING) .nullable(true).build());
		cols.put("mid", KuduColumnInfo.Builder.create("mid", Type.STRING) .nullable(true).build());
		cols.put("uuid", KuduColumnInfo.Builder.create("uuid", Type.STRING) .nullable(true).build());
		cols.put("deviceid2", KuduColumnInfo.Builder.create("deviceid2", Type.STRING) .nullable(true).build());
		cols.put("kugouid", KuduColumnInfo.Builder.create("kugouid", Type.STRING) .nullable(true).build());
		cols.put("fanxid", KuduColumnInfo.Builder.create("fanxid", Type.STRING) .nullable(true).build());
		cols.put("p1", KuduColumnInfo.Builder.create("p1", Type.STRING) .nullable(true).build());
		cols.put("p2", KuduColumnInfo.Builder.create("p2", Type.STRING) .nullable(true).build());
		cols.put("actorid", KuduColumnInfo.Builder.create("actorid", Type.STRING) .nullable(true).build());
		cols.put("roomid", KuduColumnInfo.Builder.create("roomid", Type.STRING) .nullable(true).build());
		cols.put("isfollower", KuduColumnInfo.Builder.create("isfollower", Type.STRING) .nullable(true).build());
		cols.put("livetype", KuduColumnInfo.Builder.create("livetype", Type.STRING) .nullable(true).build());
		cols.put("plugin", KuduColumnInfo.Builder.create("plugin", Type.STRING) .nullable(true).build());
		cols.put("p3", KuduColumnInfo.Builder.create("p3", Type.STRING) .nullable(true).build());
		return cols;
	}

	public static String[] toFieldNames(Map<String, KuduColumnInfo> cols) {
		return cols.keySet().toArray(new String[0]);
	}

	public static TypeInformation[] toFieldTypes(Map<String, KuduColumnInfo> cols) {
		TypeInformation[] typeInformations = new TypeInformation[cols.size()];
		String[] fieldNames = toFieldNames(cols);
		for (int i = 0; i < fieldNames.length; i++) {
			TypeInformation ti = null;
			Type type = cols.get(fieldNames[i]).getType();
			switch (type) {
				case STRING:
					ti = Types.STRING;
					break;
				case FLOAT:
					ti = Types.FLOAT;
					break;
				case INT8:
					ti = Types.BYTE;
					break;
				case INT16:
					ti = Types.SHORT;
					break;
				case INT32:
					ti = Types.INT;
					break;
				case INT64:
					ti = Types.LONG;
					break;
				case DOUBLE:
					ti = Types.DOUBLE;
					break;
				case BOOL:
					ti = Types.BOOLEAN;
					break;
				case UNIXTIME_MICROS:
					ti = Types.LONG;
					break;
				case BINARY:
					ti = Types.SHORT;
					break;
				default:
					throw new IllegalArgumentException("Illegal var type: " + type);
			}

			typeInformations[i] = ti;
		}

		return typeInformations;
	}
}
