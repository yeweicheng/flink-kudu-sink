package org.yewc.test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.connectors.kudu.KuduOutputFormat;
import org.apache.flink.streaming.connectors.kudu.connector.KuduColumnInfo;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.kudu.Type;

import java.util.ArrayList;
import java.util.List;

public class KuduOutputTest {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

// create a table info object
		KuduTableInfo tableInfo = KuduTableInfo.Builder
			.create("users")
			.createIfNotExist(true)
			.replicas(1)
			.addColumn(KuduColumnInfo.Builder.create("user_id", Type.INT32).key(true).hashKey(true).build())
			.addColumn(KuduColumnInfo.Builder.create("first_name", Type.STRING).build())
			.addColumn(KuduColumnInfo.Builder.create("last_name", Type.STRING).build())
			.build();

		List<KuduRow> users = new ArrayList<>();
		KuduRow row = new KuduRow(3);
		row.setField(0, "user_id", 3);
		row.setField(1, "first_name", "ye");
		row.setField(2, "last_name", "weicheng");
		users.add(row);

		row = new KuduRow(3);
		row.setField(0, "user_id", 3);
		row.setField(1, "first_name", "ha");
		row.setField(2, "last_name", "haha");
		users.add(row);

		DataSource<KuduRow> ds = env.fromCollection(users);
		ds.output(new KuduOutputFormat<KuduRow>("10.17.4.11", tableInfo));

		env.execute();
	}
}
