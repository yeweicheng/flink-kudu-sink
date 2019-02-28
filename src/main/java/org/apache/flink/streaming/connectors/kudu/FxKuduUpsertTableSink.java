package org.apache.flink.streaming.connectors.kudu;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kudu.connector.KuduConnector;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

public class FxKuduUpsertTableSink implements UpsertStreamTableSink<Row> {

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private KuduConnector.Consistency consistency;
    private KuduConnector.WriteMode writeMode;
    private String[] fieldNames;
    private TypeInformation[] fieldTypes;
    private TableSchema schema;

    public FxKuduUpsertTableSink(TableSchema schema, String kuduMasters, KuduTableInfo tableInfo,
                                 KuduConnector.Consistency consistency, KuduConnector.WriteMode writeMode) {
        Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");
        this.kuduMasters = kuduMasters;
        this.schema = schema;

        Preconditions.checkNotNull(tableInfo, "tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = consistency;
        this.writeMode = writeMode;
    }

    @Override
    public void setKeyFields(String[] keys) {

    }

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {

    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return this.schema.toRowType();
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> ds) {
        DataStream<Row> dataStream = ds.map((MapFunction<Tuple2<Boolean, Row>, Row>) value -> value.f1)
                .returns(new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames()));
        KuduSink.addSink(dataStream).init(this.kuduMasters, this.tableInfo, this.consistency, this.writeMode);
    }

    @Override
    public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
        return Types.TUPLE(Types.BOOLEAN, this.getRecordType());
    }

    @Override
    public String[] getFieldNames() {
        return this.schema.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return this.schema.getFieldTypes();
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types does not match.");

        if (this.schema == null) {
            TableSchema.Builder builder = TableSchema.builder();
            for (int i = 0; i < fieldNames.length; i++) {
                builder.field(fieldNames[i], fieldTypes[i]);
            }
            this.schema = builder.build();
        }

        FxKuduUpsertTableSink kuduSink = new FxKuduUpsertTableSink(this.schema, this.kuduMasters, this.tableInfo,
                this.consistency, this.writeMode);
        kuduSink.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names must not be null.");
        kuduSink.fieldTypes = Preconditions.checkNotNull(fieldTypes, "Field types must not be null.");

        return kuduSink;
    }
}
