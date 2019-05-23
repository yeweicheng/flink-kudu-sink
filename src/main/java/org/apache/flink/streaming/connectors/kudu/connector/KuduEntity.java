package org.apache.flink.streaming.connectors.kudu.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

public class KuduEntity {

    private String kuduMasters;
    private KuduConnector.Consistency consistency = KuduConnector.Consistency.EVENTUAL;
    private KuduConnector.WriteMode writeMode = KuduConnector.WriteMode.INSERT;
    private KuduTableInfo tableInfo;
    private TableSchema schema;
    private int flushInterval = 1000;
    private int mutationBufferSpace = 1000;
    private String columnMappingIndex;
    private Integer columnSize;


    public String getKuduMasters() {
        return kuduMasters;
    }

    public void setKuduMasters(String kuduMasters) {
        this.kuduMasters = kuduMasters;
    }

    public KuduConnector.Consistency getConsistency() {
        return consistency;
    }

    public void setConsistency(KuduConnector.Consistency consistency) {
        this.consistency = consistency;
    }

    public KuduConnector.WriteMode getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(KuduConnector.WriteMode writeMode) {
        this.writeMode = writeMode;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public int getMutationBufferSpace() {
        return mutationBufferSpace;
    }

    public void setMutationBufferSpace(int mutationBufferSpace) {
        this.mutationBufferSpace = mutationBufferSpace;
    }

    public KuduTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(KuduTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public String getColumnMappingIndex() {
        return columnMappingIndex;
    }

    public void setColumnMappingIndex(String columnMappingIndex) {
        this.columnMappingIndex = columnMappingIndex;
    }

    public Integer getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(Integer columnSize) {
        this.columnSize = columnSize;
    }
}
