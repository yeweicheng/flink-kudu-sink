/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kudu;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kudu.connector.KuduConnector;
import org.apache.flink.streaming.connectors.kudu.connector.KuduRow;
import org.apache.flink.streaming.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduSink<OUT> extends RichSinkFunction<OUT> implements CheckpointedFunction, CheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private KuduConnector.Consistency consistency;
    private KuduConnector.WriteMode writeMode;
    private SessionConfiguration.FlushMode flushMode = SessionConfiguration.FlushMode.MANUAL_FLUSH;
    private int flushInterval = 1000;
    private int mutationBufferSpace = 1000;
    private long timeout = 60000;
    private Map<Integer, Integer> columnMapping;
    private Integer columnSize;

    private transient KuduConnector tableContext;

    private ListState<Row> rowListState;
    private List<Row> bufferRows = new ArrayList<>();
    private int counter = 0;

    public KuduSink() {}

    public KuduSink(String kuduMasters, KuduTableInfo tableInfo) {
        init(kuduMasters, tableInfo);
    }

    public KuduSink<OUT> init(String kuduMasters, KuduTableInfo tableInfo) {
        Preconditions.checkNotNull(kuduMasters,"kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo,"tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = KuduConnector.Consistency.STRONG;
        this.writeMode = KuduConnector.WriteMode.UPSERT;

        return this;
    }

    public KuduSink<OUT> init(String kuduMasters, KuduTableInfo tableInfo,
                              KuduConnector.Consistency consistency, KuduConnector.WriteMode writeMode) {
        Preconditions.checkNotNull(kuduMasters,"kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo,"tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = consistency;
        this.writeMode = writeMode;

        return this;
    }

    public static KuduSink addSink(DataStream dataStream) {
        KuduSink kuduSink = new KuduSink();
        dataStream.addSink(kuduSink);
        return kuduSink;
    }

    public KuduSink<OUT> withEventualConsistency() {
        this.consistency = KuduConnector.Consistency.EVENTUAL;
        return this;
    }

    public KuduSink<OUT> withStrongConsistency() {
        this.consistency = KuduConnector.Consistency.STRONG;
        return this;
    }

    public KuduSink<OUT> withUpsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        return this;
    }

    public KuduSink<OUT> withInsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.INSERT;
        return this;
    }

    public KuduSink<OUT> withUpdateWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPDATE;
        return this;
    }

    public KuduSink<OUT> setFlushMode(SessionConfiguration.FlushMode flushMode) {
        this.flushMode = flushMode;
        return this;
    }

    public KuduSink<OUT> setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    public KuduSink<OUT> setMutationBufferSpace(int mutationBufferSpace) {
        this.mutationBufferSpace = mutationBufferSpace;
        return this;
    }

    public KuduSink<OUT> setColumnMapping(String columnMappingIndex, Integer columnSize) {
        this.columnSize = columnSize;
        if (StringUtils.isNotBlank(columnMappingIndex) && columnSize > 0) {
            String[] indexs = columnMappingIndex.split(";");
            this.columnMapping = new HashMap<>(indexs.length);
            String[] temp;
            for (int i = 0; i < indexs.length; i++) {
                temp = indexs[i].split("=");
                columnMapping.put(Integer.valueOf(temp[1]), Integer.valueOf(temp[0]));
            }
        }
        return this;
    }

    public KuduSink<OUT> setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        startTableContext();

        // flush buffer
        LOG.info("reload state size: " + bufferRows.size());
        try {
            for (int i = 0; i < bufferRows.size(); i++) {
                invoke((OUT) bufferRows.get(i));
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        bufferRows.clear();
    }

    private void startTableContext() throws IOException {
        if (tableContext != null) return;
        tableContext = new KuduConnector(kuduMasters, tableInfo, timeout)
                .setFlushMode(flushMode)
                .setMutationBufferSpace(mutationBufferSpace)
                .setFlushInterval(flushInterval);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        synchronized (rowListState) {
            LOG.info("snapshot state size: " + counter);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(60))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        ListStateDescriptor<Row> sd = new ListStateDescriptor<Row>("buffer-row", Row.class);
//        sd.enableTimeToLive(ttlConfig);

        rowListState = context.getOperatorStateStore().getListState(sd);

        if (context.isRestored()) {
            for (Row row : rowListState.get()) {
                bufferRows.add(row);
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("current checkpoint id: " + checkpointId);
    }

    @Override
    public void invoke(OUT row, Context context) throws Exception {
        try {
            if (row == null) {
                return;
            }

			KuduRow kuduRow = null;
            if (columnMapping == null) {
                int length = ((Row) row).getArity();
                kuduRow = new KuduRow(length);
                for (int i = 0; i < length; i++) {
                    kuduRow.setField(i, ((Row) row).getField(i));
                }
            } else {
                kuduRow = new KuduRow(columnSize);
                for (int i = 0; i < columnSize; i++) {
                    if (columnMapping.containsKey(i)) {
                        kuduRow.setField(i, ((Row) row).getField(columnMapping.get(i)));
                    } else {
                        kuduRow.setField(i, KuduRow.getIgnoreValue());
                    }
                }
            }

            rowListState.add((Row) row);
            counter++;
            tableContext.writeRow(kuduRow, consistency, writeMode);
            if (tableContext.counter == 0) {
                synchronized (rowListState) {
                    rowListState.clear();
                    counter = 0;
                }
            }
        } catch (Exception e) {
            if (tableInfo.isErrorBreak()) {
                throw new IOException(row.toString() + "\n" + e.getLocalizedMessage(), e);
            } else {
                LOG.warn(e.getMessage());
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (this.tableContext == null) return;
        try {
            this.tableContext.close();
            synchronized (rowListState) {
                rowListState.clear();
                counter = 0;
            }
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }
}
