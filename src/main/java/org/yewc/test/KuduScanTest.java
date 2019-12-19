// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.yewc.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;

/*
 * A simple example of using the synchronous Kudu Java client to
 * - Create a table.
 * - Insert rows.
 * - Alter a table.
 * - Scan rows.
 * - Delete a table.
 */
public class KuduScanTest {
    private static final Double DEFAULT_DOUBLE = 12.345;
    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "10.17.4.11:7051");

    static void createExampleTable(KuduClient client, String tableName)  throws KuduException {
        // Set up a simple schema.
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        // Set up the partition schema, which distributes rows to different tablets by hash.
        // Kudu also supports partitioning by key range. Hash and range partitioning can be combined.
        // For more information, see http://kudu.apache.org/docs/schema_design.html.
        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("key");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable(tableName, schema, cto);
        System.out.println("Created table " + tableName);
    }

    static void insertRows(KuduClient client, String tableName, int numRows) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        for (int i = 0; i < numRows; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("key", i);
            // Make even-keyed row have a null 'value'.
            if (i % 2 == 0) {
                row.setNull("value");
            } else {
                row.addString("value", "value " + i);
            }
            session.apply(insert);
        }

        // Call session.close() to end the session and ensure the rows are
        // flushed and errors are returned.
        // You can also call session.flush() to do the same without ending the session.
        // When flushing in AUTO_FLUSH_BACKGROUND mode (the default mode recommended
        // for most workloads, you must check the pending errors as shown below, since
        // write operations are flushed to Kudu in background threads.
        session.close();
        if (session.countPendingErrors() != 0) {
            System.out.println("errors inserting rows");
            org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
            org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
            int numErrs = Math.min(errs.length, 5);
            System.out.println("there were errors inserting rows to Kudu");
            System.out.println("the first few errors follow:");
            for (int i = 0; i < numErrs; i++) {
                System.out.println(errs[i]);
            }
            if (roStatus.isOverflowed()) {
                System.out.println("error buffer overflowed: some errors were discarded");
            }
            throw new RuntimeException("error inserting rows to Kudu");
        }
        System.out.println("Inserted " + numRows + " rows");
    }

    static void scanTableAndCheckResults(KuduClient client, String tableName, int numRows) throws KuduException {
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();

        // Scan with a predicate on the 'key' column, returning the 'value' and "added" columns.
        List<String> projectColumns = new ArrayList<>(2);
        projectColumns.add("key");
        projectColumns.add("value");
        projectColumns.add("added");
        int lowerBound = 0;
        KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("key"),
                ComparisonOp.GREATER_EQUAL,
                lowerBound);
        int upperBound = numRows / 2;
        KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("key"),
                ComparisonOp.LESS,
                upperBound);

        KuduPredicate equalPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("key"),
                ComparisonOp.EQUAL,
                1);

        KuduPredicate listPred = KuduPredicate.newInListPredicate(
                schema.getColumn("key"),
                Arrays.asList(1, 2));

        KuduScanner scanner = client.newScannerBuilder(table)
                .setProjectedColumnNames(projectColumns)
                .addPredicate(listPred)
                .build();

        // Check the correct number of values and null values are returned, and
        // that the default value was set for the new column on each row.
        // Note: scanning a hash-partitioned table will not return results in primary key order.
        int resultCount = 0;
        int nullCount = 0;
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if (result.isNull("value")) {
                    nullCount++;
                }
                System.out.print(result.getInt(0) + ", ");
                System.out.print(result.getString(1) + ", ");
                System.out.println(result.getDouble(2));
                resultCount++;
            }
        }
        System.out.println("Scanned some rows and checked the results");
    }

    static void scanTableSnapshot(KuduClient client, String tableName, int key) throws KuduException {
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();

        // Scan with a predicate on the 'key' column, returning the 'value' and "added" columns.
        List<String> projectColumns = new ArrayList<>(2);
        projectColumns.add("key");
        projectColumns.add("value");
        projectColumns.add("added");
        KuduPredicate pred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("key"),
                ComparisonOp.EQUAL,
                key);
        KuduScanner scanner = client.newScannerBuilder(table)
                .setProjectedColumnNames(projectColumns)
                .addPredicate(pred)
                .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)
                .snapshotTimestampMicros(1553351998521325l)
                .build();

        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                System.out.println(result.getInt("key")
                        + "," + result.getString("value")
                        + "," + result.getDouble("added"));
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master(s) at " + KUDU_MASTERS);
        System.out.println("Run with -DkuduMasters=master-0:port,master-1:port,... to override.");
        System.out.println("-----------------------------------------------");
        String tableName = "my_test";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
//            createExampleTable(client, tableName);
//
//            int numRows = 150;
//            insertRows(client, tableName, numRows);
//
//            // Alter the table, adding a column with a default value.
//            // Note: after altering the table, the table needs to be re-opened.
//            AlterTableOptions ato = new AlterTableOptions();
//            ato.addColumn("added", org.apache.kudu.Type.DOUBLE, DEFAULT_DOUBLE);
//            client.alterTable(tableName, ato);
//            System.out.println("Altered the table");
//
            scanTableAndCheckResults(client, tableName, 3);

//            scanTableSnapshot(client, tableName, 1);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
//                client.deleteTable(tableName);
//                System.out.println("Deleted the table");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}