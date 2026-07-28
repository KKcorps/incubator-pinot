/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.upsert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * A single {@link PartialUpsertHandler} is created per table and shared by every partition's upsert metadata manager,
 * and nothing serializes {@code merge()} across partitions. This test drives the shared handler from several threads at
 * once and checks that each row's post-partial-upsert transform only ever sees its own values.
 *
 * <p>Record transformers hold reusable scratch state, most importantly the argument array in each
 * {@code InbuiltFunctionEvaluator} function node. Sharing one chain across threads lets a row be evaluated against
 * another row's column values, which silently writes a wrong value into the derived column.
 */
public class PartialUpsertHandlerConcurrencyTest {
  private static final String PK_COLUMN = "shipmentItemId";
  private static final String COMPARISON_COLUMN = "trackedAt";
  private static final String DERIVED_COLUMN = "isMissed";

  /// Same shape as the transform that exposed the problem in production: two predicates over a column that partial
  /// upsert fills in from the previous row, and an ELSE that returns false.
  private static final String IS_MISSED_TRANSFORM =
      "CASE WHEN (shippedAt < 0 AND eventAt > shippedCutline) THEN true "
          + "WHEN (shippedAt > 0 AND shippedAt > shippedCutline) THEN true ELSE false END";

  private static final long NOW = System.currentTimeMillis();
  private static final long CUTLINE = NOW - TimeUnit.HOURS.toMillis(1);
  /// Shipped after the cutline, so the second predicate holds and the row is missed.
  private static final long SHIPPED_AT = CUTLINE + TimeUnit.MINUTES.toMillis(1);
  /// Before the cutline, so the first predicate never holds on its own.
  private static final long EVENT_AT = CUTLINE - TimeUnit.MINUTES.toMillis(1);

  private static final int NUM_THREADS = 8;
  private static final int ROWS_PER_THREAD = 20_000;
  private static final int SAMPLES_PER_THREAD = 5;

  /**
   * Drives one shared handler from several threads, half merging rows that are missed and half rows that are not.
   */
  @Test
  public void testConcurrentMergeKeepsDerivedColumnPerRow()
      throws Exception {
    Schema schema = createSchema();
    UpsertConfig upsertConfig = createUpsertConfig();
    TableConfig tableConfig = createTableConfig(schema, upsertConfig);
    PartialUpsertHandler handler =
        new PartialUpsertHandler(tableConfig, schema, List.of(COMPARISON_COLUMN), upsertConfig);

    // Baseline: what each kind of row must produce when nothing else is running.
    Object expectedForShipped = mergeOne(handler, schema, true).getValue(DERIVED_COLUMN);
    Object expectedForNotShipped = mergeOne(handler, schema, false).getValue(DERIVED_COLUMN);
    assertNotEquals(expectedForShipped, expectedForNotShipped,
        "The two row kinds must differ, otherwise this test cannot detect cross-talk");

    AtomicInteger corruptedRows = new AtomicInteger();
    List<String> samples = runConcurrently(threadIndex -> {
      boolean shipped = threadIndex % 2 == 0;
      Object expected = shipped ? expectedForShipped : expectedForNotShipped;
      List<String> localSamples = new ArrayList<>();
      for (int i = 0; i < ROWS_PER_THREAD; i++) {
        GenericRow merged = mergeOne(handler, schema, shipped);
        Object actual = merged.getValue(DERIVED_COLUMN);
        if (!expected.equals(actual)) {
          corruptedRows.incrementAndGet();
          if (localSamples.size() < SAMPLES_PER_THREAD) {
            localSamples.add(String.format("thread %d row %d: shippedAt=%s eventAt=%s shippedCutline=%s "
                    + "-> %s=%s, expected %s", threadIndex, i, merged.getValue("shippedAt"),
                merged.getValue("eventAt"), merged.getValue("shippedCutline"), DERIVED_COLUMN, actual, expected));
          }
        }
      }
      return localSamples;
    });

    if (corruptedRows.get() > 0) {
      fail(corruptedRows.get() + " of " + (NUM_THREADS * ROWS_PER_THREAD) + " rows got a " + DERIVED_COLUMN
          + " value that does not match their own data. Up to " + SAMPLES_PER_THREAD + " samples per thread:\n"
          + String.join("\n", samples));
    }
  }

  /**
   * Runs the body on {@link #NUM_THREADS} threads that all start together, and returns everything they reported.
   */
  private List<String> runConcurrently(ThreadBody body)
      throws Exception {
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(NUM_THREADS);
    List<String> failures = new CopyOnWriteArrayList<>();
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    AtomicInteger threadIds = new AtomicInteger();
    List<Thread> threads = new ArrayList<>(NUM_THREADS);

    for (int i = 0; i < NUM_THREADS; i++) {
      Thread thread = new Thread(() -> {
        int threadIndex = threadIds.getAndIncrement();
        try {
          start.await();
          failures.addAll(body.run(threadIndex));
        } catch (Throwable t) {
          errors.add(t);
        } finally {
          done.countDown();
        }
      }, "merge-" + i);
      thread.setDaemon(true);
      threads.add(thread);
      thread.start();
    }

    start.countDown();
    assertTrue(done.await(2, TimeUnit.MINUTES), "Threads did not finish in time");
    for (Thread thread : threads) {
      thread.join();
    }
    if (!errors.isEmpty()) {
      fail("Merging threw on " + errors.size() + " thread(s), first: " + errors.get(0), errors.get(0));
    }
    return failures;
  }

  /**
   * Merges one new record against a previous row, mirroring what a consumer thread does for a single stream message.
   */
  private GenericRow mergeOne(PartialUpsertHandler handler, Schema schema, boolean shipped) {
    // The record on the stream never carries shippedAt or isMissed. shippedAt arrives through the merge, exactly as
    // it does for the rows this reproduces.
    GenericRow newRow = new GenericRow();
    newRow.putValue(PK_COLUMN, 1000L);
    newRow.putValue(COMPARISON_COLUMN, NOW);
    newRow.putValue("eventAt", EVENT_AT);
    newRow.putValue("shippedCutline", CUTLINE);
    newRow.putDefaultNullValue("shippedAt", schema.getFieldSpecFor("shippedAt").getDefaultNullValue());
    newRow.putDefaultNullValue(DERIVED_COLUMN, schema.getFieldSpecFor(DERIVED_COLUMN).getDefaultNullValue());

    Map<String, Object> previousValues = new HashMap<>();
    previousValues.put(PK_COLUMN, 1000L);
    previousValues.put(COMPARISON_COLUMN, NOW - 1);
    previousValues.put("eventAt", EVENT_AT);
    previousValues.put("shippedCutline", CUTLINE);
    previousValues.put("shippedAt", shipped ? SHIPPED_AT : null);
    previousValues.put(DERIVED_COLUMN, null);

    handler.merge(new FixedPreviousRow(previousValues), newRow, new HashMap<>());
    return newRow;
  }

  private Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName("shipmentItems")
        .addSingleValueDimension(PK_COLUMN, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DERIVED_COLUMN, FieldSpec.DataType.BOOLEAN)
        .addDateTime(COMPARISON_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addDateTime("shippedAt", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addDateTime("eventAt", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addDateTime("shippedCutline", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(List.of(PK_COLUMN)).build();
  }

  private UpsertConfig createUpsertConfig() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setComparisonColumns(List.of(COMPARISON_COLUMN));
    upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    upsertConfig.setPostPartialUpsertTransformConfigs(
        List.of(new TransformConfig(DERIVED_COLUMN, IS_MISSED_TRANSFORM)));
    return upsertConfig;
  }

  private TableConfig createTableConfig(Schema schema, UpsertConfig upsertConfig) {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(schema.getSchemaName())
        .setTimeColumnName(COMPARISON_COLUMN).setNullHandlingEnabled(true).setUpsertConfig(upsertConfig).build();
  }

  private interface ThreadBody {
    List<String> run(int threadIndex);
  }

  /**
   * A {@link LazyRow} backed by a fixed map, so each thread gets its own previous row with no segment reads.
   */
  private static final class FixedPreviousRow extends LazyRow {
    private final Map<String, Object> _values;

    FixedPreviousRow(Map<String, Object> values) {
      _values = values;
    }

    @Nullable
    @Override
    public Object getValue(String fieldName) {
      return _values.get(fieldName);
    }

    @Override
    public boolean isNullValue(String fieldName) {
      return _values.get(fieldName) == null;
    }

    @Override
    public Set<String> getColumnNames() {
      return Collections.unmodifiableSet(_values.keySet());
    }

    @Override
    public void clear() {
    }
  }
}
