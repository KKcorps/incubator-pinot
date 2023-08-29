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
package org.apache.pinot.core.operator.filter;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Operator for TEXT_CONTAINS query.
 */
public class VectorSimilarityFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "VECTOR_SIMILARITY_INDEX";

  private final VectorIndexReader _vectorIndexReader;
  private final VectorSimilarityPredicate _predicate;

  public VectorSimilarityFilterOperator(VectorIndexReader textIndexReader, VectorSimilarityPredicate predicate, int numDocs) {
    super(numDocs, false);
    _vectorIndexReader = textIndexReader;
    _predicate = predicate;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    return new BitmapDocIdSet(_vectorIndexReader.getDocIds(_predicate.getValue()), _numDocs);
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    return _vectorIndexReader.getDocIds(_predicate.getValue()).getCardinality();
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    ImmutableRoaringBitmap bitmap = _vectorIndexReader.getDocIds(_predicate.getValue());
    record(bitmap);
    return new BitmapCollection(_numDocs, false, bitmap);
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:vector_index");
    stringBuilder.append(",operator:").append(_predicate.getType());
    stringBuilder.append(",predicate:").append(_predicate.toString());
    return stringBuilder.append(')').toString();
  }

  private void record(ImmutableRoaringBitmap matches) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setNumDocsMatchingAfterFilter(matches.getCardinality());
      recording.setColumnName(_predicate.getLhs().getIdentifier());
      recording.setFilter(FilterType.INDEX, "VECTOR_SIMILARITY");
    }
  }
}
