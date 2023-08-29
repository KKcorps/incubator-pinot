package org.apache.pinot.segment.local.segment.creator.impl.vec;

import java.util.List;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.pinot.spi.data.readers.Vector;


public class ListRandomAccessVectorValues implements RandomAccessVectorValues<Vector> {

  private final List<Vector> vectors;
  private final int dimension;

  public ListRandomAccessVectorValues(List<Vector> vectors, int dimension) {
    this.vectors = vectors;
    this.dimension = dimension;
  }

  @Override
  public int size() {
    return vectors.size();
  }

  @Override
  public int dimension() {
    return dimension;
  }

  @Override
  public Vector vectorValue(int targetOrd) {
    return vectors.get(targetOrd);
  }

  @Override
  public ListRandomAccessVectorValues copy() {
    return new ListRandomAccessVectorValues(vectors, dimension);
  }
}
