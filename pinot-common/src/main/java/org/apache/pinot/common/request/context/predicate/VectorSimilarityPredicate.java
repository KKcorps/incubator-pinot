package org.apache.pinot.common.request.context.predicate;

import java.util.Objects;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.spi.data.readers.Vector;


public class VectorSimilarityPredicate extends BasePredicate {
  private final Vector _value;

  public VectorSimilarityPredicate(ExpressionContext lhs, Vector value) {
    super(lhs);
    _value = value;
//    _value = new Vector(value.length, value);
//    _value = Vector.fromString(value);
  }

  @Override
  public Type getType() {
    return Type.VECTOR_SIMILARITY;
  }

  @Override
  public ExpressionContext getLhs() {
    return _lhs;
  }

  @Override
  public void setLhs(ExpressionContext lhs) {
    _lhs = lhs;
  }

  public Vector getValue() {
    return _value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TextContainsPredicate)) {
      return false;
    }
    VectorSimilarityPredicate that = (VectorSimilarityPredicate) o;
    return Objects.equals(_lhs, that._lhs) && Objects.equals(_value, that._value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _value);
  }

  @Override
  public String toString() {
    return "vector_similarity(" + _lhs + ",'" + _value + "')";
  }
}
