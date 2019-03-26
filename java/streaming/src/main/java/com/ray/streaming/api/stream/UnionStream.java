package com.ray.streaming.api.stream;

import com.ray.streaming.operator.StreamOperator;
import java.util.ArrayList;
import java.util.List;

/**
 * Multi DataStream Union.
 * @param <T> input element
 */
public class UnionStream<T> extends DataStream<T> {

  private List<DataStream> unionStreams;

  public UnionStream(DataStream input, StreamOperator streamOperator, DataStream<T> other) {
    super(input, streamOperator);
    this.unionStreams = new ArrayList<>();
    this.unionStreams.add(other);
  }

  public List<DataStream> getUnionStreams() {
    return unionStreams;
  }
}
