package com.ray.streaming.api.function.internal;

import com.ray.streaming.api.function.impl.SourceFunction;
import java.util.Collection;

/**
 * The SourceFunction that fetch data from a Java Collection object.
 *
 * @param <T> Type of the data output by the source.
 */
public class CollectionSourceFunction<T> implements SourceFunction<T> {

  private Collection<T> values;

  public CollectionSourceFunction(Collection<T> values) {
    this.values = values;
  }

  @Override
  public void init(int parallel, int index) {
  }

  @Override
  public void fetch(long batchId, SourceContext<T> ctx) throws Exception {
    for (T value : values) {
      ctx.collect(value);
    }
  }

  @Override
  public void close() {
  }

}
