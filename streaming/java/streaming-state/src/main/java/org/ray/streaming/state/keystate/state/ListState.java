package org.ray.streaming.state.keystate.state;

import java.util.List;

/**
 * ListState interface.
 *
 * @author wutao on 2019/7/25
 */
public interface ListState<T> extends OneOutState<List<T>> {

  /**
   * add the value to list
   *
   * @param value the new value
   */
  void add(T value);

  /**
   * update list state
   *
   * @param list the new value
   */
  void update(List<T> list);
}
