package org.ray.streaming.runtime.core.processor;

import java.util.List;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.operator.Operator;
import org.ray.streaming.runtime.worker.context.RayRuntimeContext;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.backend.impl.MemoryStateBackend;
import org.ray.streaming.state.config.ConfigKey;
import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.KeyGroupAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingProcessor is a process unit for a operator.
 *
 * @param <T> The type of process data.
 * @param <P> Type of the specific operator class.
 */
public abstract class StreamProcessor<T, P extends Operator> implements Processor<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamProcessor.class);

  protected List<Collector> collectors;
  protected RuntimeContext runtimeContext;
  protected P operator;
  private KeyStateBackend keyStateManager;

  public StreamProcessor(P operator) {
    this.operator = operator;
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    this.collectors = collectors;
    this.runtimeContext = runtimeContext;
    int maxPara = Integer.valueOf(runtimeContext.getConfig().getOrDefault(
        ConfigKey.JOB_MAX_PARALLEL, "1024"));
    KeyGroup subProcessorKeyGroup = KeyGroupAssignment
        .computeKeyGroupRangeForOperatorIndex(maxPara, runtimeContext.getParallelism(),
            runtimeContext.getTaskIndex());
    this.keyStateManager = new KeyStateBackend(maxPara, subProcessorKeyGroup,
        new MemoryStateBackend(runtimeContext.getConfig()));
    ((RayRuntimeContext) runtimeContext).setKeyStateManager(keyStateManager);
    if (operator != null) {
      this.operator.open(collectors, runtimeContext);
    }
    LOGGER.info("opened {}", this);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
