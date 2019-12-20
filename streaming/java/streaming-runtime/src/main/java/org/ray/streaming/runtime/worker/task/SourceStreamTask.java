package org.ray.streaming.runtime.worker.task;

import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.core.processor.SourceProcessor;
import org.ray.streaming.runtime.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SourceStreamTask extends StreamTask{
  private static final Logger LOG = LoggerFactory.getLogger(SourceStreamTask.class);

  private final SourceProcessor sourceProcessor;

  public SourceStreamTask(int taskId, Processor sourceProcessor, JobWorker jobWorker) {
    super(taskId, sourceProcessor, jobWorker);
    this.sourceProcessor = (SourceProcessor) processor;
  }

  @Override
  public void run() {
    while (running) {
      sourceProcessor.run();
    }
  }
}
