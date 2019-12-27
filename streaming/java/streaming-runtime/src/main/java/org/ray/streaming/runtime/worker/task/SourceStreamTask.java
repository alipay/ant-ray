package org.ray.streaming.runtime.worker.task;

import org.slf4j.Logger;

import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.core.processor.SourceProcessor;
import org.ray.streaming.runtime.util.LoggerFactory;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 *
 */
public class SourceStreamTask extends StreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStreamTask.class);

  private final SourceProcessor sourceProcessor;

  public SourceStreamTask(int taskId, Processor sourceProcessor, JobWorker jobWorker) {
    super(taskId, sourceProcessor, jobWorker);
    this.sourceProcessor = (SourceProcessor) processor;
  }

  @Override
  protected void init() throws Exception {
  }

  @Override
  protected void cancelTask() throws Exception {
  }

  @Override
  public void run() {
    LOG.info("Source stream task thread start.");

    while (running) {
      sourceProcessor.run();
    }
  }
}
