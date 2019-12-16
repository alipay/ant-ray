package org.ray.streaming.runtime.worker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.ray.api.Checkpointable;
import org.ray.api.Ray;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;
import org.ray.streaming.runtime.config.StreamingWorkerConfig;
import org.ray.streaming.runtime.config.internal.WorkerConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.jobgraph.JobEdge;
import org.ray.streaming.runtime.core.processor.OneInputProcessor;
import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.core.processor.SourceProcessor;
import org.ray.streaming.runtime.core.processor.TwoInputProcessor;
import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.worker.task.ControlMessage;
import org.ray.streaming.runtime.worker.task.SourceStreamTask;
import org.ray.streaming.runtime.worker.task.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The streaming worker implementation class, it is ray actor.
 */
@RayRemote
public class JobWorker implements IJobWorker {

  private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);

  /**
   * Worker(execution vertex) configuration
   */
  private StreamingWorkerConfig workerConfig;

  /**
   * The context of job worker
   */
  protected JobWorkerContext workerContext;

  private ExecutionVertex executionVertex;

  /**
   * The thread of stream task
   */
  private StreamTask task;



  private byte[] executionVertexBytes;

  /**
   * Control message
   */
  private volatile boolean hasMessage = false;

  private Object lock = new Object();

  public JobWorker() {
  }

  public JobWorker(final byte[] confBytes) {
    LOG.info("Job worker begin init.");

    Map<String, String> confMap = KryoUtils.readFromByteArray(confBytes);
    workerConfig = new StreamingWorkerConfig(confMap);
    LOG.info("Job worker conf is {}.", workerConfig.configMap);

    LOG.info("Job worker init success.");
  }

  @Override
  public void init(JobWorkerContext workerContext) {
    LOG.info("Init worker context {}. workerId: {}.", workerContext, workerContext.workerId);
    ExecutionVertex executionVertex = null;
    if (null != workerContext.executionVertexBytes) {
      executionVertex = KryoUtils.readFromByteArray(workerContext.executionVertexBytes);
    }
    this.workerContext = workerContext;
    this.workerConfig = new StreamingWorkerConfig(workerContext.conf);
    this.executionVertex = executionVertex;
  }

  @Override
  public void start() {
    if (task != null) {
      task.close();
      task = null;
    }

    task = createStreamTask();

  }

  // ----------------------------------------------------------------------
  // Job Worker Destroy
  // ----------------------------------------------------------------------

  @Override
  public boolean destroy() {
    shouldCheckpoint = true;

    try {
      if (task != null) {
        // make sure the runner is closed
        task.close();
        task = null;
      }
    } catch (Exception e) {
      LOG.error("Close runner has exception.", e);
    }

    return true;
  }

  // ----------------------------------------------------------------------
  // Job Worker Auto Scale
  // ----------------------------------------------------------------------

  /**
   * Inserts control message at the tail of this queue, waiting for space to become available if the
   * queue is full.
   *
   * @return true if put successfully
   */
  private boolean insertControlMessage(ControlMessage message) {
    try {
      synchronized (lock) {
        LOG.info("Worker {} before inserting, mailbox: {}, hasMessage: {}.", context.workerId,
            context.mailbox, hasMessage);

        context.mailbox.put(message);
        hasMessage = true;

        LOG.info("Worker {} after inserting, mailbox: {}, hasMessage: {}.", context.workerId,
            context.mailbox, hasMessage);
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to insert control message to mailbox.", e);
      return false;
    }
    return true;
  }

  /**
   * Retrieves and removes control message at the head of this queue, or returns {@code null} if
   * this queue is empty.
   *
   * @return control message at head of this queue, or {@code null} if this queue is empty.
   */
  public ControlMessage pollControlMessage() {
    ControlMessage message;
    synchronized (lock) {
      LOG.info("Worker {} before polling, mailbox: {}, hasMessage: {}.", context.workerId,
          context.mailbox, hasMessage);

      message = context.mailbox.poll();
      hasMessage = !context.mailbox.isEmpty();

      LOG.info("Worker {} polled message from mailbox: {}, remaining: {}, hasMessage: {}.",
          context.workerId, message, context.mailbox, hasMessage);
      return message;
    }
  }

  /**
   * Check whether mailbox has control message or not (lock free)
   *
   * @return true if worker mailbox still has message.
   */
  public boolean hasControlMessage() {
    return this.hasMessage;
  }

  /**
   * Hot update worker context
   *
   * @param newContext job worker context which is updated
   */
  @Override
  public void updateContext(JobWorkerContext newContext) {
    try {
      LOG.info("Insert update context control message into mailbox.");
      ControlMessage<JobWorkerContext> message = new ControlMessage(newContext, UPDATE_CONTEXT);
      insertControlMessage(message);
    } catch (Exception e) {
      LOG.error("Failed to update context.", e);
    }
  }


  @Override
  public void shutdown() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Worker shutdown now.");
      }
    });
    System.exit(0);
  }



  public void setContext(JobWorkerContext context) {
    this.context = context;
  }

  public JobWorkerContext getContext() {
    return context;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  public StreamTask getTask() {
    return task;
  }

  private StreamTask createStreamTask() {
    StreamTask task;
    Processor processor = this.executionVertex.getExeJobVertex().getJobVertex().getProcessor();
    if (processor instanceof SourceProcessor) {
      // source actor
      LOG.info("Create source stream task with {}, operator is {}.",
          checkpointId, conf.workerConfig.operatorName());
      task = new SourceStreamTask(processor, checkpointId,
          stateBackend, this);
    } else if (processor instanceof OneInputProcessor) {
      LOG.info("Create one input stream task with {}, operator is {}.",
          checkpointId, conf.workerConfig.operatorName());
      task = new OneInputStreamTask(processor, checkpointId, stateBackend, this);
    } else if (processor instanceof TwoInputProcessor) {
      LOG.info("Create two input stream task with {}, operator is {}.",
          checkpointId, conf.workerConfig.operatorName());
      List<JobEdge> jobEdges = this.executionVertex.getExeJobVertex().getJobVertex().getInputs();
      Preconditions.checkState(jobEdges.size() == 2,
          "Two input vertex input edge size must be 2.");
      String leftStream = jobEdges.get(0).getSource().getProducer().getId().toString();
      String rightStream = jobEdges.get(1).getSource().getProducer().getId().toString();
      task = new TwoInputStreamTask(processor, checkpointId,
          stateBackend, this,
          leftStream,
          rightStream);
    } else {
      throw new RuntimeException("Unsupported processor type: " + processor);
    }
    return task;
  }


  private static StreamingWorkerConfig getJobWorkerConf(final byte[] confBytes) {
    Map<String, String> confMap = KryoUtils.readFromByteArray(confBytes);
    return new StreamingWorkerConfig(confMap);
  }

  private Map<String, String> getJobWorkerTags() {
    Map<String, String> workerTags = new HashMap<>();
    workerTags.put("worker_name", this.context.workerName);
    workerTags.put("op_name", this.context.opName);
    workerTags.put("worker_id", this.context.workerId.toString());
    return workerTags;
  }

  public StreamingWorkerConfig getConf() {
    return this.conf;
  }
}
