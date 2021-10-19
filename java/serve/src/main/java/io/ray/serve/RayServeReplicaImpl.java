package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import io.ray.api.BaseActorHandle;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Histogram;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import io.ray.serve.poll.KeyListener;
import io.ray.serve.poll.KeyType;
import io.ray.serve.poll.LongPollClient;
import io.ray.serve.poll.LongPollNamespace;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ReflectUtil;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles requests with the provided callable. */
public class RayServeReplicaImpl implements RayServeReplica {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayServeReplicaImpl.class);

  private String backendTag;

  private String replicaTag;

  private BackendConfig config;

  private AtomicInteger numOngoingRequests = new AtomicInteger();

  private Object callable;

  private Count requestCounter;

  private Count errorCounter;

  private Count restartCounter;

  private Histogram processingLatencyTracker;

  private Gauge numProcessingItems;

  private LongPollClient longPollClient;

  private BackendVersion version;

  private boolean isDeleted = false;

  private final Method checkHealthMethod;

  private final Method callMethod;

  public RayServeReplicaImpl(
      Object callable,
      BackendConfig backendConfig,
      BackendVersion version,
      BaseActorHandle actorHandle) {
    this.backendTag = Serve.getReplicaContext().getBackendTag();
    this.replicaTag = Serve.getReplicaContext().getReplicaTag();
    this.callable = callable;
    this.config = backendConfig;
    this.version = version;

    this.checkHealthMethod = getRunnerMethod(Constants.CHECK_HEALTH_METHOD, null, true);
    this.callMethod = getRunnerMethod(Constants.CALL_METHOD, new Object[] {new Object()}, true);

    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(
        new KeyType(LongPollNamespace.BACKEND_CONFIGS, backendTag),
        newConfig -> updateBackendConfigs(newConfig));
    this.longPollClient = new LongPollClient(actorHandle, keyListeners);
    this.longPollClient.start();
    registerMetrics();
  }

  private void registerMetrics() {
    RayServeMetrics.execute(
        () ->
            requestCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_BACKEND_REQUEST_COUNTER.getName())
                    .description(RayServeMetrics.SERVE_BACKEND_REQUEST_COUNTER.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            errorCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_BACKEND_ERROR_COUNTER.getName())
                    .description(RayServeMetrics.SERVE_BACKEND_ERROR_COUNTER.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            restartCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_BACKEND_REPLICA_STARTS.getName())
                    .description(RayServeMetrics.SERVE_BACKEND_REPLICA_STARTS.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            processingLatencyTracker =
                Metrics.histogram()
                    .name(RayServeMetrics.SERVE_BACKEND_PROCESSING_LATENCY_MS.getName())
                    .description(
                        RayServeMetrics.SERVE_BACKEND_PROCESSING_LATENCY_MS.getDescription())
                    .unit("")
                    .boundaries(Constants.DEFAULT_LATENCY_BUCKET_MS)
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            numProcessingItems =
                Metrics.gauge()
                    .name(RayServeMetrics.SERVE_REPLICA_PROCESSING_QUERIES.getName())
                    .description(RayServeMetrics.SERVE_REPLICA_PROCESSING_QUERIES.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(() -> restartCounter.inc(1.0));
  }

  @Override
  public Object handleRequest(Object requestMetadata, Object requestArgs) {
    long startTime = System.currentTimeMillis();
    Query request = new Query((RequestMetadata) requestMetadata, requestArgs);
    LOGGER.debug(
        "Replica {} received request {}", replicaTag, request.getMetadata().getRequestId());

    numOngoingRequests.incrementAndGet();
    RayServeMetrics.execute(() -> numProcessingItems.update(numOngoingRequests.get()));
    Object result = invokeSingle(request);
    numOngoingRequests.decrementAndGet();

    long requestTimeMs = System.currentTimeMillis() - startTime;
    LOGGER.debug(
        "Replica {} finished request {} in {}ms",
        replicaTag,
        request.getMetadata().getRequestId(),
        requestTimeMs);

    return result;
  }

  private Object invokeSingle(Query requestItem) {

    long start = System.currentTimeMillis();
    Method methodToCall = null;
    try {
      LOGGER.debug(
          "Replica {} started executing request {}",
          replicaTag,
          requestItem.getMetadata().getRequestId());

      Object[] args = parseRequestItem(requestItem);
      methodToCall =
          args.length == 1 && callMethod != null
              ? callMethod
              : getRunnerMethod(requestItem.getMetadata().getCallMethod(), args, false);
      Object result = methodToCall.invoke(callable, args);
      RayServeMetrics.execute(() -> requestCounter.inc(1.0));
      return result;
    } catch (Throwable e) {
      RayServeMetrics.execute(() -> errorCounter.inc(1.0));
      throw new RayServeException(
          LogUtil.format(
              "Replica {} failed to invoke method {}",
              replicaTag,
              methodToCall == null ? "unknown" : methodToCall.getName()),
          e);
    } finally {
      RayServeMetrics.execute(
          () -> processingLatencyTracker.update(System.currentTimeMillis() - start));
    }
  }

  private Object[] parseRequestItem(Query requestItem) {
    if (requestItem.getArgs() == null) {
      return new Object[0];
    }

    // From Java Proxy or Handle.
    if (requestItem.getArgs() instanceof Object[]) {
      return (Object[]) requestItem.getArgs();
    }

    // From other language Proxy or Handle.
    RequestWrapper requestWrapper = (RequestWrapper) requestItem.getArgs();
    if (requestWrapper.getBody() == null || requestWrapper.getBody().isEmpty()) {
      return new Object[0];
    }

    return new Object[] {
      MessagePackSerializer.decode(requestWrapper.getBody().toByteArray(), Object.class)
    };
  }

  private Method getRunnerMethod(String methodName, Object[] args, boolean isNullable) {
    try {
      return ReflectUtil.getMethod(callable.getClass(), methodName, args);
    } catch (NoSuchMethodException e) {
      String errMsg =
          LogUtil.format(
              "Tried to call a method {} that does not exist. Available methods: {}",
              methodName,
              ReflectUtil.getMethodStrings(callable.getClass()));
      if (isNullable) {
        LOGGER.warn(errMsg);
        return null;
      } else {
        LOGGER.error(errMsg, e);
        throw new RayServeException(errMsg, e);
      }
    }
  }

  /**
   * Perform graceful shutdown. Trigger a graceful shutdown protocol that will wait for all the
   * queued tasks to be completed and return to the controller.
   */
  @Override
  public synchronized boolean prepareForShutdown() {
    while (true) {
      // Sleep first because we want to make sure all the routers receive the notification to remove
      // this replica first.
      try {
        Thread.sleep((long) (config.getGracefulShutdownWaitLoopS() * 1000));
      } catch (InterruptedException e) {
        LOGGER.error(
            "Replica {} was interrupted in sheep when draining pending queries", replicaTag);
      }
      if (numOngoingRequests.get() == 0) {
        break;
      } else {
        LOGGER.info(
            "Waiting for an additional {}s to shut down because there are {} ongoing requests.",
            config.getGracefulShutdownWaitLoopS(),
            numOngoingRequests.get());
      }
    }

    // Explicitly call the del method to trigger clean up. We set isDeleted = true after
    // succssifully calling it so the destructor is called only once.
    try {
      if (!isDeleted) {
        ReflectUtil.getMethod(callable.getClass(), "del").invoke(callable);
      }
    } catch (NoSuchMethodException e) {
      LOGGER.warn("Deployment {} has no del method.", backendTag);
    } catch (Throwable e) {
      LOGGER.error("Exception during graceful shutdown of replica.");
    } finally {
      isDeleted = true;
    }
    return true;
  }

  /**
   * Reconfigure user's configuration in the callable object through its reconfigure method.
   *
   * @param userConfig new user's configuration
   */
  @Override
  public BackendVersion reconfigure(Object userConfig) {
    BackendVersion backendVersion = new BackendVersion(version.getCodeVersion(), userConfig);
    version = backendVersion;
    if (userConfig == null) {
      return backendVersion;
    }

    LOGGER.info(
        "Replica {} of deployment {} reconfigure userConfig: {}",
        replicaTag,
        backendTag,
        userConfig);
    try {
      ReflectUtil.getMethod(callable.getClass(), Constants.RECONFIGURE_METHOD, userConfig)
          .invoke(callable, userConfig);
      return version;
    } catch (NoSuchMethodException e) {
      String errMsg =
          LogUtil.format(
              "user_config specified but backend {} missing {} method",
              backendTag,
              Constants.RECONFIGURE_METHOD);
      LOGGER.error(errMsg);
      throw new RayServeException(errMsg, e);
    } catch (Throwable e) {
      String errMsg =
          LogUtil.format(
              "Replica {} of deployment {} failed to reconfigure userConfig {}",
              replicaTag,
              backendTag,
              userConfig);
      LOGGER.error(errMsg);
      throw new RayServeException(errMsg, e);
    } finally {
      LOGGER.info(
          "Replica {} of deployment {} finished reconfiguring userConfig: {}",
          replicaTag,
          backendTag,
          userConfig);
    }
  }

  /**
   * Update backend configs.
   *
   * @param newConfig the new configuration of backend
   */
  private void updateBackendConfigs(Object newConfig) {
    config = (BackendConfig) newConfig;
  }

  public BackendVersion getVersion() {
    return version;
  }

  @Override
  public boolean checkHealth() {
    if (checkHealthMethod == null) {
      return true;
    }
    boolean result = true;
    try {
      LOGGER.info(
          "Replica {} of deployment {} check health of {}",
          replicaTag,
          backendTag,
          callable.getClass().getName());
      Object isHealthy = checkHealthMethod.invoke(callable);
      if (!(isHealthy instanceof Boolean)) {
        LOGGER.error(
            "The health check result {} of {} in replica {} of deployment {} is illegal.",
            isHealthy == null ? "null" : isHealthy.getClass().getName() + ":" + isHealthy,
            callable.getClass().getName(),
            replicaTag,
            backendTag);
        result = false;
      } else {
        result = (boolean) isHealthy;
      }
    } catch (Throwable e) {
      LOGGER.error(
          "Replica {} of deployment {} failed to check health of {}",
          replicaTag,
          backendTag,
          callable.getClass().getName(),
          e);
      result = false;
    } finally {
      LOGGER.info(
          "The health check result of {} in replica {} of deployment {} is {}.",
          callable.getClass().getName(),
          replicaTag,
          backendTag,
          result);
    }
    return result;
  }

  public Object getCallable() {
    return callable;
  }
}
