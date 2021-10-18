package io.ray.serve;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ReflectUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replica class wrapping the provided class. Note that Java function is not supported now. */
public class RayServeWrappedReplica implements RayServeReplica {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayServeReplicaImpl.class);

  private DeploymentInfo deploymentInfo;

  private RayServeReplicaImpl backend;

  public RayServeWrappedReplica(
      String backendTag,
      String replicaTag,
      String backendDef,
      byte[] initArgsbytes,
      byte[] backendConfigBytes,
      byte[] backendVersionBytes,
      String controllerName) {

    // Parse BackendConfig.
    BackendConfig backendConfig = ServeProtoUtil.parseBackendConfig(backendConfigBytes);

    // Parse init args.
    Object[] initArgs = null;
    try {
      initArgs = parseInitArgs(initArgsbytes, backendConfig);
    } catch (IOException e) {
      String errMsg =
          LogUtil.format(
              "Failed to initialize replica {} of deployment {}",
              replicaTag,
              deploymentInfo.getName());
      LOGGER.error(errMsg, e);
      throw new RayServeException(errMsg, e);
    }

    // Init replica.
    init(
        new DeploymentInfo()
            .setName(backendTag)
            .setBackendConfig(backendConfig)
            .setBackendVersion(ServeProtoUtil.parseBackendVersion(backendVersionBytes))
            .setBackendDef(backendDef)
            .setInitArgs(initArgs),
        replicaTag,
        controllerName);
  }

  public RayServeWrappedReplica(
      DeploymentInfo deploymentInfo, String replicaTag, String controllerName) {
    init(deploymentInfo, replicaTag, controllerName);
  }

  @SuppressWarnings("rawtypes")
  private void init(DeploymentInfo deploymentInfo, String replicaTag, String controllerName) {
    try {
      // Set the controller name so that Serve.connect() in the user's backend code will connect to
      // the instance that this backend is running in.
      Serve.setInternalReplicaContext(deploymentInfo.getName(), replicaTag, controllerName, null);

      // Instantiate the object defined by backendDef.
      Class backendClass = Class.forName(deploymentInfo.getBackendDef());
      Object callable =
          ReflectUtil.getConstructor(backendClass, deploymentInfo.getInitArgs())
              .newInstance(deploymentInfo.getInitArgs());
      Serve.getReplicaContext().setServableObject(callable);

      // Get the controller by controllerName.
      Preconditions.checkArgument(
          StringUtils.isNotBlank(controllerName), "Must provide a valid controllerName");
      Optional<BaseActorHandle> optional = Ray.getActor(controllerName);
      Preconditions.checkState(optional.isPresent(), "Controller does not exist");

      // Enable metrics.
      enableMetrics(deploymentInfo.getConfig());

      // Construct worker replica.
      this.backend =
          new RayServeReplicaImpl(
              callable,
              deploymentInfo.getBackendConfig(),
              deploymentInfo.getBackendVersion(),
              optional.get());
      this.deploymentInfo = deploymentInfo;
    } catch (Throwable e) {
      String errMsg =
          LogUtil.format(
              "Failed to initialize replica {} of deployment {}",
              replicaTag,
              deploymentInfo.getName());
      LOGGER.error(errMsg, e);
      throw new RayServeException(errMsg, e);
    }
  }

  private void enableMetrics(Map<String, String> config) {
    Optional.ofNullable(config)
        .map(conf -> conf.get(RayServeConfig.METRICS_ENABLED))
        .ifPresent(
            enabled -> {
              if (Boolean.valueOf(enabled)) {
                RayServeMetrics.enable();
              } else {
                RayServeMetrics.disable();
              }
            });
  }

  private Object[] parseInitArgs(byte[] initArgsbytes, BackendConfig backendConfig)
      throws IOException {

    if (initArgsbytes == null || initArgsbytes.length == 0) {
      return new Object[0];
    }

    if (backendConfig.isCrossLanguage()) {
      // For other language like Python API, not support Array type.
      return new Object[] {MessagePackSerializer.decode(initArgsbytes, Object.class)};
    } else {
      // If the construction request is from Java API, deserialize initArgsbytes to Object[]
      // directly.
      return MessagePackSerializer.decode(initArgsbytes, Object[].class); // TODO hession
    }
  }

  /**
   * The entry method to process the request.
   *
   * @param requestMetadata the real type is byte[] if this invocation is cross-language. Otherwise,
   *     the real type is {@link io.ray.serve.generated.RequestMetadata}.
   * @param requestArgs The input parameters of the specified method of the object defined by
   *     backendDef. The real type is serialized {@link io.ray.serve.generated.RequestWrapper} if
   *     this invocation is cross-language. Otherwise, the real type is Object[].
   * @return the result of request being processed
   */
  @Override
  public Object handleRequest(Object requestMetadata, Object requestArgs) {
    boolean isCrossLanguage = requestMetadata instanceof byte[];
    return backend.handleRequest(
        isCrossLanguage
            ? ServeProtoUtil.parseRequestMetadata((byte[]) requestMetadata)
            : (RequestMetadata) requestMetadata,
        isCrossLanguage ? ServeProtoUtil.parseRequestWrapper((byte[]) requestArgs) : requestArgs);
  }

  /** Check if the actor is healthy. */
  @Override
  public boolean checkHealth() {
    return backend.checkHealth();
  }

  /**
   * Wait until there is no request in processing. It is used for stopping replica gracefully.
   *
   * @return true if it is ready for shutdown.
   */
  @Override
  public boolean prepareForShutdown() {
    return backend.prepareForShutdown();
  }

  @Override
  public Object reconfigure(Object userConfig) {
    BackendVersion backendVersion =
        backend.reconfigure(
            deploymentInfo.getBackendConfig().isCrossLanguage() && userConfig != null
                ? new Object[] {MessagePackSerializer.decode((byte[]) userConfig, Object.class)}
                : (Object[]) userConfig);
    return deploymentInfo.getBackendConfig().isCrossLanguage()
        ? ServeProtoUtil.toProtobuf(backendVersion).toByteArray()
        : backendVersion;
  }

  public Object getVersion() {
    BackendVersion backendVersion = backend.getVersion();
    return deploymentInfo.getBackendConfig().isCrossLanguage()
        ? ServeProtoUtil.toProtobuf(backendVersion).toByteArray()
        : backendVersion;
  }

  public Object getCallable() {
    return backend.getCallable();
  }
}
