package org.ray.runtime;

import com.google.common.base.Preconditions;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtime.RayRuntimeFactory;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.runner.RunManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default Ray runtime factory. It produces an instance of RayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRayRuntimeFactory.class);

  @Override
  public RayRuntime createRayRuntime() {
    RayConfig rayConfig = RayConfig.create();
    try {
      FunctionManager functionManager = new FunctionManager(rayConfig.jobResourcePath);
      RayRuntime runtime;
      if (rayConfig.runMode == RunMode.SINGLE_PROCESS) {
        runtime = new RayDevRuntime(rayConfig, functionManager);
      } else {
        RunManager manager = null;
        final boolean startHeadProcesses = (rayConfig.getRedisAddress() == null);
        if (startHeadProcesses) {
          manager = new RunManager(rayConfig);
          manager.startRayProcesses(true);
        }
        RayNativeRuntime.setup(startHeadProcesses, rayConfig);

        if (rayConfig.workerMode == WorkerType.DRIVER) {
          runtime = new RayNativeRuntime(manager, rayConfig, functionManager);
        } else {
          Preconditions.checkState(manager == null);
          runtime = new RayMultiWorkerNativeRuntime(rayConfig, functionManager);
        }
      }
      return runtime;
    } catch (Exception e) {
      LOGGER.error("Failed to initialize ray runtime", e);
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
