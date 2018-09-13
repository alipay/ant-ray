package org.ray.api.runtime;

import java.lang.reflect.Method;

import org.ray.api.config.RayConfig;

/**
 * The default Ray runtime factory. It produces an instance of AbstractRayRuntime.
 */
public class DefaultRayRuntimeFactory implements RayRuntimeFactory {

  // we already get the ray config, choose the right runtime object here directly
  @Override
  public RayRuntime createRayRuntime(RayConfig initConfig) {
    try {
      Class clz;
      // todo:: make it better
      if (initConfig.getParams().run_mode.isNativeRuntime()) {
        clz = Class.forName("org.ray.runtime.RayNativeRuntime");
      } else {
        clz = Class.forName("org.ray.runtime.RayDevRuntime");
      }
      RayRuntime runtime = (RayRuntime) clz.newInstance();

      Method init = clz.getMethod("init", RayConfig.class);
      init.setAccessible(true);
      init.invoke(runtime, initConfig);
      init.setAccessible(false);
      return runtime;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize ray runtime", e);
    }
  }
}
