package io.ray.serve;

import java.io.Serializable;

/**
 * BackendConfig.
 */
public class BackendConfig implements Serializable {
  
  private static final long serialVersionUID = 6356109792183539217L;

  private int numReplicas;

  private int maxConcurrentQueries;

  private Object userConfig;

  private float experimentalGracefulShutdownWaitLoop;

  private float experimentalGracefulShutdownTimeout;

  public Object getUserConfig() {
    return userConfig;
  }

}
