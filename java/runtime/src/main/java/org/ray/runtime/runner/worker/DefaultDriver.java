package org.ray.runtime.runner.worker;

import org.ray.api.Ray;
import org.ray.api.RayInitConfig;
import org.ray.api.WorkerMode;
import org.ray.runtime.AbstractRayRuntime;

/**
 * The main function of DefaultDriver.
 */
public class DefaultDriver {

  //
  // " --node-ip-address=" + ip
  // + " --redis-address=" + redisAddress
  // + " --driver-class" + className
  //
  public static void main(String[] args) {
    try {
      //TODO(qwang): We should use `Ray.init`, not `AbstractRayRuntime.init`.
      RayInitConfig initConfig = new RayInitConfig(args);
      Ray.init(initConfig);
      //assert AbstractRayRuntime.getParams().worker_mode == WorkerMode.DRIVER;

      String driverClass = AbstractRayRuntime.configReader
          .getStringValue("ray.java.start", "driver_class", "",
              "java class which main is served as the driver in a java worker");
      String driverArgs = AbstractRayRuntime.configReader
          .getStringValue("ray.java.start", "driver_args", "",
              "arguments for the java class main function which is served at the driver");
      Class<?> cls = Class.forName(driverClass);
      String[] argsArray = (driverArgs != null) ? driverArgs.split(",") : (new String[] {});
      cls.getMethod("main", String[].class).invoke(null, (Object) argsArray);
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
