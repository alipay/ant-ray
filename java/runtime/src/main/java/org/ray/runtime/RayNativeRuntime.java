package org.ray.runtime;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.plasma.PlasmaClient;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.functionmanager.LocalFunctionManager;
import org.ray.runtime.functionmanager.NativeRemoteFunctionManager;
import org.ray.runtime.functionmanager.NopRemoteFunctionManager;
import org.ray.runtime.functionmanager.RemoteFunctionManager;
import org.ray.runtime.gcs.KeyValueStoreLink;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.objectstore.ObjectStoreProxy;
import org.ray.runtime.raylet.RayletClientImpl;
import org.ray.runtime.runner.RunManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * native runtime for local box and cluster run.
 */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private KeyValueStoreLink kvStore = null;
  private RunManager manager = null;

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  @Override
  public void start() throws Exception {
    // Load native libraries.
    try {
      System.loadLibrary("local_scheduler_library_java");
      System.loadLibrary("plasma_java");
    } catch (Exception e) {
      LOGGER.error("Failed to load native libraries.", e);
      throw e;
    }

    if (rayConfig.getRedisAddress() == null) {
      manager = new RunManager(rayConfig);
      manager.startRayProcesses(true);
    }
    kvStore = new RedisClient(rayConfig.getRedisAddress());

    // initialize remote function manager
    RemoteFunctionManager rfm = rayConfig.runMode != RunMode.CLUSTER
        ? new NopRemoteFunctionManager(rayConfig.driverId)
        : new NativeRemoteFunctionManager(kvStore);
    this.functions = new LocalFunctionManager(rfm);

    // initialize worker context
    if (rayConfig.workerMode == WorkerMode.DRIVER) {
      // TODO: The relationship between workerID, driver_id and dummy_task.driver_id should be
      // recheck carefully
      workerContext.setWorkerId(rayConfig.driverId);
    }

    ObjectStoreLink store = new PlasmaClient(rayConfig.objectStoreSocketName, "", 0);
    objectStoreProxy = new ObjectStoreProxy(this, store);

    rayletClient = new RayletClientImpl(
        rayConfig.rayletSocketName,
        workerContext.getCurrentWorkerId(),
        rayConfig.workerMode == WorkerMode.WORKER,
        workerContext.getCurrentTask().taskId
    );

    // register
    registerWorker();

    LOGGER.info("RayNativeRuntime started with store {}, raylet {}",
        rayConfig.objectStoreSocketName, rayConfig.rayletSocketName);
  }

  @Override
  public void shutdown() {
    if (null != manager) {
      manager.cleanup();
    }
  }

  private void registerWorker() {
    Map<String, String> workerInfo = new HashMap<>();
    String workerId = new String(workerContext.getCurrentWorkerId().getBytes());
    if (rayConfig.workerMode == WorkerMode.DRIVER) {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("driver_id", workerId);
      workerInfo.put("start_time", String.valueOf(System.currentTimeMillis()));
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      workerInfo.put("name", System.getProperty("user.dir"));
      //TODO: worker.redis_client.hmset(b"Drivers:" + worker.workerId, driver_info)
      kvStore.hmset("Drivers:" + workerId, workerInfo);
    } else {
      workerInfo.put("node_ip_address", rayConfig.nodeIp);
      workerInfo.put("plasma_store_socket", rayConfig.objectStoreSocketName);
      workerInfo.put("raylet_socket", rayConfig.rayletSocketName);
      //TODO: b"Workers:" + worker.workerId,
      kvStore.hmset("Workers:" + workerId, workerInfo);
    }
  }

}
