package io.ray.api;

import io.ray.api.id.UniqueId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.api.runtime.RayRuntime;
import io.ray.api.runtime.RayRuntimeFactory;
import io.ray.api.runtimecontext.RuntimeContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * This class contains all public APIs of Ray.
 */
public final class Ray extends RayCall {

  private static RayRuntime runtime = null;

  /**
   * Initialize Ray runtime with the default runtime implementation.
   */
  public static void init() {
    try {
      Class clz = Class.forName("io.ray.runtime.DefaultRayRuntimeFactory");
      RayRuntimeFactory factory = (RayRuntimeFactory) clz.newInstance();
      init(factory);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize Ray runtime.", e);
    }

  }

  /**
   * Initialize Ray runtime with a custom runtime implementation.
   *
   * @param factory A factory that produces the runtime instance.
   */
  public static synchronized void init(RayRuntimeFactory factory) {
    if (runtime == null) {
      runtime = factory.createRayRuntime();
      Runtime.getRuntime().addShutdownHook(new Thread(Ray::shutdown));
    }
  }

  /**
   * Shutdown Ray runtime.
   */
  public static synchronized void shutdown() {
    if (runtime != null) {
      getRuntime().shutdown();
      runtime = null;
    }
  }

  /**
   * Store an object in the object store.
   *
   * @param obj The Java object to be stored.
   * @return A ObjectRef instance that represents the in-store object.
   */
  public static <T> ObjectRef<T> put(T obj) {
    return getRuntime().put(obj);
  }

  /**
   * Get an object by `ObjectRef` from the object store.
   *
   * @param objectRef The reference of the object to get.
   * @return The Java object.
   */
  public static <T> T get(ObjectRef<T> objectRef) {
    return getRuntime().get(objectRef);
  }

  /**
   * Get a list of objects by `ObjectRef`s from the object store.
   *
   * @param objectList A list of object references.
   * @return A list of Java objects.
   */
  public static <T> List<T> get(List<ObjectRef<T>> objectList) {
    return getRuntime().get(objectList);
  }

  /**
   * Wait for a list of RayObjects to be locally available,
   * until specified number of objects are ready, or specified timeout has passed.
   *
   * @param waitList A list of object references to wait for.
   * @param numReturns The number of objects that should be returned.
   * @param timeoutMs The maximum time in milliseconds to wait before returning.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(List<ObjectRef<T>> waitList, int numReturns,
                                       int timeoutMs) {
    return getRuntime().wait(waitList, numReturns, timeoutMs);
  }

  /**
   * A convenient helper method for Ray.wait. It will wait infinitely until
   * specified number of objects are locally available.
   *
   * @param waitList A list of object references to wait for.
   * @param numReturns The number of objects that should be returned.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(List<ObjectRef<T>> waitList, int numReturns) {
    return getRuntime().wait(waitList, numReturns, Integer.MAX_VALUE);
  }

  /**
   * A convenient helper method for Ray.wait. It will wait infinitely until
   * all objects are locally available.
   *
   * @param waitList A list of object references to wait for.
   * @return Two lists, one containing locally available objects, one containing the rest.
   */
  public static <T> WaitResult<T> wait(List<ObjectRef<T>> waitList) {
    return getRuntime().wait(waitList, waitList.size(), Integer.MAX_VALUE);
  }

  /**
   * Get a handle to a named actor of current job.
   * <p>
   * Gets a handle to a named actor with the given name. The actor must
   * have been created with name specified.
   *
   * @param name The name of the named actor.
   * @return an ActorHandle to the actor if the actor of specified name exists or an
   *     Optional.empty()
   */
  public static <T extends BaseActorHandle> Optional<T> getActor(String name) {
    return getRuntime().getActor(name, false);
  }

  /**
   * Get a handle to a global named actor.
   * <p>
   * Gets a handle to a global named actor with the given name. The actor must
   * have been created with global name specified.
   *
   * @param name The global name of the named actor.
   * @return an ActorHandle to the actor if the actor of specified name exists or an
   *     Optional.empty()
   */
  public static <T extends BaseActorHandle> Optional<T> getGlobalActor(String name) {
    return getRuntime().getActor(name, true);
  }

  /**
   * If users want to use Ray API in their own threads, call this method to get the async context
   * and then call {@link #setAsyncContext} at the beginning of the new thread.
   *
   * @return The async context.
   */
  public static Object getAsyncContext() {
    return getRuntime().getAsyncContext();
  }

  /**
   * Set the async context for the current thread.
   *
   * @param asyncContext The async context to set.
   */
  public static void setAsyncContext(Object asyncContext) {
    getRuntime().setAsyncContext(asyncContext);
  }

  // TODO (kfstorm): add the `rollbackAsyncContext` API to allow rollbacking the async context of
  // the current thread to the one before `setAsyncContext` is called.

  // TODO (kfstorm): unify the `wrap*` methods.

  /**
   * If users want to use Ray API in their own threads, they should wrap their {@link Runnable}
   * objects with this method.
   *
   * @param runnable The runnable to wrap.
   * @return The wrapped runnable.
   */
  public static Runnable wrapRunnable(Runnable runnable) {
    return getRuntime().wrapRunnable(runnable);
  }

  /**
   * If users want to use Ray API in their own threads, they should wrap their {@link Callable}
   * objects with this method.
   *
   * @param callable The callable to wrap.
   * @return The wrapped callable.
   */
  public static <T> Callable<T> wrapCallable(Callable<T> callable) {
    return getRuntime().wrapCallable(callable);
  }

  /**
   * Get the underlying runtime instance.
   */
  public static RayRuntime internal() {
    return runtime;
  }

  public static RayRuntime getRuntime() {
    if (runtime == null) {
      throw new IllegalStateException(
          "Ray has not been started yet. You can start Ray with 'Ray.init()'");
    }
    return runtime;
  }

  /**
   * Update the resource for the specified client.
   * Set the resource for the specific node.
   */
  public static void setResource(UniqueId nodeId, String resourceName, double capacity) {
    getRuntime().setResource(resourceName, capacity, nodeId);
  }

  /**
   * Set the resource for local node.
   */
  public static void setResource(String resourceName, double capacity) {
    getRuntime().setResource(resourceName, capacity, UniqueId.NIL);
  }

  /**
   * Get the runtime context.
   */
  public static RuntimeContext getRuntimeContext() {
    return getRuntime().getRuntimeContext();
  }

  /**
   * Create a placement group.
   * A placement group is used to place actors according to a specific strategy
   * and resource constraints.
   * It will sends a request to GCS to preallocate the specified resources, which is asynchronous.
   * If the specified resource cannot be allocated, it will wait for the resource
   * to be updated and rescheduled.
   * This function only works when gcs actor manager is turned on.
   *
   * @param bundles Preallocated resource list.
   * @param strategy Actor placement strategy.
   * @return A handle to the created placement group.
   */
  public static PlacementGroup createPlacementGroup(List<Map<String, Double>> bundles,
      PlacementStrategy strategy) {
    return getRuntime().createPlacementGroup(bundles, strategy);
  }
}
