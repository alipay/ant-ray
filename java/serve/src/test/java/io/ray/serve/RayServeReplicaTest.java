package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.BackendLanguage;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import java.io.IOException;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeReplicaTest {

  @SuppressWarnings("unused")
  @Test
  public void test() throws IOException {
    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String controllerName = "RayServeReplicaTest";
      String backendTag = "b_tag";
      String replicaTag = "r_tag";
      String version = "v1";

      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      BackendConfig backendConfig =
          new BackendConfig().setBackendLanguage(BackendLanguage.JAVA.getNumber());
      DeploymentInfo deploymentInfo =
          new DeploymentInfo()
              .setName(backendTag)
              .setBackendConfig(backendConfig)
              .setDeploymentVersion(new DeploymentVersion(version))
              .setBackendDef(DummyBackendReplica.class.getName());

      ActorHandle<RayServeWrappedReplica> backendHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  deploymentInfo,
                  replicaTag,
                  controllerName,
                  (RayServeConfig) null)
              .remote();

      // ready
      Assert.assertTrue(backendHandle.task(RayServeWrappedReplica::checkHealth).remote().get());

      // handle request
      RequestMetadata.Builder requestMetadata = RequestMetadata.newBuilder();
      requestMetadata.setRequestId(RandomStringUtils.randomAlphabetic(10));
      requestMetadata.setCallMethod(Constants.CALL_METHOD);
      RequestWrapper.Builder requestWrapper = RequestWrapper.newBuilder();

      ObjectRef<Object> resultRef =
          backendHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  requestWrapper.build().toByteArray())
              .remote();
      Assert.assertEquals((String) resultRef.get(), "1");

      // reconfigure
      ObjectRef<Object> versionRef =
          backendHandle.task(RayServeWrappedReplica::reconfigure, (Object) null).remote();
      Assert.assertEquals(((DeploymentVersion) versionRef.get()).getCodeVersion(), version);

      backendHandle.task(RayServeWrappedReplica::reconfigure, new Object()).remote().get();
      resultRef =
          backendHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  requestWrapper.build().toByteArray())
              .remote();
      Assert.assertEquals((String) resultRef.get(), "1");

      backendHandle
          .task(RayServeWrappedReplica::reconfigure, ImmutableMap.of("value", "100"))
          .remote()
          .get();
      resultRef =
          backendHandle
              .task(
                  RayServeWrappedReplica::handleRequest,
                  requestMetadata.build().toByteArray(),
                  requestWrapper.build().toByteArray())
              .remote();
      Assert.assertEquals((String) resultRef.get(), "101");

      // get version
      versionRef = backendHandle.task(RayServeWrappedReplica::getVersion).remote();
      Assert.assertEquals(((DeploymentVersion) versionRef.get()).getCodeVersion(), version);

      // prepare for shutdown
      ObjectRef<Boolean> shutdownRef =
          backendHandle.task(RayServeWrappedReplica::prepareForShutdown).remote();
      Assert.assertTrue(shutdownRef.get());

    } finally {
      if (!inited) {
        Ray.shutdown();
      }
      Serve.setInternalReplicaContext(null);
    }
  }
}
