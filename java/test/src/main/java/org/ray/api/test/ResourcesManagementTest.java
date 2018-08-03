package org.ray.api.test;

import jdk.nashorn.internal.codegen.CompilerConstants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.*;

import java.lang.reflect.Method;
import java.util.concurrent.*;

/**
 * Resources Management Test
 */
@RunWith(MyRunner.class)
public class ResourcesManagementTest {

  @RayRemote(resources = {@ResourceItem(name = "ResourceA", value = 10),
      @ResourceItem(name = "ResourceB", value = 5)})
  public static Integer echo1(Integer number) {
    return number;
  }

  @RayRemote(resources = {@ResourceItem(name = "ResourceA", value = 10),
      @ResourceItem(name = "ResourceB", value = 10)})
  public static Integer echo2(Integer number) {
    return number;
  }

  @RayRemote(resources = {@ResourceItem(name = "ResourceA", value = 8),
      @ResourceItem(name = "ResourceB", value = 8)})
  public static class Echo1 {
    public Integer echo(Integer number) {
      return number;
    }
  }

  @RayRemote(resources = {@ResourceItem(name = "ResourceA", value = 12),
      @ResourceItem(name = "ResourceB", value = 6)})
  public static class Echo2 {
    public Integer echo(Integer number) {
      return number;
    }
  }

  @Test
  public void testMethods() {
    // This is a case that can satisfy required resources.
    RayObject<Integer> result1 = Ray.call(ResourcesManagementTest::echo1, 100);
    Assert.assertEquals(100, (int) result1.get());

    // This is a case that can't satisfy required resources.
    final RayObject<Integer> result2 = Ray.call(ResourcesManagementTest::echo2, 200);
    WaitResult<Integer> waitResult = Ray.wait(result2, 3 * 1000);

    Assert.assertEquals(0, waitResult.getReadyOnes().size());
    Assert.assertEquals(1, waitResult.getRemainOnes().size());
  }

  @Test
  public void testActors() {
    // This is a case that can satisfy required resources.
    RayActor<ResourcesManagementTest.Echo1> echo1 = Ray.create(Echo1.class);
    final RayObject<Integer> result1 = Ray.call(Echo1::echo, echo1,100);
    Assert.assertEquals(100, (int) result1.get());

    // This is a case that can't satisfy required resources.
    RayActor<ResourcesManagementTest.Echo2> echo2 = Ray.create(Echo2.class);
    final RayObject<Integer> result2 = Ray.call(Echo2::echo, echo2,100);
    WaitResult<Integer> waitResult = Ray.wait(result2, 3 * 1000);

    Assert.assertEquals(0, waitResult.getReadyOnes().size());
    Assert.assertEquals(1, waitResult.getRemainOnes().size());
  }

}

