package org.ray.streaming.state.keystate.state.impl;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.backend.impl.MemoryStateBackend;
import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author wutao on 2019/8/18.
 */
public class MapStateImplTest {

  MapStateImpl<Integer, String> mapState;
  KeyStateBackend keyStateBackend;

  @BeforeClass
  public void setUp() throws Exception {
    keyStateBackend = new KeyStateBackend(1, new KeyGroup(1, 2),
      new MemoryStateBackend(new HashMap<>()));
    MapStateDescriptor<Integer, String> descriptor = MapStateDescriptor
      .build("MapStateImplTest", Integer.class, String.class);
    descriptor.setTableName("table");
    mapState = (MapStateImpl<Integer, String>) keyStateBackend.getMapState(descriptor);
  }

  @Test
  public void testPuTGet() throws Exception {
    keyStateBackend.setContext(1L, 1);

    Assert.assertEquals(mapState.get().size(), 0);

    mapState.put(1, "1");
    mapState.put(2, "2");

    Assert.assertTrue(mapState.contains(1));
    Assert.assertTrue(mapState.contains(2));
    Assert.assertFalse(mapState.contains(3));

    Assert.assertEquals("1", mapState.get(1));
    Assert.assertEquals("2", mapState.get(2));

    mapState.remove(1);
    Assert.assertFalse(mapState.contains(1));
    Assert.assertTrue(mapState.contains(2));

    mapState.remove(2);
    mapState.putAll(ImmutableMap.of(1, "1", 2, "2"));
    Assert.assertEquals("1", mapState.get(1));
    Assert.assertEquals("2", mapState.get(2));
  }

  @Test(dependsOnMethods = {"testPuTGet"})
  public void testUpdate() throws Exception {
    Assert.assertEquals(mapState.get().size(), 2);

    mapState.update(ImmutableMap.of(3, "3", 4, "4"));
    Assert.assertEquals(mapState.get(3), "3");
    Assert.assertEquals(mapState.get(4), "4");
    Assert.assertEquals(mapState.get().size(), 2);

    Map<Integer, String> map = ImmutableMap.of(5, "5", 6, "6");
    mapState.update(map);
    Assert.assertEquals(mapState.get(), map);

    mapState.update(null);
    Assert.assertEquals(mapState.get().size(), 0);
  }


  @Test
  public void testFailover() throws Exception {
    keyStateBackend.setContext(1L, 1);

    Assert.assertEquals(mapState.get().size(), 0);

    mapState.put(1, "1");
    mapState.put(2, "2");

    Assert.assertTrue(mapState.contains(1));
    Assert.assertTrue(mapState.contains(2));
    Assert.assertFalse(mapState.contains(3));

    Assert.assertEquals("1", mapState.get(1));
    Assert.assertEquals("2", mapState.get(2));

    mapState.remove(1);
    Assert.assertFalse(mapState.contains(1));
    Assert.assertTrue(mapState.contains(2));

    mapState.remove(2);
    mapState.putAll(ImmutableMap.of(1, "1", 2, "2"));
    Assert.assertEquals("1", mapState.get(1));
    Assert.assertEquals("2", mapState.get(2));

    keyStateBackend.finish(5);

    Assert.assertEquals("1", mapState.get(1));
    Assert.assertEquals("2", mapState.get(2));

    mapState.put(2, "3");
    Assert.assertEquals("3", mapState.get(2));

    keyStateBackend.finish(6);

    keyStateBackend.commit(5, "");
    Assert.assertEquals("3", mapState.get(2));

    keyStateBackend.commit(6, "");
    keyStateBackend.ackCommit(5, 0);
    keyStateBackend.ackCommit(6, 1);

    Assert.assertEquals("1", mapState.get(1));
    mapState.put(2, "5");
    Assert.assertEquals("5", mapState.get(2));
    mapState.update(null);
  }
}