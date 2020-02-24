package org.ray.streaming.state.backend;

import java.util.Arrays;
import java.util.HashMap;
import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.MapState;
import org.ray.streaming.state.keystate.state.ValueState;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by eagle on 2019/8/15.
 */
public class KeyStateBackendTest {

  private AbstractStateBackend stateBackend;
  private KeyStateBackend keyStateBackend;

  public void testGetValueState() {
    keyStateBackend.setBatchId(1l);
    ValueStateDescriptor<String> valueStateDescriptor = ValueStateDescriptor
      .build("value", String.class, null);
    valueStateDescriptor.setTableName("kepler_hlg_ut");
    ValueState<String> valueState = keyStateBackend.getValueState(valueStateDescriptor);

    valueState.setCurrentKey("1");
    valueState.update("hello");
    Assert.assertEquals(valueState.get(), "hello");

    valueState.update("hello1");
    Assert.assertEquals(valueState.get(), "hello1");

    valueState.setCurrentKey("2");
    Assert.assertEquals(valueState.get(), null);

    valueState.update("eagle");
    Assert.assertEquals(valueState.get(), "eagle");

    keyStateBackend.rollBack(1);
    valueState.setCurrentKey("1");
    Assert.assertEquals(valueState.get(), null);
    valueState.setCurrentKey("2");
    Assert.assertEquals(valueState.get(), null);

    valueState.setCurrentKey("1");
    valueState.update("eagle");
    keyStateBackend.finish(1);

    keyStateBackend.setBatchId(2);
    valueState.setCurrentKey("2");
    valueState.update("tim");

    valueState.setCurrentKey("2-1");
    valueState.update("jim");
    keyStateBackend.finish(2);

    keyStateBackend.setBatchId(3);
    valueState.setCurrentKey("3");
    valueState.update("lucy");
    keyStateBackend.finish(3);

    keyStateBackend.setBatchId(4);
    valueState.setCurrentKey("4");
    valueState.update("eric");
    keyStateBackend.finish(4);

    keyStateBackend.setBatchId(5);
    valueState.setCurrentKey("4");
    valueState.update("eric-1");
    valueState.setCurrentKey("5");
    valueState.update("jack");
    keyStateBackend.finish(5);
    keyStateBackend.commit(5, null);
    keyStateBackend.ackCommit(5, System.currentTimeMillis());

    keyStateBackend.setBatchId(6);
    valueState.setCurrentKey("5");
    Assert.assertEquals(valueState.get(), "jack");

    valueState.setCurrentKey("4");
    Assert.assertEquals(valueState.get(), "eric-1");

    valueState.setCurrentKey(4);
    valueState.update("if-ttt");
    Assert.assertEquals(valueState.get(), "if-ttt");

    keyStateBackend.setBatchId(7);
    valueState.setCurrentKey(9);
    valueState.update("6666");

    keyStateBackend.rollBack(5);
    keyStateBackend.setBatchId(6);
    valueState.setCurrentKey("4");
    Assert.assertEquals(valueState.get(), "eric-1");
    valueState.setCurrentKey("5");
    Assert.assertEquals(valueState.get(), "jack");
    valueState.setCurrentKey("9");
    Assert.assertEquals(valueState.get(), null);

  }

  public void testGetListState() {
    keyStateBackend.setBatchId(1l);
    ListStateDescriptor<String> listStateDescriptor = ListStateDescriptor
      .build("list", String.class);
    listStateDescriptor.setTableName("kepler_hlg_ut");
    ListState<String> listState = keyStateBackend.getListState(listStateDescriptor);

    listState.setCurrentKey("1");
    listState.add("hello1");
    Assert.assertEquals(listState.get(), Arrays.asList("hello1"));

    listState.add("hello2");
    Assert.assertEquals(listState.get(), Arrays.asList("hello1", "hello2"));

    listState.setCurrentKey("2");
    Assert.assertEquals(listState.get(), Arrays.asList());

    listState.setCurrentKey("2");
    listState.add("eagle");
    listState.setCurrentKey("1");
    Assert.assertEquals(listState.get(), Arrays.asList("hello1", "hello2"));
    listState.setCurrentKey("2");
    Assert.assertEquals(listState.get(), Arrays.asList("eagle"));

    keyStateBackend.rollBack(1);
    listState.setCurrentKey("1");
    Assert.assertEquals(listState.get(), Arrays.asList());
    listState.setCurrentKey("2");
    Assert.assertEquals(listState.get(), Arrays.asList());

    listState.setCurrentKey("1");
    listState.add("eagle");
    listState.add("eagle-2");
    keyStateBackend.finish(1);

    keyStateBackend.setBatchId(2);
    listState.setCurrentKey("2");
    listState.add("tim");

    listState.setCurrentKey("2-1");
    listState.add("jim");
    keyStateBackend.finish(2);

    keyStateBackend.setBatchId(3);
    listState.setCurrentKey("3");
    listState.add("lucy");
    keyStateBackend.finish(3);

    keyStateBackend.setBatchId(4);
    listState.setCurrentKey("4");
    listState.add("eric");
    keyStateBackend.finish(4);

    keyStateBackend.setBatchId(5);
    listState.setCurrentKey("4");
    listState.add("eric-1");
    Assert.assertEquals(listState.get(), Arrays.asList("eric", "eric-1"));

    listState.setCurrentKey("5");
    listState.add("jack");
    keyStateBackend.finish(5);
    keyStateBackend.commit(5, null);
    keyStateBackend.ackCommit(5, System.currentTimeMillis());

    keyStateBackend.setBatchId(6);
    listState.setCurrentKey("5");
    Assert.assertEquals(listState.get(), Arrays.asList("jack"));

    listState.setCurrentKey("4");
    Assert.assertEquals(listState.get(), Arrays.asList("eric", "eric-1"));

    listState.setCurrentKey(4);
    listState.add("if-ttt");
    Assert.assertEquals(listState.get(), Arrays.asList("eric", "eric-1", "if-ttt"));

    keyStateBackend.setBatchId(7);
    listState.setCurrentKey(9);
    listState.add("6666");

    keyStateBackend.rollBack(5);
    keyStateBackend.setBatchId(6);
    listState.setCurrentKey("4");
    Assert.assertEquals(listState.get(), Arrays.asList("eric", "eric-1"));
    listState.setCurrentKey("5");
    Assert.assertEquals(listState.get(), Arrays.asList("jack"));
    listState.setCurrentKey("9");
    Assert.assertEquals(listState.get(), Arrays.asList());
  }

  public void testGetMapState() {
    keyStateBackend.setBatchId(1l);
    MapStateDescriptor<String, String> mapStateDescriptor = MapStateDescriptor
      .build("map", String.class, String.class);
    mapStateDescriptor.setTableName("kepler_hlg_ut");
    MapState<String, String> mapState = keyStateBackend.getMapState(mapStateDescriptor);

    mapState.setCurrentKey("1");
    mapState.put("hello1", "world1");
    Assert.assertEquals(mapState.get("hello1"), "world1");

    mapState.put("hello2", "world2");
    Assert.assertEquals(mapState.get("hello2"), "world2");
    Assert.assertEquals(mapState.get("hello1"), "world1");
    Assert.assertEquals(mapState.get("hello3"), null);

    mapState.setCurrentKey("2");
    //Assert.assertEquals(mapState.iterator(), (new HashMap()));

    mapState.setCurrentKey("2");
    mapState.put("eagle", "eagle-1");
    mapState.setCurrentKey("1");
    Assert.assertEquals(mapState.get("hello1"), "world1");
    mapState.setCurrentKey("2");
    Assert.assertEquals(mapState.get("eagle"), "eagle-1");
    Assert.assertEquals(mapState.get("xxx"), null);

    keyStateBackend.rollBack(1);
    mapState.setCurrentKey("1");
    Assert.assertEquals(mapState.iterator(), (new HashMap()).entrySet().iterator());
    mapState.setCurrentKey("2");
    Assert.assertEquals(mapState.iterator(), (new HashMap()).entrySet().iterator());

    mapState.setCurrentKey("1");
    mapState.put("eagle", "eagle-1");
    mapState.put("eagle-2", "eagle-3");
    keyStateBackend.finish(1);

    keyStateBackend.setBatchId(2);
    mapState.setCurrentKey("2");
    mapState.put("tim", "tina");

    mapState.setCurrentKey("2-1");
    mapState.put("jim", "tick");
    keyStateBackend.finish(2);

    keyStateBackend.setBatchId(3);
    mapState.setCurrentKey("3");
    mapState.put("lucy", "ja");
    keyStateBackend.finish(3);

    keyStateBackend.setBatchId(4);
    mapState.setCurrentKey("4");
    mapState.put("eric", "sam");
    keyStateBackend.finish(4);

    keyStateBackend.setBatchId(5);
    mapState.setCurrentKey("4");
    mapState.put("eric-1", "zxy");
    Assert.assertEquals(mapState.get("eric-1"), "zxy");
    Assert.assertEquals(mapState.get("eric"), "sam");

    mapState.setCurrentKey("5");
    mapState.put("jack", "zhang");
    keyStateBackend.finish(5);
    keyStateBackend.commit(5, null);
    keyStateBackend.ackCommit(5, System.currentTimeMillis());

    keyStateBackend.setBatchId(6);
    mapState.setCurrentKey("5");
    Assert.assertEquals(mapState.get("jack"), "zhang");
    mapState.put("hlll", "gggg");

    mapState.setCurrentKey("4");
    Assert.assertEquals(mapState.get("eric-1"), "zxy");
    Assert.assertEquals(mapState.get("eric"), "sam");

    mapState.setCurrentKey(4);
    mapState.put("if-ttt", "if-ggg");
    Assert.assertEquals(mapState.get("if-ttt"), "if-ggg");

    keyStateBackend.setBatchId(7);
    mapState.setCurrentKey(9);
    mapState.put("6666", "7777");

    keyStateBackend.rollBack(5);
    keyStateBackend.setBatchId(6);
    mapState.setCurrentKey("4");
    Assert.assertEquals(mapState.get("eric-1"), "zxy");
    Assert.assertEquals(mapState.get("eric"), "sam");
    Assert.assertEquals(mapState.get("if-ttt"), null);

    mapState.setCurrentKey("5");
    Assert.assertEquals(mapState.get("hlll"), null);
    mapState.setCurrentKey("9");
    Assert.assertEquals(mapState.get("6666"), null);
  }


  @Test
  public void testMem() {
    stateBackend = AbstractStateBackend.buildStateBackend(new HashMap<String, String>());
    keyStateBackend = stateBackend.createKeyedStateBackend("id", 4, new KeyGroup(2, 3));
    testGetValueState();
    testGetListState();
    testGetMapState();
  }

}