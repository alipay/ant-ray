package org.ray.runtime.serializer;

import org.nustaq.serialization.FSTConfiguration;
import org.ray.runtime.actor.NativeRayActor;
import org.ray.runtime.actor.NativeRayActorSerializer;

/**
 * Java object serialization TODO: use others (e.g. Arrow) for higher performance
 */
public class FSTSerializer {

  private static final ThreadLocal<FSTConfiguration> conf = ThreadLocal.withInitial(() -> {
    FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
    conf.registerSerializer(NativeRayActor.class, new NativeRayActorSerializer(), true);
    return conf;
  });


  public static byte[] encode(Object obj, Serializer.Meta meta, ClassLoader classLoader) {
    byte[] result;
    FSTConfiguration current = conf.get();
    if (classLoader != null && classLoader != current.getClassLoader()) {
      ClassLoader old = current.getClassLoader();
      current.setClassLoader(classLoader);
      result = current.asByteArray(obj);
      current.setClassLoader(old);
    } else {
      result = current.asByteArray(obj);
    }
    meta.isCrossLanguage = false;
    return result;
  }


  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, ClassLoader classLoader) {
    Object object;
    FSTConfiguration current = conf.get();
    if (classLoader != null && classLoader != current.getClassLoader()) {
      ClassLoader old = current.getClassLoader();
      current.setClassLoader(classLoader);
      object = current.asObject(bs);
      current.setClassLoader(old);
    } else {
      object = current.asObject(bs);
    }
    return (T) object;
  }

  public static void setClassloader(ClassLoader classLoader) {
    conf.get().setClassLoader(classLoader);
  }
}
