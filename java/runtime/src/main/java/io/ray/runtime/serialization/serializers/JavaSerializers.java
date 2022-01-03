package io.ray.runtime.serialization.serializers;

import com.google.common.base.Preconditions;
import io.ray.runtime.io.ClassLoaderObjectInputStream;
import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.io.Platform;
import io.ray.runtime.io.SerdeObjectInput;
import io.ray.runtime.io.SerdeObjectOutput;
import io.ray.runtime.serialization.RaySerde;
import io.ray.runtime.serialization.util.LoggerFactory;
import io.ray.runtime.serialization.util.ReflectionUtils;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;

@SuppressWarnings( {"unchecked", "rawtypes"})
public class JavaSerializers {
  /**
   * Serializes objects using Java's built in serialization to be compatible with java
   * serialization. This is very inefficient and should be avoided if possible. User can call {@link
   * RaySerde#registerSerializer} to avoid this.
   *
   * <p>When a serializer not found and {@link JavaSerializer#requireJavaSerialization(Class)}
   * return true, this serializer will be used.
   */
  public static class JavaSerializer extends Serializer {
    private static final Logger LOG = LoggerFactory.getLogger(JavaSerializer.class);
    private final SerdeObjectInput objectInput;
    private final SerdeObjectOutput objectOutput;

    public JavaSerializer(RaySerde raySerDe, Class<?> cls) {
      super(raySerDe, cls);
      Preconditions.checkArgument(requireJavaSerialization(cls));
      LOG.warn(
        "{} use java built-in serialization, which is inefficient. "
          + "Please replace it with a {} or implements {}",
        cls,
        Serializer.class.getCanonicalName(),
        Externalizable.class.getCanonicalName());
      objectInput = new SerdeObjectInput(raySerDe, null);
      objectOutput = new SerdeObjectOutput(raySerDe, null);
    }

    /**
     * Returns true if a class satisfy following requirements.
     * <li>implements {@link Serializable}
     * <li>is not an {@link Enum}
     * <li>is not an array
     * <li>has {@code readResolve}/{@code writePlace} method or class has {@code readObject}/{@code
     * writeObject} method, but doesn't implements {@link Externalizable}
     * <li/>
     */
    public static boolean requireJavaSerialization(Class<?> clz) {
      if (clz.isEnum() || clz.isArray()) {
        return false;
      }
      if (isDynamicGeneratedCLass(clz)) {
        // use corresponding serializer.
        return false;
      }
      if (Externalizable.class.isAssignableFrom(clz)) {
        return false;
      }
      if (getWriteReplaceMethod(clz) != null || getReadResolveMethod(clz) != null ||
        getReadObjectMethod(clz) != null || getWriteObjectMethod(clz) != null) {
        Preconditions.checkArgument(
          ReflectionUtils.getAllInterfaces(clz).contains(Serializable.class));
        return true;
      }
      return false;
    }

    public static Class<? extends Serializer> getJavaSerializer(Class<?> clz) {
      return JavaSerializer.class;
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {
      try {
        objectOutput.setBuffer(buffer);
        ObjectOutputStream objectOutputStream =
          (ObjectOutputStream) raySerDe.getSerializationContext().get(objectOutput);
        if (objectOutputStream == null) {
          objectOutputStream = new ObjectOutputStream(objectOutput);
          raySerDe.getSerializationContext().add(objectOutput, objectOutputStream);
        }
        objectOutputStream.writeObject(value);
        objectOutputStream.flush();
      } catch (IOException e) {
        Platform.throwException(e);
      }
    }

    @Override
    public Object read(MemoryBuffer buffer) {
      try {
        objectInput.setBuffer(buffer);
        ObjectInputStream objectInputStream =
          (ObjectInputStream) raySerDe.getSerializationContext().get(objectInput);
        if (objectInputStream == null) {
          objectInputStream =
            new ClassLoaderObjectInputStream(raySerDe.getClassLoader(), objectInput);
          raySerDe.getSerializationContext().add(objectInput, objectInputStream);
        }
        return objectInputStream.readObject();
      } catch (IOException | ClassNotFoundException e) {
        Platform.throwException(e);
      }
      throw new IllegalStateException("unreachable code");
    }
  }

  public static class JavaCompatibleSerializer extends Serializer {
    private final Method writeObjectMethod;
    private final Method readObjectMethod;

    public JavaCompatibleSerializer(RaySerde raySerDe, Class cls) {
      super(raySerDe, cls);
      writeObjectMethod = getWriteObjectMethod(cls);
      writeObjectMethod.setAccessible(true);
      readObjectMethod = getReadObjectMethod(cls);
      readObjectMethod.setAccessible(true);
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {

    }

    @Override
    public Object read(MemoryBuffer buffer) {

      return null;
    }
  }

  public static class ReplacementSerializer extends Serializer {
    private final Method writeReplaceMethod;
    private final Method readResolveMethod;

    public ReplacementSerializer(RaySerde raySerDe, Class cls) {
      super(raySerDe, cls);
      writeReplaceMethod = Objects.requireNonNull(getWriteReplaceMethod(cls));
      writeReplaceMethod.setAccessible(true);
      readResolveMethod = Objects.requireNonNull(getReadResolveMethod(cls));
      readResolveMethod.setAccessible(true);
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {
      try {
        Object replacement = writeReplaceMethod.invoke(value);
        raySerDe.serializeReferencableToJava(buffer, replacement);
      } catch (IllegalAccessException | InvocationTargetException e) {
        Platform.throwException(e);
      }
    }

    @Override
    public Object read(MemoryBuffer buffer) {
      Object replacement = raySerDe.deserializeNonReferenceFromJava(buffer);
      if (readResolveMethod != null) {
        try {
          readResolveMethod.invoke(replacement);
        } catch (IllegalAccessException | InvocationTargetException e) {
          Platform.throwException(e);
        }
      } else {
        return replacement;
      }
      throw new IllegalStateException("unreachable");
    }
  }

  public static class LambdaSerializer extends Serializer {
    private static final Class SERIALIZED_LAMBDA = java.lang.invoke.SerializedLambda.class;

    @SuppressWarnings("unchecked")
    public LambdaSerializer(RaySerde raySerDe, Class cls) {
      super(raySerDe, cls);
      if (cls != ReplaceStub.class) {
        if (!Serializable.class.isAssignableFrom(cls)) {
          String msg =
            String.format("Lambda needs to implement %s for serialization", Serializable.class);
          throw new UnsupportedOperationException(msg);
        }
      }
    }

    /**
     * Returns true if the specified class is a lambda.
     */
    public static boolean isLambda(Class clz) {
      Preconditions.checkNotNull(clz);
      return clz.getName().indexOf('/') >= 0;
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {
      try {
        Method writeReplaceMethod = getWriteReplaceMethod(value.getClass());
        Objects.requireNonNull(writeReplaceMethod);
        writeReplaceMethod.setAccessible(true);
        Object replacement = writeReplaceMethod.invoke(value);
        Preconditions.checkArgument(SERIALIZED_LAMBDA.isInstance(replacement));
        raySerDe.serializeReferencableToJava(buffer, replacement);
      } catch (Exception e) {
        throw new RuntimeException("Can't serialize lambda " + value, e);
      }
    }

    @Override
    public Object read(MemoryBuffer buffer) {
      Preconditions.checkArgument(cls == ReplaceStub.class);
      try {
        return raySerDe.deserializeReferencableFromJava(buffer);
      } catch (Exception e) {
        throw new RuntimeException("Can't deserialize lambda", e);
      }
    }

    /**
     * Class name of dynamic generated class is not fixed, so we use a stub class to mock dynamic
     * class.
     */
    public static class ReplaceStub {
    }
  }

  private static Method getWriteObjectMethod(Class<?> clz) {
    Method writeObject = getMethod(clz, "writeObject");
    if (writeObject != null) {
      if (writeObject.getParameterTypes().length == 1
        && writeObject.getParameterTypes()[0] == ObjectOutputStream.class
        && writeObject.getReturnType() == void.class
        && Modifier.isPrivate(writeObject.getModifiers())) {
        return writeObject;
      }
    }
    return null;
  }

  private static Method getReadObjectMethod(Class<?> clz) {
    Method readObject = getMethod(clz, "readObject");
    if (readObject != null) {
      if (readObject.getParameterTypes().length == 1
        && readObject.getParameterTypes()[0] == ObjectInputStream.class
        && readObject.getReturnType() == void.class
        && Modifier.isPrivate(readObject.getModifiers())) {
        return readObject;
      }
    }
    return null;
  }

  private static Method getReadResolveMethod(Class<?> clz) {
    Method readResolve = getMethod(clz, "readResolve");
    if (readResolve != null) {
      if (readResolve.getParameterTypes().length == 0
        && readResolve.getReturnType() == Object.class) {
        return readResolve;
      }
    }
    return null;
  }

  private static Method getWriteReplaceMethod(Class<?> clz) {
    Method writeReplace = getMethod(clz, "writeReplace");
    if (writeReplace != null) {
      if (writeReplace.getParameterTypes().length == 0
        && writeReplace.getReturnType() == Object.class) {
        return writeReplace;
      }
    }
    return null;
  }

  // Child class method lookup order. See more on `java.io.ObjectStreamClass.getInheritableMethod`.
  private static Method getMethod(Class<?> clz, String methodName) {
    Class<?> cls = clz;
    while (cls != null) {
      for (Method method : cls.getDeclaredMethods()) {
        if (method.getName().equals(methodName)) {
          return method;
        }
      }
      cls = cls.getSuperclass();
    }
    return null;
  }

  public static boolean isDynamicGeneratedCLass(Class<?> cls) {
    // TODO(mubai) add cglib check
    return LambdaSerializer.isLambda(cls) || JdkProxySerializer.isJdkProxy(cls);
  }

  public static class JdkProxySerializer extends Serializer {

    @SuppressWarnings("unchecked")
    public JdkProxySerializer(RaySerde raySerDe, Class cls) {
      super(raySerDe, cls);
      if (cls != ReplaceStub.class) {
        Preconditions.checkArgument(isJdkProxy(cls), "Require a jdk proxy class");
      }
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {
      raySerDe.serializeReferencableToJava(buffer, Proxy.getInvocationHandler(value));
      raySerDe.serializeReferencableToJava(buffer, value.getClass().getInterfaces());
    }

    @Override
    public Object read(MemoryBuffer buffer) {
      Preconditions.checkArgument(cls == ReplaceStub.class);
      InvocationHandler invocationHandler =
        (InvocationHandler) raySerDe.deserializeReferencableFromJava(buffer);
      Preconditions.checkNotNull(invocationHandler);
      final Class<?>[] interfaces = (Class<?>[]) raySerDe.deserializeReferencableFromJava(buffer);
      Preconditions.checkNotNull(interfaces);
      return Proxy.newProxyInstance(raySerDe.getClassLoader(), interfaces, invocationHandler);
    }

    public static boolean isJdkProxy(Class<?> clz) {
      return Proxy.isProxyClass(clz);
    }

    public static class ReplaceStub {
    }
  }

  public static final class CollectionJavaSerializer<T extends Collection> extends Serializer<T> {
    private final JavaSerializer javaSerializer;

    public CollectionJavaSerializer(RaySerde raySerDe, Class<T> cls) {
      super(raySerDe, cls);
      javaSerializer = new JavaSerializer(raySerDe, cls);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(MemoryBuffer buffer) {
      return (T) javaSerializer.read(buffer);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      javaSerializer.write(buffer, value);
    }
  }

  public static class MapJavaSerializer<T extends Map> extends Serializer<T> {
    private final JavaSerializer javaSerializer;

    public MapJavaSerializer(RaySerde raySerDe, Class<T> cls) {
      super(raySerDe, cls);
      javaSerializer = new JavaSerializer(raySerDe, cls);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(MemoryBuffer buffer) {
      return (T) javaSerializer.read(buffer);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      javaSerializer.write(buffer, value);
    }
  }
}
