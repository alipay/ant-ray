package io.ray.serve.util;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.BackendConfig;
import io.ray.serve.BackendVersion;
import io.ray.serve.Constants;
import io.ray.serve.RayServeException;
import io.ray.serve.generated.BackendLanguage;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.generated.LongPollResult;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import io.ray.serve.generated.UpdatedObject;
import io.ray.serve.poll.KeyType;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class ServeProtoUtil {

  private static final Gson GSON = new Gson();

  public static BackendConfig parseBackendConfig(byte[] backendConfigBytes) {

    BackendConfig backendConfig = new BackendConfig();
    if (backendConfigBytes == null) {
      return backendConfig;
    }

    io.ray.serve.generated.BackendConfig pbBackendConfig = null;
    try {
      pbBackendConfig = io.ray.serve.generated.BackendConfig.parseFrom(backendConfigBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse BackendConfig from protobuf bytes.", e);
    }

    if (pbBackendConfig == null) {
      return backendConfig;
    }

    if (pbBackendConfig.getNumReplicas() != 0) {
      backendConfig.setNumReplicas(pbBackendConfig.getNumReplicas());
    }
    if (pbBackendConfig.getMaxConcurrentQueries() != 0) {
      backendConfig.setMaxConcurrentQueries(pbBackendConfig.getMaxConcurrentQueries());
    }
    if (pbBackendConfig.getGracefulShutdownWaitLoopS() != 0) {
      backendConfig.setGracefulShutdownWaitLoopS(pbBackendConfig.getGracefulShutdownWaitLoopS());
    }
    if (pbBackendConfig.getGracefulShutdownTimeoutS() != 0) {
      backendConfig.setGracefulShutdownTimeoutS(pbBackendConfig.getGracefulShutdownTimeoutS());
    }
    backendConfig.setCrossLanguage(pbBackendConfig.getIsCrossLanguage());
    if (pbBackendConfig.getBackendLanguage() == BackendLanguage.UNRECOGNIZED) {
      throw new RayServeException(
          LogUtil.format(
              "Unrecognized backend language {}. Backend language must be in {}.",
              pbBackendConfig.getBackendLanguageValue(),
              Lists.newArrayList(BackendLanguage.values())));
    }
    backendConfig.setBackendLanguage(pbBackendConfig.getBackendLanguageValue());
    if (pbBackendConfig.getUserConfig() != null && pbBackendConfig.getUserConfig().size() != 0) {
      backendConfig.setUserConfig(
          new Object[] {
            MessagePackSerializer.decode(
                pbBackendConfig.getUserConfig().toByteArray(), Object.class)
          });
    }
    return backendConfig;
  }

  public static RequestMetadata parseRequestMetadata(byte[] requestMetadataBytes) {

    // Get a builder from RequestMetadata(bytes) or create a new one.
    RequestMetadata.Builder builder = null;
    if (requestMetadataBytes == null) {
      builder = RequestMetadata.newBuilder();
    } else {
      RequestMetadata requestMetadata = null;
      try {
        requestMetadata = RequestMetadata.parseFrom(requestMetadataBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RayServeException("Failed to parse RequestMetadata from protobuf bytes.", e);
      }
      if (requestMetadata == null) {
        builder = RequestMetadata.newBuilder();
      } else {
        builder = RequestMetadata.newBuilder(requestMetadata);
      }
    }

    // Set default values.
    if (StringUtils.isBlank(builder.getCallMethod())) {
      builder.setCallMethod(Constants.CALL_METHOD);
    }

    return builder.build();
  }

  public static RequestWrapper parseRequestWrapper(byte[] httpRequestWrapperBytes) {

    // Get a builder from HTTPRequestWrapper(bytes) or create a new one.
    RequestWrapper.Builder builder = null;
    if (httpRequestWrapperBytes == null) {
      builder = RequestWrapper.newBuilder();
    } else {
      RequestWrapper requestWrapper = null;
      try {
        requestWrapper = RequestWrapper.parseFrom(httpRequestWrapperBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RayServeException("Failed to parse RequestWrapper from protobuf bytes.", e);
      }
      if (requestWrapper == null) {
        builder = RequestWrapper.newBuilder();
      } else {
        builder = RequestWrapper.newBuilder(requestWrapper);
      }
    }

    return builder.build();
  }

  public static Map<KeyType, UpdatedObject> parseUpdatedObjects(byte[] longPollResultBytes)
      throws InvalidProtocolBufferException {
    if (longPollResultBytes == null) {
      return null;
    }
    LongPollResult longPollResult = LongPollResult.parseFrom(longPollResultBytes);
    Map<String, UpdatedObject> updatedObjects = longPollResult.getUpdatedObjectsMap();
    if (updatedObjects == null || updatedObjects.isEmpty()) {
      return null;
    }
    Map<KeyType, UpdatedObject> udpates = new HashMap<>(updatedObjects.size());
    updatedObjects.forEach(
        (key, value) -> udpates.put(ServeProtoUtil.GSON.fromJson(key, KeyType.class), value));
    return udpates;
  }

  public static Map<String, EndpointInfo> parseEndpointSet(byte[] endpointSetBytes) {
    if (endpointSetBytes == null) {
      return null;
    }
    EndpointSet endpointSet = null;
    try {
      endpointSet = EndpointSet.parseFrom(endpointSetBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse EndpointSet from protobuf bytes.", e);
    }
    if (endpointSet == null) {
      return null;
    }
    return endpointSet.getEndpointsMap();
  }

  public static BackendVersion parseBackendVersion(byte[] backendVersionBytes) {
    BackendVersion backendVersion = new BackendVersion();
    if (backendVersionBytes == null) {
      return backendVersion;
    }

    io.ray.serve.generated.BackendVersion pbBackendVersion = null;
    try {
      pbBackendVersion = io.ray.serve.generated.BackendVersion.parseFrom(backendVersionBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse BackendVersion from protobuf bytes.", e);
    }

    if (pbBackendVersion == null) {
      return backendVersion;
    }

    if (StringUtils.isNotBlank(pbBackendVersion.getCodeVersion())) {
      backendVersion.setCodeVersion(pbBackendVersion.getCodeVersion());
    }
    if (pbBackendVersion.getUserConfig() != null && pbBackendVersion.getUserConfig().size() != 0) {
      backendVersion.setUserConfig(
          new Object[] {
            MessagePackSerializer.decode(
                pbBackendVersion.getUserConfig().toByteArray(), Object.class)
          });
    }
    return backendVersion;
  }

  public static io.ray.serve.generated.BackendVersion toProtobuf(BackendVersion backendVersion) {
    io.ray.serve.generated.BackendVersion.Builder pbBackendVersion =
        io.ray.serve.generated.BackendVersion.newBuilder();
    if (backendVersion == null) {
      return pbBackendVersion.build();
    }

    if (StringUtils.isNotBlank(backendVersion.getCodeVersion())) {
      pbBackendVersion.setCodeVersion(backendVersion.getCodeVersion());
    }
    if (backendVersion.getUserConfig() != null) {
      pbBackendVersion.setUserConfig(
          ByteString.copyFrom(
              MessagePackSerializer.encode(backendVersion.getUserConfig()).getLeft()));
    }
    return pbBackendVersion.build();
  }
}
