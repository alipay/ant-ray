// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class io_ray_runtime_RayNativeRuntime */

#ifndef _Included_io_ray_runtime_RayNativeRuntime
#define _Included_io_ray_runtime_RayNativeRuntime
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeInitCoreWorker
 * Signature:
 * (ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;I[BLorg/ray/runtime/gcs/GcsClientOptions;)J
 */
JNIEXPORT jlong JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeInitCoreWorker(
    JNIEnv *, jclass, jint, jstring, jstring, jstring, jint, jbyteArray, jobject);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeRunTaskExecutor
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeRunTaskExecutor(JNIEnv *, jclass, jlong);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeDestroyCoreWorker
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_ray_runtime_RayNativeRuntime_nativeDestroyCoreWorker(JNIEnv *, jclass, jlong);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeSetup
 * Signature: (Ljava/lang/String;Ljava/util/Map;)V
 */
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeSetup(JNIEnv *, jclass,
                                                                         jstring,
                                                                         jobject);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeShutdownHook
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeShutdownHook(JNIEnv *,
                                                                                jclass);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeSetResource
 * Signature: (JLjava/lang/String;D[B)V
 */
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeSetResource(
    JNIEnv *, jclass, jlong, jstring, jdouble, jbyteArray);

/*
 * Class:     io_ray_runtime_RayNativeRuntime
 * Method:    nativeKillActor
 * Signature: (J[BZ)V
 */
JNIEXPORT void JNICALL Java_io_ray_runtime_RayNativeRuntime_nativeKillActor(
    JNIEnv *, jclass, jlong, jbyteArray, jboolean);

#ifdef __cplusplus
}
#endif
#endif
