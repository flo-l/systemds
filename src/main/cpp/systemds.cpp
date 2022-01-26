/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifdef _WIN32
#include <winsock.h>
#else
#include <arpa/inet.h>
#endif

#include "common.h"
#include "libmatrixdnn.h"
#include "libmatrixmult.h"
#include "libhe.h"
#include "systemds.h"

// Results from Matrix-vector/vector-matrix 1M x 1K, dense show that GetDoubleArrayElements creates a copy on OpenJDK.

// Logic:
// 1. We chose GetDoubleArrayElements over GetPrimitiveArrayCritical in a multi-threaded scenario. This avoids any potential OOM related to GC halts.
// 2. For input array, we don't copy back the array using JNI_ABORT.

// JNI Methods to get/release double*
#define GET_DOUBLE_ARRAY(env, input, numThreads) \
	((double*)env->GetPrimitiveArrayCritical(input, NULL))
// ( maxThreads != -1 && ((int)numThreads) == maxThreads ? ((double*)env->GetPrimitiveArrayCritical(input, NULL)) :  env->GetDoubleArrayElements(input,NULL) )
 
// ------------------------------------------------------------------- 
// From: https://developer.android.com/training/articles/perf-jni.html
// 0
// Actual: the array object is un-pinned.
// Copy: data is copied back. The buffer with the copy is freed.
// JNI_ABORT
// Actual: the array object is un-pinned. Earlier writes are not aborted.
// Copy: the buffer with the copy is freed; any changes to it are lost.
#define RELEASE_INPUT_ARRAY(env, input, inputPtr, numThreads) \
	env->ReleasePrimitiveArrayCritical(input, inputPtr, JNI_ABORT)
// ( maxThreads != -1 && ((int)numThreads) == maxThreads ? env->ReleasePrimitiveArrayCritical(input, inputPtr, JNI_ABORT) : env->ReleaseDoubleArrayElements(input, inputPtr, JNI_ABORT) )

#define RELEASE_ARRAY(env, input, inputPtr, numThreads) \
	env->ReleasePrimitiveArrayCritical(input, inputPtr, 0)
// ( maxThreads != -1 && ((int)numThreads) == maxThreads ? env->ReleasePrimitiveArrayCritical(input, inputPtr, 0) :  env->ReleaseDoubleArrayElements(input, inputPtr, 0) )
  
// -------------------------------------------------------------------
JNIEXPORT void JNICALL Java_org_apache_sysds_utils_NativeHelper_setMaxNumThreads
  (JNIEnv *, jclass, jint jmaxThreads) {
	setNumThreadsForBLAS(jmaxThreads);
}

JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_dmmdd(
    JNIEnv* env, jclass cls, jdoubleArray m1, jdoubleArray m2, jdoubleArray ret,
    jint m1rlen, jint m1clen, jint m2clen, jint numThreads)
{
  double* m1Ptr = GET_DOUBLE_ARRAY(env, m1, numThreads);
  double* m2Ptr = GET_DOUBLE_ARRAY(env, m2, numThreads);
  double* retPtr = GET_DOUBLE_ARRAY(env, ret, numThreads);
  if(m1Ptr == NULL || m2Ptr == NULL || retPtr == NULL)
    return -1;

  dmatmult(m1Ptr, m2Ptr, retPtr, (int)m1rlen, (int)m1clen, (int)m2clen, (int)numThreads);
  size_t nnz = computeNNZ<double>(retPtr, m1rlen * m2clen);

  RELEASE_INPUT_ARRAY(env, m1, m1Ptr, numThreads);
  RELEASE_INPUT_ARRAY(env, m2, m2Ptr, numThreads);
  RELEASE_ARRAY(env, ret, retPtr, numThreads); 

  return static_cast<jlong>(nnz);
}

JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_smmdd(
    JNIEnv* env, jclass cls, jobject m1, jobject m2, jobject ret,
    jint m1rlen, jint m1clen, jint m2clen, jint numThreads)
{
  float* m1Ptr = (float*) env->GetDirectBufferAddress(m1);
  float* m2Ptr = (float*) env->GetDirectBufferAddress(m2);
  float* retPtr = (float*) env->GetDirectBufferAddress(ret);
  if(m1Ptr == NULL || m2Ptr == NULL || retPtr == NULL)
    return -1;

  smatmult(m1Ptr, m2Ptr, retPtr, (int)m1rlen, (int)m1clen, (int)m2clen, (int)numThreads);

  return static_cast<jlong>(computeNNZ<float>(retPtr, m1rlen * m2clen));
}

JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_tsmm
  (JNIEnv * env, jclass cls, jdoubleArray m1, jdoubleArray ret, jint m1rlen, jint m1clen, jboolean leftTrans, jint numThreads) {
  double* m1Ptr = GET_DOUBLE_ARRAY(env, m1, numThreads);
  double* retPtr = GET_DOUBLE_ARRAY(env, ret, numThreads);
  if(m1Ptr == NULL || retPtr == NULL)
    return -1;

  tsmm(m1Ptr, retPtr, (int)m1rlen, (int)m1clen, (bool)leftTrans, (int)numThreads);

  int n = leftTrans ? m1clen : m1rlen;
  size_t nnz = computeNNZ<double>(retPtr, n * n);

  RELEASE_INPUT_ARRAY(env, m1, m1Ptr, numThreads);
  RELEASE_ARRAY(env, ret, retPtr, numThreads);

  return static_cast<jlong>(nnz);
}

JNIEXPORT jboolean JNICALL Java_org_apache_sysds_utils_NativeHelper_conv2dSparse
		(JNIEnv * env, jclass, jint apos, jint alen, jintArray aix, jdoubleArray avals, jdoubleArray filter,
		jdoubleArray ret, jint N, jint C, jint H, jint W, jint K, jint R, jint S,
		jint stride_h, jint stride_w, jint pad_h, jint pad_w, jint P, jint Q, jint numThreads) {
  int* aixPtr = ((int*)env->GetPrimitiveArrayCritical(aix, NULL));
  double* avalsPtr = GET_DOUBLE_ARRAY(env, avals, numThreads);
  double* filterPtr = GET_DOUBLE_ARRAY(env, filter, numThreads);
  double* retPtr = GET_DOUBLE_ARRAY(env, ret, numThreads);
  
  conv2dSparse((int)apos, (int)alen, aixPtr, avalsPtr, filterPtr, retPtr, (int)N, (int)C, (int)H, (int)W, 
		(int)K, (int)R, (int)S, (int)stride_h, (int)stride_w, (int)pad_h, (int)pad_w, (int)P, (int)Q, (int)numThreads);
  
  RELEASE_INPUT_ARRAY(env, avals, avalsPtr, numThreads);
  RELEASE_INPUT_ARRAY(env, filter, filterPtr, numThreads);
  env->ReleasePrimitiveArrayCritical(aix, aixPtr, JNI_ABORT);
  RELEASE_ARRAY(env, ret, retPtr, numThreads); 
  return (jboolean) true;
}

JNIEXPORT jboolean JNICALL Java_org_apache_sysds_utils_NativeHelper_conv2dBackwardFilterSparseDense
		(JNIEnv * env, jclass, jint apos, jint alen, jintArray aix, jdoubleArray avals, jdoubleArray dout,  
		jdoubleArray ret, jint N, jint C, jint H, jint W, jint K, jint R, jint S,
		jint stride_h, jint stride_w, jint pad_h, jint pad_w, jint P, jint Q, jint numThreads) {
  int* aixPtr = ((int*)env->GetPrimitiveArrayCritical(aix, NULL));
  double* avalsPtr = GET_DOUBLE_ARRAY(env, avals, numThreads);
  double* doutPtr = GET_DOUBLE_ARRAY(env, dout, numThreads);
  double* retPtr = GET_DOUBLE_ARRAY(env, ret, numThreads);
  
  conv2dBackwardFilterSparseDense((int)apos, (int)alen, aixPtr, avalsPtr, doutPtr, retPtr, (int)N, (int)C, (int)H, (int)W, 
		(int)K, (int)R, (int)S, (int)stride_h, (int)stride_w, (int)pad_h, (int)pad_w, (int)P, (int)Q, (int)numThreads);
  
  RELEASE_INPUT_ARRAY(env, avals, avalsPtr, numThreads);
  RELEASE_INPUT_ARRAY(env, dout, doutPtr, numThreads);
  env->ReleasePrimitiveArrayCritical(aix, aixPtr, JNI_ABORT);
  RELEASE_ARRAY(env, ret, retPtr, numThreads); 
  return (jboolean) true;
}

JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_conv2dDense(
		JNIEnv* env, jclass, jdoubleArray input, jdoubleArray filter,
		jdoubleArray ret, jint N, jint C, jint H, jint W, jint K, jint R, jint S,
		jint stride_h, jint stride_w, jint pad_h, jint pad_w, jint P, jint Q, jint numThreads)
{
  double* inputPtr = GET_DOUBLE_ARRAY(env, input, numThreads);
  double* filterPtr = GET_DOUBLE_ARRAY(env, filter, numThreads);
  double* retPtr = GET_DOUBLE_ARRAY(env, ret, numThreads);
  if(inputPtr == NULL || filterPtr == NULL || retPtr == NULL)
    return -1;
  
  size_t nnz = dconv2dBiasAddDense(inputPtr, 0, filterPtr, retPtr, (int) N, (int) C, (int) H, (int) W, (int) K,
		(int) R, (int) S, (int) stride_h, (int) stride_w, (int) pad_h, (int) pad_w, (int) P,
		(int) Q, false, (int) numThreads);
  
  RELEASE_INPUT_ARRAY(env, input, inputPtr, numThreads);
  RELEASE_INPUT_ARRAY(env, filter, filterPtr, numThreads);
  RELEASE_ARRAY(env, ret, retPtr, numThreads);
  return static_cast<jlong>(nnz);
}

JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_dconv2dBiasAddDense(
		JNIEnv* env, jclass, jdoubleArray input, jdoubleArray bias, jdoubleArray filter,
		jdoubleArray ret, jint N, jint C, jint H, jint W, jint K, jint R, jint S,
		jint stride_h, jint stride_w, jint pad_h, jint pad_w, jint P, jint Q, jint numThreads)
{
  double* inputPtr = GET_DOUBLE_ARRAY(env, input, numThreads);
  double* biasPtr = GET_DOUBLE_ARRAY(env, bias, numThreads);
  double* filterPtr = GET_DOUBLE_ARRAY(env, filter, numThreads);
  double* retPtr = GET_DOUBLE_ARRAY(env, ret, numThreads);
  if(inputPtr == NULL || biasPtr == NULL || filterPtr == NULL || retPtr == NULL)
	return -1;
  
  size_t nnz = dconv2dBiasAddDense(inputPtr, biasPtr, filterPtr, retPtr, (int) N, (int) C, (int) H, (int) W, (int) K,
		(int) R, (int) S, (int) stride_h, (int) stride_w, (int) pad_h, (int) pad_w, (int) P,
		(int) Q, true, (int) numThreads);
  
  RELEASE_INPUT_ARRAY(env, input, inputPtr, numThreads);
  RELEASE_INPUT_ARRAY(env, bias, biasPtr, numThreads);
  RELEASE_INPUT_ARRAY(env, filter, filterPtr, numThreads);
  RELEASE_ARRAY(env, ret, retPtr, numThreads);
  return static_cast<jlong>(nnz);
}

JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_sconv2dBiasAddDense(
		JNIEnv* env, jclass, jobject input, jobject bias, jobject filter,
		jobject ret, jint N, jint C, jint H, jint W, jint K, jint R, jint S,
		jint stride_h, jint stride_w, jint pad_h, jint pad_w, jint P, jint Q, jint numThreads) 
{
  float* inputPtr = (float*) env->GetDirectBufferAddress(input);
  float* biasPtr =  (float*) env->GetDirectBufferAddress(bias);
  float* filterPtr = (float*) env->GetDirectBufferAddress(filter);
  float* retPtr = (float*) env->GetDirectBufferAddress(ret);
  if(inputPtr == NULL || biasPtr == NULL || filterPtr == NULL || retPtr == NULL)
    return -1;
  
  size_t nnz = sconv2dBiasAddDense(inputPtr, biasPtr, filterPtr, retPtr, (int) N, (int) C, (int) H, (int) W, (int) K,
    (int) R, (int) S, (int) stride_h, (int) stride_w, (int) pad_h, (int) pad_w, (int) P,
		(int) Q, true, (int) numThreads);

  return static_cast<jlong>(nnz);
}

JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_conv2dBackwardDataDense(
    JNIEnv* env, jclass, jdoubleArray filter, jdoubleArray dout,
    jdoubleArray ret, jint N, jint C, jint H, jint W, jint K, jint R, jint S,
    jint stride_h, jint stride_w, jint pad_h, jint pad_w, jint P, jint Q, jint numThreads) {

  double* filterPtr = GET_DOUBLE_ARRAY(env, filter, numThreads);
  double* doutPtr = GET_DOUBLE_ARRAY(env, dout, numThreads);
  double* retPtr = GET_DOUBLE_ARRAY(env, ret, numThreads);
  if(doutPtr == NULL || filterPtr == NULL || retPtr == NULL)
    return -1;

  size_t nnz = conv2dBackwardDataDense(filterPtr, doutPtr, retPtr, (int) N, (int) C, (int) H, (int) W, (int) K,
    (int) R, (int) S, (int) stride_h, (int) stride_w, (int) pad_h, (int) pad_w,
    (int) P, (int) Q, (int) numThreads);

  RELEASE_INPUT_ARRAY(env, filter, filterPtr, numThreads);
  RELEASE_INPUT_ARRAY(env, dout, doutPtr, numThreads);
  RELEASE_ARRAY(env, ret, retPtr, numThreads);
  return static_cast<jlong>(nnz);
}

JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_conv2dBackwardFilterDense(
    JNIEnv* env, jclass, jdoubleArray input, jdoubleArray dout,
    jdoubleArray ret, jint N, jint C, jint H, jint W, jint K, jint R, jint S,
    jint stride_h, jint stride_w, jint pad_h, jint pad_w, jint P, jint Q, jint numThreads) {
  double* inputPtr = GET_DOUBLE_ARRAY(env, input, numThreads);
  double* doutPtr = GET_DOUBLE_ARRAY(env, dout, numThreads);
  double* retPtr = GET_DOUBLE_ARRAY(env, ret, numThreads);
  if(doutPtr == NULL || inputPtr == NULL || retPtr == NULL)
    return -1;

  size_t nnz = conv2dBackwardFilterDense(inputPtr, doutPtr, retPtr, (int)N, (int) C, (int) H, (int) W, (int) K, (int) R,
    (int) S, (int) stride_h, (int) stride_w, (int) pad_h, (int) pad_w, (int) P,
    (int) Q, (int) numThreads);

  RELEASE_INPUT_ARRAY(env, input, inputPtr, numThreads);
  RELEASE_INPUT_ARRAY(env, dout, doutPtr, numThreads);
  RELEASE_ARRAY(env, ret, retPtr, numThreads);
  return static_cast<jlong>(nnz);
}

unique_ptr<istream> get_stream(JNIEnv* env, jbyteArray ary) {
    size_t size = env->GetArrayLength(ary);
    jbyte* data = env->GetByteArrayElements(ary, NULL);

    // FIXME: this copies string data TWICE. maybe implement a custom stream
    // idea: implement a custom stream that wraps a jbyteArray, which calls ReleaseByteArrayElements in its d'tor
    string data_s = string(reinterpret_cast<char*>(data), size);
    unique_ptr<istream> ret = std::make_unique<istringstream>(data_s);
    env->ReleaseByteArrayElements(ary, data, JNI_ABORT);
    return ret;
}

jbyteArray allocate_byte_array(JNIEnv* env, ostringstream& stream) {
    string data = stream.str(); // FIXME: this copies string content. maybe implement custom ostream
    jbyteArray ret = env->NewByteArray(data.size());
    env->SetByteArrayRegion(ret, 0, data.size(), reinterpret_cast<jbyte*>(data.data()));
    return ret;
}

void my_assert(bool assertion, const char* message = "Assertion failed") {
    if (!assertion) {
        throw logic_error(message);
    }
}

template<typename T> jbyteArray serialize(JNIEnv* env, T& object) {
    ostringstream ss;
    object.save(ss);
    return allocate_byte_array(env, ss);
}

void serialize_uint32_t(ostream& ss, uint32_t n) {
    n = htonl(n);
    ss.write(reinterpret_cast<char*>(&n), sizeof(n));
}

uint32_t deserialize_uint32_t(istream& ss) {
    uint32_t ret;
    ss.read(reinterpret_cast<char*>(&ret), sizeof(ret));
    ret = ntohl(ret);
    return ret;
}

Ciphertext deserialize_ciphertext(istream& ss, const SEALContext& context) {
    Ciphertext ret;
    ret.load(context, ss);
    return ret;
}

void serialize_plaintext(ostream& ss, Plaintext plaintext) {
    plaintext.save(ss);
}

template<typename T> T deserialize_unsafe(JNIEnv* env, const SEALContext& context, jbyteArray serialized_object) {
    auto ss = get_stream(env, serialized_object);
    T deserialized;
    deserialized.unsafe_load(context, *ss); // necessary bc partial public keys are not valid public keys
    return deserialized;
}

template<typename T> T deserialize(JNIEnv* env, const SEALContext& context, jbyteArray serialized_object) {
    auto ss = get_stream(env, serialized_object);
    T deserialized;
    deserialized.load(context, *ss); // necessary bc partial public keys are not valid public keys
    return deserialized;
}

JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_initClient
  (JNIEnv* env, jclass, jbyteArray a_ary) {
    double scale = pow(2.0, 40);
    GlobalState gs(scale);

    // copy a to global state
    size_t byte_size = env->GetArrayLength(a_ary);
    my_assert(byte_size % sizeof(uint64_t) == 0);
    size_t size = byte_size / sizeof(uint64_t);
    uint64_t* a = reinterpret_cast<uint64_t*>(env->GetByteArrayElements(a_ary, NULL));
    gsl::span<uint64_t > new_a(a, size);

    vector<uint64_t> new_a_buf;
    new_a_buf.assign(new_a.begin(), new_a.end());
    gs.a.set_data(new_a_buf);

    // release a without back-copy
    env->ReleaseByteArrayElements(a_ary, reinterpret_cast<jbyte*>(a), JNI_ABORT);

    Client* client = new Client(gs);
    return reinterpret_cast<jlong>(client);
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_sysds_utils_NativeHelper_generatePartialPublicKey
  (JNIEnv* env, jclass, jlong client_ptr) {
    Client* client = reinterpret_cast<Client*>(client_ptr);
    return serialize(env, client->partial_public_key().data());
}


JNIEXPORT void JNICALL Java_org_apache_sysds_utils_NativeHelper_setPublicKey
  (JNIEnv* env, jclass, jlong client_ptr, jbyteArray serialized_public_key) {
    Client* client = reinterpret_cast<Client*>(client_ptr);
    client->set_public_key(deserialize<PublicKey>(env, client->context(), serialized_public_key));
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_sysds_utils_NativeHelper_encrypt
  (JNIEnv* env, jclass, jlong client_ptr, jdoubleArray jdata) {
    Client* client = reinterpret_cast<Client*>(client_ptr);
    size_t slot_count = get_slot_count(client->context());
    size_t num_data = env->GetArrayLength(jdata);
    const double* data = static_cast<const double*>(env->GetDoubleArrayElements(jdata, NULL));

    std::ostringstream ss;
    // write chunk size
    uint32_t num_chunks = (num_data - 1) / slot_count + 1;
    serialize_uint32_t(ss, num_chunks);
    for (size_t i = 0; i < num_chunks; i++) {
        size_t offset = slot_count * i;
        size_t length = min(slot_count, num_data-offset);
        gsl::span<const double> data_span(&data[offset], length);
        Ciphertext encrypted_chunk = client->encrypted_data(data_span);
        encrypted_chunk.save(ss);
    }
    return allocate_byte_array(env, ss);
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_sysds_utils_NativeHelper_partiallyDecrypt
  (JNIEnv* env, jclass, jlong client_ptr, jbyteArray serialized_ciphertexts) {
    Client* client = reinterpret_cast<Client*>(client_ptr);
    auto input = get_stream(env, serialized_ciphertexts);
    std::ostringstream ss;

    // read num of chunks
    uint32_t num_chunks = deserialize_uint32_t(*input);

    // write chunk size
    serialize_uint32_t(ss, num_chunks);
    for (int i = 0; i < num_chunks; i++) {
        Ciphertext ciphertext = deserialize_ciphertext(*input, client->context());
        Plaintext plaintext = client->partial_decryption(ciphertext);
        plaintext.save(ss);
    }

    return allocate_byte_array(env, ss);
}


JNIEXPORT jlong JNICALL Java_org_apache_sysds_utils_NativeHelper_initServer
  (JNIEnv *, jclass) {
    double scale = pow(2.0, 40);
    GlobalState gs(scale);
    Server* server = new Server(gs);
    return reinterpret_cast<jlong>(server);
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_sysds_utils_NativeHelper_generateA
  (JNIEnv* env, jclass, jlong server_ptr) {
    Server* server = reinterpret_cast<Server*>(server_ptr);
    uint64_t* data = server->a().data();
    size_t size = server->a().size() * sizeof(data[0]) / sizeof(jbyte);
    jbyteArray ret = env->NewByteArray(size);
    env->SetByteArrayRegion(ret, 0, size, reinterpret_cast<jbyte*>(data));
    return ret;
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_sysds_utils_NativeHelper_aggregatePartialPublicKeys
  (JNIEnv* env, jclass, jlong server_ptr, jobjectArray partial_public_keys_serialized) {
    Server* server = reinterpret_cast<Server*>(server_ptr);
    size_t num_partial_public_keys = env->GetArrayLength(partial_public_keys_serialized);
    std::vector<Ciphertext> partial_public_keys;
    partial_public_keys.reserve(num_partial_public_keys);

    for (int i = 0; i < num_partial_public_keys; i++) {
        jbyteArray j_data = static_cast<jbyteArray>(env->GetObjectArrayElement(partial_public_keys_serialized, i));
        partial_public_keys.push_back(deserialize_unsafe<Ciphertext>(env, server->context(), j_data));
    }

    server->accumulate_partial_public_keys(gsl::span(partial_public_keys));
    return serialize(env, server->public_key());
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_sysds_utils_NativeHelper_accumulateCiphertexts
  (JNIEnv* env, jclass, jlong server_ptr, jobjectArray ciphertexts_serialized) {
    Server* server = reinterpret_cast<Server*>(server_ptr);
    size_t num_ciphertext_arys = env->GetArrayLength(ciphertexts_serialized);

    // init streams
    vector<unique_ptr<istream>> buf;
    buf.reserve(num_ciphertext_arys);
    for (int i = 0; i < num_ciphertext_arys; i++) {
        jbyteArray j_data = static_cast<jbyteArray>(env->GetObjectArrayElement(ciphertexts_serialized, i));
        auto stream = get_stream(env, j_data);
        buf.emplace_back(std::move(stream));
    }

    // read lengths of ciphertext arys and check that they are all the same
    uint32_t num_slots = deserialize_uint32_t(*buf[0]);
    for (int i = 1; i < num_ciphertext_arys; i++) {
        my_assert(deserialize_uint32_t(*buf[i]) == num_slots);
    }

    // read ciphertexts in chunks and accumulate them
    ostringstream result;
    serialize_uint32_t(result, num_slots);
    for (int chunk_idx = 0; chunk_idx < num_slots; chunk_idx++) {
        vector<Ciphertext> ciphertexts;
        ciphertexts.reserve(num_ciphertext_arys);
        for (int i = 0; i < num_ciphertext_arys; i++) {
            Ciphertext deserialized;
            deserialized.load(server->context(), *buf[i]);
            ciphertexts.emplace_back(deserialized);
        }
        Ciphertext sum = server->sum_data(std::move(ciphertexts));
        sum.save(result);
    }

    return allocate_byte_array(env, result);
}


JNIEXPORT jdoubleArray JNICALL Java_org_apache_sysds_utils_NativeHelper_average
  (JNIEnv* env, jclass, jlong server_ptr, jbyteArray ciphertext_sum_serialized, jobjectArray partial_decryptions_serialized) {
    Server* server = reinterpret_cast<Server*>(server_ptr);
    size_t slot_size = get_slot_count(server->context());
    size_t num_plaintext_arys = env->GetArrayLength(partial_decryptions_serialized);

    // init streams
    vector<unique_ptr<istream>> buf;
    buf.reserve(num_plaintext_arys);
    for (int i = 0; i < num_plaintext_arys; i++) {
        jbyteArray j_data = static_cast<jbyteArray>(env->GetObjectArrayElement(partial_decryptions_serialized, i));
        auto stream = get_stream(env, j_data);
        buf.emplace_back(std::move(stream));
    }

    // read lengths of ciphertext arys and check that they are all the same
    uint32_t num_slots = deserialize_uint32_t(*buf[0]);
    for (int i = 1; i < num_plaintext_arys; i++) {
        my_assert(deserialize_uint32_t(*buf[i]) == num_slots, "number of plaintext slots is different");
    }

    auto encrypted_sum_stream = get_stream(env, ciphertext_sum_serialized);
    my_assert(deserialize_uint32_t(*encrypted_sum_stream) == num_slots, "number of ciphertext slots is different");

    // read ciphertexts in chunks and accumulate them
    jdoubleArray result = env->NewDoubleArray(num_slots * slot_size);
    for (int chunk_idx = 0; chunk_idx < num_slots; chunk_idx++) {
        Ciphertext encrypted_sum = deserialize_ciphertext(*encrypted_sum_stream, server->context());

        vector<Plaintext> partial_decryptions;
        partial_decryptions.reserve(num_plaintext_arys);
        for (int i = 0; i < num_plaintext_arys; i++) {
            Plaintext deserialized;
            deserialized.load(server->context(), *buf[i]);
            partial_decryptions.emplace_back(deserialized);
        }
        vector<double> averages = server->average(encrypted_sum, move(partial_decryptions));
        env->SetDoubleArrayRegion(result, chunk_idx*slot_size, averages.size(), averages.data());
    }

    return result;
}