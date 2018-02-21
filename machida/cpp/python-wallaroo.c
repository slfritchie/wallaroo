/*

Copyright 2017 The Wallaroo Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License.

*/

#ifdef __APPLE__
    #include <AvailabilityMacros.h>
    #if MAC_OS_X_VERSION_MAX_ALLOWED < 101300
        #include <Python/Python.h>
    #else
        #include <Python2.7/Python.h>
    #endif
#else
    #include <python2.7/Python.h>
#endif

#include "pony.h"

PyThreadState* main_thread_state;

PyInterpreterState* interpreter_state;

PyThreadState** thread_states;

int ponyint_sched_cores();

PyObject *g_user_deserialization_fn;
PyObject *g_user_serialization_fn;

extern void init_python_threads()
{
  PyEval_InitThreads();
  main_thread_state = PyThreadState_Get();
  interpreter_state = main_thread_state->interp;

  int idx = pony_scheduler_index(pony_ctx());

  thread_states = calloc(ponyint_sched_cores(), sizeof(PyThreadState));

  thread_states[idx] = PyEval_SaveThread();
}

extern void acquire_python_lock()
{
  int idx = pony_scheduler_index(pony_ctx());

  PyThreadState *thread_state = thread_states[idx];

  if (thread_state == NULL)
  {
    thread_state = thread_states[idx] = PyThreadState_New(interpreter_state);
  }

  PyEval_RestoreThread(thread_state);
}

extern void release_python_lock()
{
  int idx = pony_scheduler_index(pony_ctx());

  thread_states[idx] = PyEval_SaveThread();
}

extern PyObject *load_module(char *module_name)
{
  PyObject *pName, *pModule;

  acquire_python_lock();
  pName = PyString_FromString(module_name);
  /* Error checking of pName left out */

  pModule = PyImport_Import(pName);
  Py_DECREF(pName);
  release_python_lock();

  return pModule;
}

extern PyObject *application_setup(PyObject *pModule, PyObject *args)
{
  PyObject *pFunc, *pValue;

  acquire_python_lock();
  pFunc = PyObject_GetAttrString(pModule, "application_setup");
  pValue = PyObject_CallFunctionObjArgs(pFunc, args, NULL);
  Py_DECREF(pFunc);
  release_python_lock();

  return pValue;
}

extern size_t list_item_count(PyObject *list)
{
  return PyList_Size(list);
}

extern PyObject *get_application_setup_item(PyObject *list, size_t idx)
{
  return PyList_GetItem(list, idx);
}

extern char *get_application_setup_action(PyObject *item)
{
  PyObject *action = PyTuple_GetItem(item, 0);
  char * rtn = PyString_AsString(action);
  Py_DECREF(action);
  return rtn;
}

extern size_t source_decoder_header_length(PyObject *source_decoder)
{
  PyObject *pFunc, *pValue;

  acquire_python_lock();
  PyErr_Clear();
  pFunc = PyObject_GetAttrString(source_decoder, "header_length");
  pValue = PyObject_CallFunctionObjArgs(pFunc, NULL);

  size_t sz = PyInt_AsSsize_t(pValue);
  Py_XDECREF(pFunc);
  Py_DECREF(pValue);
  release_python_lock();

  if (sz > 0 && sz < SIZE_MAX) {
    return sz;
  } else {
    return 0;
  }
}

extern size_t source_decoder_payload_length(PyObject *source_decoder, char *bytes, size_t size)
{
  PyObject *pFunc, *pValue, *pBytes;

  acquire_python_lock();
  PyErr_Clear();

  pFunc = PyObject_GetAttrString(source_decoder, "payload_length");
  pBytes = PyBytes_FromStringAndSize(bytes, size);
  pValue = PyObject_CallFunctionObjArgs(pFunc, pBytes, NULL);

  size_t sz = PyInt_AsSsize_t(pValue);

  Py_XDECREF(pFunc);
  Py_XDECREF(pBytes);
  Py_XDECREF(pValue);

  release_python_lock();

  /*
  ** NOTE: This doesn't protect us from Python from returning
  **       something bogus like -7.  There is no Python/C API
  **       function to tell us if the Python value is negative.
  */
  if (sz > 0 && sz < SIZE_MAX) {
    return sz;
  } else {
    printf("ERROR: Python payload_length() method returned invalid size\n");
    return 0;
  }
}

extern PyObject *source_decoder_decode(PyObject *source_decoder, char *bytes, size_t size)
{
  PyObject *pFunc, *pBytes, *pValue;

  acquire_python_lock();
  pFunc = PyObject_GetAttrString(source_decoder, "decode");
  pBytes = PyBytes_FromStringAndSize(bytes, size);
  pValue = PyObject_CallFunctionObjArgs(pFunc, pBytes, NULL);

  Py_DECREF(pFunc);
  Py_DECREF(pBytes);
  release_python_lock();

  return pValue;
}

extern PyObject *instantiate_python_class(PyObject *class)
{
  acquire_python_lock();
  PyObject *p = PyObject_CallFunctionObjArgs(class, NULL);
  release_python_lock();
  return p;
}

extern PyObject *get_name(PyObject *pObject)
{
  PyObject *pFunc, *pValue = NULL;

  acquire_python_lock();
  pFunc = PyObject_GetAttrString(pObject, "name");
  if (pFunc != NULL) {
    pValue = PyObject_CallFunctionObjArgs(pFunc, NULL);
    Py_DECREF(pFunc);
  }
  release_python_lock();

  return pValue;
}

extern PyObject *computation_compute(PyObject *computation, PyObject *data,
  char* method)
{
  PyObject *pFunc, *pValue;

  acquire_python_lock();
  pFunc = PyObject_GetAttrString(computation, method);
  pValue = PyObject_CallFunctionObjArgs(pFunc, data, NULL);
  Py_DECREF(pFunc);
  release_python_lock();

  if (pValue != Py_None)
    return pValue;
  else
    return NULL;
}

extern PyObject *sink_encoder_encode(PyObject *sink_encoder, PyObject *data)
{
  PyObject *pFunc, *pArgs, *pValue;

  acquire_python_lock();
  pFunc = PyObject_GetAttrString(sink_encoder, "encode");
  pValue = PyObject_CallFunctionObjArgs(pFunc, data, NULL);
  Py_DECREF(pFunc);
  release_python_lock();

  return pValue;
}

extern void py_incref(PyObject *o)
{
  acquire_python_lock();
  Py_INCREF(o);
  release_python_lock();
}

extern void py_decref(PyObject *o)
{
  acquire_python_lock();
  Py_DECREF(o);
  release_python_lock();
}

extern PyObject *state_builder_build_state(PyObject *state_builder)
{
  PyObject *pFunc, *pArgs, *pValue;

  acquire_python_lock();
  pFunc = PyObject_GetAttrString(state_builder, "____wallaroo_build____");
  pValue = PyObject_CallFunctionObjArgs(pFunc, NULL);
  Py_DECREF(pFunc);
  release_python_lock();

  return pValue;
}

extern PyObject *stateful_computation_compute(PyObject *computation,
  PyObject *data, PyObject *state, char *method)
{
  PyObject *pFunc, *pValue;

  acquire_python_lock();
  pFunc = PyObject_GetAttrString(computation, method);
  pValue = PyObject_CallFunctionObjArgs(pFunc, data, state, NULL);
  Py_DECREF(pFunc);
  release_python_lock();

  return pValue;
}

extern long key_hash(PyObject *key)
{
  acquire_python_lock();
  PyErr_Clear();
  long l = PyObject_Hash(key);
  release_python_lock();
  return l;
}


extern int key_eq(PyObject *key, PyObject* other)
{
  acquire_python_lock();
  int i = PyObject_RichCompareBool(key, other, Py_EQ);
  release_python_lock();
  return i;
}

extern PyObject *partition_function_partition(PyObject *partition_function, PyObject *data)
{
  PyObject *pFunc, *pValue;

  acquire_python_lock();
  pFunc = PyObject_GetAttrString(partition_function, "partition");
  pValue = PyObject_CallFunctionObjArgs(pFunc, data, NULL);
  Py_DECREF(pFunc);
  release_python_lock();

  return pValue;
}

extern long partition_function_partition_u64(PyObject *partition_function, PyObject *data)
{
  PyObject *pFunc, *pValue;

  acquire_python_lock();
  pFunc = PyObject_GetAttrString(partition_function, "partition");
  pValue = PyObject_CallFunctionObjArgs(pFunc, data, NULL);
  Py_DECREF(pFunc);

  long rtn = PyInt_AsLong(pValue);
  Py_DECREF(pValue);
  release_python_lock();

  return rtn;
}

extern void set_user_serialization_fns(PyObject *module)
{
  acquire_python_lock();
  if (PyObject_HasAttrString(module, "deserialize") && PyObject_HasAttrString(module, "serialize"))
  {
    g_user_deserialization_fn = PyObject_GetAttrString(module, "deserialize");
    g_user_serialization_fn = PyObject_GetAttrString(module, "serialize");
  }
  else
  {
    PyObject *wallaroo = PyObject_GetAttrString(module, "wallaroo");
    g_user_deserialization_fn = PyObject_GetAttrString(wallaroo, "deserialize");
    g_user_serialization_fn = PyObject_GetAttrString(wallaroo, "serialize");
    Py_DECREF(wallaroo);
  }
  release_python_lock();
}

extern void *user_deserialization(char *bytes)
{
  unsigned char *ubytes = (unsigned char *)bytes;
  // extract size
  size_t size = (((size_t)ubytes[0]) << 24)
    + (((size_t)ubytes[1]) << 16)
    + (((size_t)ubytes[2]) << 8)
    + ((size_t)ubytes[3]);

  acquire_python_lock();
  PyObject *py_bytes = PyBytes_FromStringAndSize(bytes + 4, size);
  PyObject *ret = PyObject_CallFunctionObjArgs(g_user_deserialization_fn, py_bytes, NULL);

  Py_DECREF(py_bytes);
  release_python_lock();

  return ret;
}

extern size_t user_serialization_get_size(PyObject *o)
{
  acquire_python_lock();
  PyObject *user_bytes = PyObject_CallFunctionObjArgs(g_user_serialization_fn, o, NULL);

  size_t ret = 0;

  // This will be null if there was an exception.
  if (user_bytes)
  {
    size_t size = PyString_Size(user_bytes);
    Py_DECREF(user_bytes);

    // return the size of the buffer plus the 4 bytes needed to record that size.
    ret = 4 + size;
  }

  release_python_lock();

  return ret;
}

extern void user_serialization(PyObject *o, char *bytes)
{
  acquire_python_lock();

  PyObject *user_bytes = PyObject_CallFunctionObjArgs(g_user_serialization_fn, o, NULL);

  // This will be null if there was an exception.
  if (user_bytes)
  {
    size_t size = PyString_Size(user_bytes);

    unsigned char *ubytes = (unsigned char *) bytes;

    ubytes[0] = (unsigned char)(size >> 24);
    ubytes[1] = (unsigned char)(size >> 16);
    ubytes[2] = (unsigned char)(size >> 8);
    ubytes[3] = (unsigned char)(size);

    memcpy(bytes + 4, PyString_AsString(user_bytes), size);

    Py_DECREF(user_bytes);
  }

  release_python_lock();
}

extern int py_bool_check(PyObject *b)
{
  acquire_python_lock();
  int ret = PyBool_Check(b);
  release_python_lock();
  return ret;
}

extern int is_py_none(PyObject *o)
{
  return o == Py_None;
}

extern int py_list_check(PyObject *l)
{
  acquire_python_lock();
  int c = PyList_Check(l);
  release_python_lock();
  return c;
}
