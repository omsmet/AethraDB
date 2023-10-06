#ifndef __AETHRADB_LIB_H
#define __AETHRADB_LIB_H

#include <graal_isolate_dynamic.h>


#if defined(__cplusplus)
extern "C" {
#endif

typedef graal_isolatethread_t* (*Java_AethraDB_util_AethraGenerator_createIsolate_fn_t)();

typedef void (*Java_AethraDB_util_AethraGenerator_plan_fn_t)(struct JNIEnv_*, size_t, graal_isolatethread_t*, void *, void *);

typedef void (*Java_AethraDB_util_AethraGenerator_codeGen_fn_t)(struct JNIEnv_*, size_t, graal_isolatethread_t*, int, int);

typedef void * (*Java_AethraDB_util_AethraGenerator_compile_fn_t)(struct JNIEnv_*, size_t, graal_isolatethread_t*);

typedef int (*run_main_fn_t)(int argc, char** argv);

#if defined(__cplusplus)
}
#endif
#endif
