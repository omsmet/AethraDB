#ifndef __AETHRADB_LIB_H
#define __AETHRADB_LIB_H

#include <graal_isolate.h>


#if defined(__cplusplus)
extern "C" {
#endif

graal_isolatethread_t* Java_AethraDB_util_AethraGenerator_createIsolate();

void Java_AethraDB_util_AethraGenerator_plan(struct JNIEnv_*, size_t, graal_isolatethread_t*, void *, void *);

void Java_AethraDB_util_AethraGenerator_codeGen(struct JNIEnv_*, size_t, graal_isolatethread_t*, int, int);

void * Java_AethraDB_util_AethraGenerator_compile(struct JNIEnv_*, size_t, graal_isolatethread_t*);

int run_main(int argc, char** argv);

#if defined(__cplusplus)
}
#endif
#endif
