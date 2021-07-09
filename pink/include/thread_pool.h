// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PINK_INCLUDE_THREAD_POOL_H_
#define PINK_INCLUDE_THREAD_POOL_H_

#include <string>
#include <queue>
#include <atomic>
#include <pthread.h>

#include "slash/include/slash_mutex.h"
#include "pink/include/pink_define.h"

namespace pink {

typedef void (*TaskFunc)(void*);

struct Task {
 TaskFunc func;
 void* arg;
 Task (TaskFunc _func, void* _arg)
     : func(_func), arg(_arg) {}
};

struct TimeTask {
  uint64_t exec_time;
  TaskFunc func;
  void* arg;
  TimeTask(uint64_t _exec_time, TaskFunc _func, void* _arg) :
    exec_time(_exec_time),
    func(_func),
    arg(_arg) {}
  bool operator < (const TimeTask& task) const {
    return exec_time > task.exec_time;
  }
};

class ThreadPool {

 public:
  class Worker;

  explicit ThreadPool(size_t worker_num,
                      size_t max_queue_size,
                      const std::string& thread_pool_name = "ThreadPool");
  virtual ~ThreadPool();

  int start_thread_pool();
  int stop_thread_pool();
  bool should_stop();
  void set_should_stop();

  void Schedule(TaskFunc func, void* arg);
  void DelaySchedule(uint64_t timeout, TaskFunc func, void* arg);
  size_t max_queue_size();
  size_t worker_size();
  void cur_queue_size(size_t* qsize);
  void cur_time_queue_size(size_t* qsize);
  std::string thread_pool_name();

 private:
  size_t worker_num_;
  size_t max_queue_size_;
  std::string thread_pool_name_;
  std::vector<Worker*> workers_;
  std::atomic<bool> running_;
  std::atomic<bool> should_stop_;

  /*
   * No allowed copy and copy assign
   */
  ThreadPool(const ThreadPool&) = delete;
  void operator=(const ThreadPool&) = delete;
};

}  // namespace pink

#endif  // PINK_INCLUDE_THREAD_POOL_H_
