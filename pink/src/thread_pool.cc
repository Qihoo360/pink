// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pink/include/thread_pool.h"
#include "pink/src/pink_thread_name.h"
//#include <terark/util/concurrent_queue.hpp>
//#include <terark/circular_queue.hpp>
//#include <terark/valvec.hpp>

#include <sys/time.h>

namespace pink {

class ThreadPool::Worker {
  public:
    explicit Worker(ThreadPool* tp)
       : rsignal_(&mu_), wsignal_(&mu_), start_(false), thread_pool_(tp)
    {
      // ignore user specified queue cap
      //queue_.queue().init(64); // cap=64 is enough
    }
    static void* WorkerMain(void* arg);

    void WorkerRun();

    void Schedual(TaskFunc func, void* arg);
    void DelaySchedule(uint64_t timeout, TaskFunc func, void* arg);

    int start();
    int stop();

    //terark::util::concurrent_queue<terark::circular_queue<Task> > queue_;
    //std::priority_queue<TimeTask, terark::valvec<TimeTask> > time_queue_;
    std::queue<Task> queue_;
    std::priority_queue<TimeTask> time_queue_;
    slash::Mutex mu_;
    slash::CondVar rsignal_;
    slash::CondVar wsignal_;

    std::atomic<bool> start_;
    ThreadPool* const thread_pool_;
    pthread_t thread_id_;
    std::string worker_name_;
    /*
      * No allowed copy and copy assign
      */
    Worker(const Worker&) = delete;
    void operator=(const Worker&) = delete;
};

void* ThreadPool::Worker::WorkerMain(void* arg) {
  static_cast<Worker*>(arg)->WorkerRun();
  return nullptr;
}

int ThreadPool::Worker::start() {
  if (!start_.load()) {
    if (pthread_create(&thread_id_, NULL, &WorkerMain, this)) {
      return -1;
    } else {
      start_.store(true);
      SetThreadName(thread_id_, thread_pool_->thread_pool_name() + "Worker");
    }
  }
  return 0;
}

int ThreadPool::Worker::stop() {
  if (start_.load()) {
    if (pthread_join(thread_id_, nullptr)) {
      return -1;
    } else {
      start_.store(false);
    }
  }
  return 0;
}

ThreadPool::ThreadPool(size_t worker_num,
                       size_t max_queue_size,
                       const std::string& thread_pool_name) :
  worker_num_(worker_num),
  max_queue_size_(max_queue_size),
  thread_pool_name_(thread_pool_name),
  running_(false),
  should_stop_(false)
  {}

ThreadPool::~ThreadPool() {
  stop_thread_pool();
}

int ThreadPool::start_thread_pool() {
  if (!running_.load()) {
    should_stop_.store(false);
    for (size_t i = 0; i < worker_num_; ++i) {
      workers_.push_back(new Worker(this));
      int res = workers_[i]->start();
      if (res != 0) {
        return kCreateThreadError;
      }
    }
    running_.store(true);
  }
  return kSuccess;
}

int ThreadPool::stop_thread_pool() {
  int res = 0;
  if (running_.load()) {
    should_stop_.store(true);
    for (const auto worker : workers_) {
      worker->rsignal_.SignalAll();
      worker->wsignal_.SignalAll();
      res = worker->stop();
      if (res != 0) {
        break;
      } else {
        delete worker;
      }
    }
    workers_.clear();
    running_.store(false);
  }
  return res;
}

bool ThreadPool::should_stop() {
  return should_stop_.load();
}

void ThreadPool::set_should_stop() {
  should_stop_.store(true);
}

void ThreadPool::Schedule(TaskFunc func, void* arg) {
  unsigned int unused = 0;
  auto tsc = __builtin_ia32_rdtscp(&unused);
  auto idx = tsc % uint16_t(worker_num_);
  auto wrk = workers_[idx];
  wrk->Schedual(func, arg);
}
void ThreadPool::Worker::Schedual(TaskFunc func, void* arg) {
  ThreadPool* tp = thread_pool_;
  mu_.Lock();
  while (queue_.size() >= tp->max_queue_size_ && !tp->should_stop()) {
    wsignal_.Wait();
  }
  if (!tp->should_stop()) {
    queue_.push(Task(func, arg));
    rsignal_.Signal();
  }
  mu_.Unlock();
}

/*
 * timeout is in millisecond
 */
void ThreadPool::DelaySchedule(
    uint64_t timeout, TaskFunc func, void* arg) {
  unsigned int unused = 0;
  auto tsc = __builtin_ia32_rdtscp(&unused);
  auto idx = tsc % uint16_t(worker_num_);
  auto wrk = workers_[idx];
  wrk->DelaySchedule(timeout, func, arg);
}
void ThreadPool::Worker::DelaySchedule(
    uint64_t timeout, TaskFunc func, void* arg) {
  /*
   * pthread_cond_timedwait api use absolute API
   * so we need gettimeofday + timeout
   */
  struct timeval now;
  gettimeofday(&now, NULL);
  uint64_t exec_time;
  exec_time = now.tv_sec * 1000000 + timeout * 1000 + now.tv_usec;

  mu_.Lock();
  if (!thread_pool_->should_stop()) {
    time_queue_.push(TimeTask(exec_time, func, arg));
    rsignal_.Signal();
  }
  mu_.Unlock();
}

size_t ThreadPool::max_queue_size() {
  return max_queue_size_;
}

void ThreadPool::cur_queue_size(size_t* qsize) {
  size_t n = 0;
  for (auto w : workers_) {
    slash::MutexLock l(&w->mu_);
    n += w->queue_.size();
  }
  *qsize = n;
}

void ThreadPool::cur_time_queue_size(size_t* qsize) {
  size_t n = 0;
  for (auto w : workers_) {
    slash::MutexLock l(&w->mu_);
    n += w->time_queue_.size();
  }
  *qsize = n;
}

std::string ThreadPool::thread_pool_name() {
  return thread_pool_name_;
}

void ThreadPool::Worker::WorkerRun() {
  ThreadPool* tp = thread_pool_;
  while (!tp->should_stop()) {
    mu_.Lock();
    while (queue_.empty() && time_queue_.empty() && !tp->should_stop()) {
      rsignal_.Wait();
    }
    if (tp->should_stop()) {
      mu_.Unlock();
      break;
    }
    if (!time_queue_.empty()) {
      struct timeval now;
      gettimeofday(&now, NULL);

      TimeTask time_task = time_queue_.top();
      uint64_t unow = now.tv_sec * 1000000 + now.tv_usec;
      if (unow / 1000 >= time_task.exec_time / 1000) {
        TaskFunc func = time_task.func;
        void* arg = time_task.arg;
        time_queue_.pop();
        mu_.Unlock();
        (*func)(arg);
        continue;
      } else if (queue_.empty() && !tp->should_stop()) {
        rsignal_.TimedWait(
            static_cast<uint32_t>((time_task.exec_time - unow) / 1000));
        mu_.Unlock();
        continue;
      }
    }
    if (!queue_.empty()) {
      TaskFunc func = queue_.front().func;
      void* arg = queue_.front().arg;
      queue_.pop();
      wsignal_.Signal();
      mu_.Unlock();
      (*func)(arg);
    }
  }
}
}  // namespace pink
