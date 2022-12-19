//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

//在锁管理器中，使用lock table管理锁，lock table是以元组ID为键，锁请求队列为值的哈希表。
//其中，锁请求中保存了请求该元组锁的事务ID、请求的元组锁类型、以及请求是否被许可；通过队列的方式保存锁保证了锁请求的先后顺序：



class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };


////其中，锁请求中保存了请求该元组锁的事务ID、请求的元组锁类型、以及请求是否被许可；通过队列的方式保存锁保证了锁请求的先后顺序：
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

    txn_id_t txn_id_;     //该元组锁的事务ID
    LockMode lock_mode_;    //请求的元组锁类型
    bool granted_;    //请求是否被许可
  };


  class LockRequestQueue {
   public:
    std::list<LockRequest> request_queue_;    // 通过队列的方式保存锁保证了锁请求的先后顺序： 


    // for notifying blocked transactions on this rid  用于通知此rid上被阻塞的事务
    std::condition_variable cv_;        // c++内置的条件变量 ，主要功能是对请求 阻塞(wait)或唤醒


    // txn_id of an upgrading transaction (if any)
    txn_id_t upgrading_ = INVALID_TXN_ID;
  };




 public:
  /**
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() = default;

  ~LockManager() = default;

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the
   * same transaction, i.e. the transaction is responsible for keeping track of
   * its current locks.
   */


  //共享锁(S锁)
  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);



  //排他锁(X锁)
  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);



  //将S锁升级为X锁
  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  

  //解锁 
  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);




 private:
  std::mutex latch_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;   //在锁管理器中，使用lock_table管理锁，lock_table是以元组ID为键，锁请求队列为值的哈希表。

  std::unordered_map<txn_id_t, Transaction *> txn_table_;
};

}  // namespace bustub
