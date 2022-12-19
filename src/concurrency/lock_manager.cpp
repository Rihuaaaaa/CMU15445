//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

//事务ID越大 ，证明越新，  新/老事务分别指事务ID较大的一方和较小的一方



//事务txn请求元组ID为rid的读锁：
bool LockManager::LockShared(Transaction *txn, const RID &rid) {

  //进行前置判断，当事务的状态为ABORT时，直接返回假
  if (txn->GetState() == TransactionState::ABORTED) 
  {
    return false;
  }
  //当前的事务状态为锁收缩时，调用获取锁函数将导致事务ABORT并抛出异常；
  if (txn->GetState() == TransactionState::SHRINKING) 
  {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  //当事务的隔离级别的READ_UNCOMMITTED（读未提交）时，其不应获取读锁，RU级别需要读到脏数据，尝试获取读锁将导致ABORT并抛出异常。
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) 
  {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  

  txn->SetState(TransactionState::GROWING);   //如前置判断通过，则将当前事务状态置为GROWING(第一阶段)，并获得互斥锁保护锁管理器的数据结构。
  std::unique_lock<std::mutex> lk(latch_);

  auto &lock_request_queue = lock_table_[rid];    // 先拿到RID对应的锁请求队列 ，lock_table是以元组ID为键，锁请求队列为值的哈希表。
  auto &request_queue = lock_request_queue.request_queue_;  //拿到锁请求队列后再具体拿到类里的请求队列成员
  
  auto &cv = lock_request_queue.cv_;      //条件变量
  auto txn_id = txn->GetTransactionId();
  
  
  request_queue.emplace_back(txn_id, LockMode::SHARED);    //获取对应元组ID的锁请求队列及其相关成员后，将当前事务的锁请求加入队列。锁请求队列里存放的是一个一个的锁请求类，因此有两个参数
  txn->GetSharedLockSet()->emplace(rid);      //在该请求被加入队列时将就应当调用GetSharedLockSet()将该元组ID加入事务的持有锁集合，使得该锁被杀死时能将该请求从队列中删除。
  txn_table_[txn_id] = txn;
  

  //Wound Wait : Kill all low priority transaction 高等低，消灭低 ； 低等高，低阻塞
  //将请求加入队列，循环判断是否满足加读锁条件（当前没有写锁），满足则继续执行，否则阻塞；
  //为了避免死锁，事务需要检查当前队列是否存在使得其阻塞的锁请求，对于X锁来说, 则仅当当前请求在请求队列首部时才合法
  //关于共享锁(S)。如果老事务要加读锁，那么要把队列中新事务的写锁请求abort掉。而对于读锁请求，可以共存。
  //如该事务杀死了任何其他事务，则通过锁请求队列的条件变量cv唤醒其他等待锁的事务，使得被杀死的事务可以退出请求队列。
  bool can_grant = true;
  bool is_kill = false;

  for (auto &request : request_queue)     //遍历锁请求队列里的每一个锁请求
  {
    if (request.lock_mode_ == LockMode::EXCLUSIVE)    //如果当前队头是写锁
    {
      if (request.txn_id_ > txn_id)      //当前队头是写锁，且当前队头的事务要更年轻，杀掉当前事务
      {
        txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);   //直接终止队头的事务
        is_kill = true;
      } 
      else      //如非则将can_grant置为false表示事务将被阻塞。
      {
        can_grant = false;
      }
    }

    if (request.txn_id_ == txn_id) 
    {
      request.granted_ = can_grant;
      break;
    }
  }

  if (is_kill)    // 确实杀掉了, 就需要立即notify_all() ,当杀死了任一事务时，将唤醒该锁等待队列的所有事务
  {
    cv.notify_all();
  }

  //Wait the lock
  //如存在阻塞该事务的其他事务，且该事务不能将其杀死，则进入循环等待锁。在循环中，事务调用条件变量cv的wait阻塞自身，并原子地释放锁。
  //每次当其被唤醒时，其检查：
  //1. 该事务是否被杀死，如是则抛出异常；
  //2. 队列中在该事务之前的锁请求是否存在活写锁（状态不为ABORT），如是则继续阻塞，如非则将该请求的granted_置为真，并返回。
  while (!can_grant)  
  {
    for (auto &request : request_queue) 
    {
      if (request.lock_mode_ == LockMode::EXCLUSIVE && txn_table_[request.txn_id_]->GetState() != TransactionState::ABORTED) 
      {
        break;
      }
      if (request.txn_id_ == txn_id) 
      {
        can_grant = true;
        request.granted_ = true;
      }
    }

    if (!can_grant) 
    {
      cv.wait(lk);
    }

    if (txn->GetState() == TransactionState::ABORTED) 
    {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  return true;
}








//事务txn尝试获得元组ID为rid的元组写锁。
bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {

  //前置检查，如当前事务状态为ABORT返回假，如当前锁在收缩阶段，则将其状态置为ABORT并抛出异常。
  if (txn->GetState() == TransactionState::ABORTED) 
  {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) 
  {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  ///更新事务状态，获取锁请求队列，并将该请求插入队列，以及将该锁加入事务的拥有锁集合。
  //查询是否有将该事务锁请求阻塞的请求，当获取写锁时，队列中的任何一个锁请求都将造成其阻塞，当锁请求的事务优先级低时，将其杀死。
  //如存在不能杀死的请求，则该事务将被阻塞。当杀死了任一事务时，将唤醒该锁等待队列的所有事务。

  txn->SetState(TransactionState::GROWING);   
  std::unique_lock<std::mutex> lk(latch_);

  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();

  request_queue.emplace_back(txn_id, LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);
  txn_table_[txn_id] = txn;

  //Wound Wait
  //将请求加入队列，循环判断是否满足加写锁条件（当前没有读锁和写锁），满足则继续执行，否则阻塞；
  bool can_grant = true;
  bool is_kill = false;
  for (auto &request : request_queue)     //遍历锁请求队列
  {
    if (request.txn_id_ == txn_id)    //队头的就是当前事务ID的锁
    {
      request.granted_ = can_grant;       //同意当前事务id的锁请求
      break;
    }
    if (request.txn_id_ > txn_id)    //队列内的锁请求要比现在请求的锁请求更年轻，杀死
    {    
      txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);
      is_kill = true;
    } 
    else    //队列内有其他的锁存在
    {
      can_grant = false;
    }
  }
  
  if (is_kill) 
  {
    cv.notify_all();
  }


  //Wait lock   89089909
  /*
  等待锁可用，每当事务被唤醒时，检查其是否被杀死，如被杀死则抛出异常；如未被杀死，则检查队列前是否有任意未被杀死的锁请求，
  如没有则获得锁并将锁请求granted_置为真。
  */      
  while (!can_grant) 
  {
    auto it = request_queue.begin();
    while (txn_table_[it->txn_id_]->GetState() == TransactionState::ABORTED) 
    {
      ++it;
    }
    if (it->txn_id_ == txn_id) 
    {
      can_grant = true;
      it->granted_ = true;
    }
    if (!can_grant)
    {
      cv.wait(lk);
    }
    if (txn->GetState() == TransactionState::ABORTED) 
    {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }
  return true;


  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}





/*
将读锁升级为写锁。由于加写锁需要保证当前没有读锁，那么如果队列中有两个更新锁的请求，就会互相等待对方解读锁。
因此，为了判断是否出现这种情况，在队列中维护标志变量upgrading_，不允许队列中出现两个更新锁的请求。
将队列中的读锁请求改为写锁，循环判断是否满足加更新写锁条件（当前没有写锁，且只有唯一一个该读锁）。
*/
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {


  //判断当前事务是否被杀死，以及该元组的锁请求序列是否已经存在等待升级锁的其他事务，如是则杀死事务并抛出异常。
  //如通过检验，则将当前锁请求队列的upgrading_置为当前事务ID，以提示该队列存在一个等待升级锁的事务。
  if (txn->GetState() == TransactionState::ABORTED) 
  {
    return false;
  }
  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];
  if (lock_request_queue.upgrading_ != INVALID_TXN_ID) 
  {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  lock_request_queue.upgrading_ = txn_id;

//在Wound Wait中并未提及有关更新锁的行为，在这里将其每次唤醒尝试升级锁视为一次写锁获取，即每次其尝试升级锁时都将杀死队列前方将其阻塞的事务。
//其具体方法为，每次事务被唤醒时，先检查其是否被杀死，然后遍历锁请求队列在其前方的请求，如其优先级较低则将其杀死，如其优先级较高则将can_grant置为假，示意其将在之后被阻塞。如杀死任意一个事务，则唤醒其他事务。如can_grant为假则阻塞事务，如为真则更新锁请求的lock_mode_并将upgrading_初始化。
//当升级成功时，更新事务的拥有锁集合。
  bool can_grant = false;
  while (!can_grant) 
  {
    auto it = request_queue.begin();
    auto target = it;
    can_grant = true;
    bool is_kill = false;
    while (it != request_queue.end() && it->granted_) 
    {
      if (it->txn_id_ == txn_id) 
      {
        target = it;
      } else if (it->txn_id_ > txn_id) 
      {
        txn_table_[it->txn_id_]->SetState(TransactionState::ABORTED);
        is_kill = true;
      } 
      else 
      {
        can_grant = false;
      }
      ++it;
    }
    if (is_kill) 
    {
      cv.notify_all();
    }
    if (!can_grant) 
    {
      cv.wait(lk);
    } 
    else 
    {
      target->lock_mode_ = LockMode::EXCLUSIVE;
      lock_request_queue.upgrading_ = INVALID_TXN_ID;
    }
    if (txn->GetState() == TransactionState::ABORTED) 
    {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}



//当事务隔离级别为READ_COMMIT时，事务获得的读锁将在使用完毕后立即释放，因此该类事务不符合2PL规则，
//为了程序的兼容性，在这里认为READ_COMMIT事务在COMMIT或ABORT之前始终保持GROWING状态，对于其他事务，将在调用Unlock时转变为SHRINKING状态。
//在释放锁时，遍历锁请求对列并删除对应事务的锁请求，然后唤醒其他事务，并在事务的锁集合中删除该锁。
bool LockManager::Unlock(Transaction *txn, const RID &rid) {

  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) 
  {
    txn->SetState(TransactionState::SHRINKING);
  }

  std::unique_lock<std::mutex> lk(latch_);
  auto &lock_request_queue = lock_table_[rid];
  auto &request_queue = lock_request_queue.request_queue_;
  auto &cv = lock_request_queue.cv_;
  auto txn_id = txn->GetTransactionId();
  auto it = request_queue.begin();
  while (it->txn_id_ != txn_id) 
  {
    ++it;
  }

  request_queue.erase(it);
  cv.notify_all();                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
