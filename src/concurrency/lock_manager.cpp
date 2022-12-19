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

//����IDԽ�� ��֤��Խ�£�  ��/������ֱ�ָ����ID�ϴ��һ���ͽ�С��һ��



//����txn����Ԫ��IDΪrid�Ķ�����
bool LockManager::LockShared(Transaction *txn, const RID &rid) {

  //����ǰ���жϣ��������״̬ΪABORTʱ��ֱ�ӷ��ؼ�
  if (txn->GetState() == TransactionState::ABORTED) 
  {
    return false;
  }
  //��ǰ������״̬Ϊ������ʱ�����û�ȡ����������������ABORT���׳��쳣��
  if (txn->GetState() == TransactionState::SHRINKING) 
  {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  //������ĸ��뼶���READ_UNCOMMITTED����δ�ύ��ʱ���䲻Ӧ��ȡ������RU������Ҫ���������ݣ����Ի�ȡ����������ABORT���׳��쳣��
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) 
  {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  

  txn->SetState(TransactionState::GROWING);   //��ǰ���ж�ͨ�����򽫵�ǰ����״̬��ΪGROWING(��һ�׶�)������û����������������������ݽṹ��
  std::unique_lock<std::mutex> lk(latch_);

  auto &lock_request_queue = lock_table_[rid];    // ���õ�RID��Ӧ����������� ��lock_table����Ԫ��IDΪ�������������Ϊֵ�Ĺ�ϣ��
  auto &request_queue = lock_request_queue.request_queue_;  //�õ���������к��پ����õ������������г�Ա
  
  auto &cv = lock_request_queue.cv_;      //��������
  auto txn_id = txn->GetTransactionId();
  
  
  request_queue.emplace_back(txn_id, LockMode::SHARED);    //��ȡ��ӦԪ��ID����������м�����س�Ա�󣬽���ǰ����������������С�������������ŵ���һ��һ�����������࣬�������������
  txn->GetSharedLockSet()->emplace(rid);      //�ڸ����󱻼������ʱ����Ӧ������GetSharedLockSet()����Ԫ��ID��������ĳ��������ϣ�ʹ�ø�����ɱ��ʱ�ܽ�������Ӷ�����ɾ����
  txn_table_[txn_id] = txn;
  

  //Wound Wait : Kill all low priority transaction �ߵȵͣ������ �� �͵ȸߣ�������
  //�����������У�ѭ���ж��Ƿ�����Ӷ�����������ǰû��д���������������ִ�У�����������
  //Ϊ�˱���������������Ҫ��鵱ǰ�����Ƿ����ʹ���������������󣬶���X����˵, �������ǰ��������������ײ�ʱ�źϷ�
  //���ڹ�����(S)�����������Ҫ�Ӷ�������ôҪ�Ѷ������������д������abort���������ڶ������󣬿��Թ��档
  //�������ɱ�����κ�����������ͨ����������е���������cv���������ȴ���������ʹ�ñ�ɱ������������˳�������С�
  bool can_grant = true;
  bool is_kill = false;

  for (auto &request : request_queue)     //����������������ÿһ��������
  {
    if (request.lock_mode_ == LockMode::EXCLUSIVE)    //�����ǰ��ͷ��д��
    {
      if (request.txn_id_ > txn_id)      //��ǰ��ͷ��д�����ҵ�ǰ��ͷ������Ҫ�����ᣬɱ����ǰ����
      {
        txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);   //ֱ����ֹ��ͷ������
        is_kill = true;
      } 
      else      //�����can_grant��Ϊfalse��ʾ���񽫱�������
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

  if (is_kill)    // ȷʵɱ����, ����Ҫ����notify_all() ,��ɱ������һ����ʱ�������Ѹ����ȴ����е���������
  {
    cv.notify_all();
  }

  //Wait the lock
  //�������������������������Ҹ������ܽ���ɱ���������ѭ���ȴ�������ѭ���У����������������cv��wait����������ԭ�ӵ��ͷ�����
  //ÿ�ε��䱻����ʱ�����飺
  //1. �������Ƿ�ɱ�����������׳��쳣��
  //2. �������ڸ�����֮ǰ���������Ƿ���ڻ�д����״̬��ΪABORT�����������������������򽫸������granted_��Ϊ�棬�����ء�
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








//����txn���Ի��Ԫ��IDΪrid��Ԫ��д����
bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {

  //ǰ�ü�飬�統ǰ����״̬ΪABORT���ؼ٣��統ǰ���������׶Σ�����״̬��ΪABORT���׳��쳣��
  if (txn->GetState() == TransactionState::ABORTED) 
  {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) 
  {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  ///��������״̬����ȡ��������У����������������У��Լ����������������ӵ�������ϡ�
  //��ѯ�Ƿ��н����������������������󣬵���ȡд��ʱ�������е��κ�һ�������󶼽����������������������������ȼ���ʱ������ɱ����
  //����ڲ���ɱ��������������񽫱���������ɱ������һ����ʱ�������Ѹ����ȴ����е���������

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
  //�����������У�ѭ���ж��Ƿ������д����������ǰû�ж�����д���������������ִ�У�����������
  bool can_grant = true;
  bool is_kill = false;
  for (auto &request : request_queue)     //�������������
  {
    if (request.txn_id_ == txn_id)    //��ͷ�ľ��ǵ�ǰ����ID����
    {
      request.granted_ = can_grant;       //ͬ�⵱ǰ����id��������
      break;
    }
    if (request.txn_id_ > txn_id)    //�����ڵ�������Ҫ���������������������ᣬɱ��
    {    
      txn_table_[request.txn_id_]->SetState(TransactionState::ABORTED);
      is_kill = true;
    } 
    else    //��������������������
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
  �ȴ������ã�ÿ�����񱻻���ʱ��������Ƿ�ɱ�����类ɱ�����׳��쳣����δ��ɱ�����������ǰ�Ƿ�������δ��ɱ����������
  ��û������������������granted_��Ϊ�档
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
����������Ϊд�������ڼ�д����Ҫ��֤��ǰû�ж�������ô��������������������������󣬾ͻụ��ȴ��Է��������
��ˣ�Ϊ���ж��Ƿ��������������ڶ�����ά����־����upgrading_������������г�������������������
�������еĶ��������Ϊд����ѭ���ж��Ƿ�����Ӹ���д����������ǰû��д������ֻ��Ψһһ���ö�������
*/
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {


  //�жϵ�ǰ�����Ƿ�ɱ�����Լ���Ԫ��������������Ƿ��Ѿ����ڵȴ�����������������������ɱ�������׳��쳣��
  //��ͨ�����飬�򽫵�ǰ��������е�upgrading_��Ϊ��ǰ����ID������ʾ�ö��д���һ���ȴ�������������
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

//��Wound Wait�в�δ�ἰ�йظ���������Ϊ�������ｫ��ÿ�λ��ѳ�����������Ϊһ��д����ȡ����ÿ���䳢��������ʱ����ɱ������ǰ����������������
//����巽��Ϊ��ÿ�����񱻻���ʱ���ȼ�����Ƿ�ɱ����Ȼ������������������ǰ���������������ȼ��ϵ�����ɱ�����������ȼ��ϸ���can_grant��Ϊ�٣�ʾ���佫��֮����������ɱ������һ��������������������can_grantΪ��������������Ϊ��������������lock_mode_����upgrading_��ʼ����
//�������ɹ�ʱ�����������ӵ�������ϡ�
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



//��������뼶��ΪREAD_COMMITʱ�������õĶ�������ʹ����Ϻ������ͷţ���˸������񲻷���2PL����
//Ϊ�˳���ļ����ԣ���������ΪREAD_COMMIT������COMMIT��ABORT֮ǰʼ�ձ���GROWING״̬�������������񣬽��ڵ���Unlockʱת��ΪSHRINKING״̬��
//���ͷ���ʱ��������������в�ɾ����Ӧ�����������Ȼ�����������񣬲����������������ɾ��������
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
