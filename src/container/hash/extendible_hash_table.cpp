//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {



//Ϊ��ϣ�����һ��Ŀ¼ҳ���Ͱҳ�棬������Ŀ¼ҳ���page_id��Ա������ϣ����׸�Ŀ¼��ָ���Ͱ����󣬲�Ҫ���ǵ���UnpinPage�򻺳�ظ�֪ҳ���ʹ����ϡ�
template <typename KeyType, typename ValueType, typename KeyComparator>   //����
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
: buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {

  //new��һ��Ŀ¼ҳ  �˴��ǽ� Page* ����ǿ��ת��Ϊ HashTableDirectoryPage* ����
  HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
  
  dir_page->SetPageId(directory_page_id_);

  page_id_t new_bucket_id;
  buffer_pool_manager->NewPage(&new_bucket_id);
  dir_page->SetBucketPageId(0, new_bucket_id);
  
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
}



 
/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}


// ����ӳ�䵽Ŀ¼����
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hashed_key = Hash(key);
  uint32_t mask = dir_page->GetGlobalDepthMask();
  return mask & hashed_key;
}

//��ȡ�����Ӧ��Ͱpage_id��
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t idx = KeyToDirectoryIndex(key , dir_page);
  return dir_page->GetBucketPageId(idx);
}


//�ӻ���ع�������ȡĿ¼ҳ��
template <typename KeyType, typename ValueType, typename KeyComparator>   
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  // HashTableDirectoryPage����ǿ��ת��Ϊ Page����
  return reinterpret_cast<HashTableDirectoryPage*>(buffer_pool_manager_->FetchPage(directory_page_id_));
}



//ʹ��Ͱ��page_id�ӻ���ع�������ȡͰҳ�档
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(buffer_pool_manager_->FetchPage(bucket_page_id));
}



/*****************************************************************************
 * SEARCH     �ӹ�ϣ���ж�ȡ���ƥ�������ֵ���
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {

  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  table_latch_.RLock();   // ��ϣ��Ķ�������Ŀ¼ҳ��(��������S��)

  page_id_t bucket_page_id = KeyToPageId(key , dir_page);
  HASH_TABLE_BUCKET_TYPE * bucket = FetchBucketPage(bucket_page_id);

  Page *p = reinterpret_cast<Page*>(bucket);      //HASH_TABLE_BUCKET_TYPE����ǿ��ת��Ϊ Page*
  p->RLatch();      //ʹ��Ͱ�Ķ�������Ͱҳ��

  bool res  = bucket->GetValue(key , comparator_ , result);
  p->RUnlatch();    //�ͷ�ҳ�����
  table_latch_.RUnlock();   // Release a read latch.

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));

  return res;

}



/*****************************************************************************
 * ���ϣ������ֵ�ԣ�����ܻᵼ��Ͱ�ķ��Ѻͱ�����ţ������Ҫ��֤Ŀ¼ҳ��Ķ��̰߳�ȫ��
 * һ�ֱȽϼ򵥵ı�֤�̰߳�ȫ�ķ���Ϊ���ڲ���Ŀ¼ҳ��ǰ��Ŀ¼ҳ��Ӷ����������ּ�����ʽʹ��Insert����������������ϣ��
 * ������Ӱ���˹�ϣ��Ĳ����ԡ�����ע�⵽��������ŵķ���Ƶ�ʲ����ߣ���Ŀ¼ҳ��Ĳ������ڶ���д�ٵ������
 * ��˿���ʹ���ֹ����ķ����Ż��������ܣ�����Insert������ʱ�����ֶ�����ֻ����ҪͰ����ʱ���»�ö�����
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {

  //1 ��ȡĿ¼ҳ���Ͱҳ�棬�ڼ�ȫ�ֶ�����Ͱд������Ͱ�Ƿ����������������ͷ�����������UnpinPage�ͷ�ҳ�棬Ȼ�����SplitInsertʵ��Ͱ���ѺͲ��룻
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();  
  table_latch_.RLock();   //���϶���
  page_id_t buck_page_id = KeyToPageId(key , dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(buck_page_id);
  Page *p = reinterpret_cast<Page*>(bucket);

  p->WLatch();      //ҳ��д��
  if(bucket->IsFull())    //���bucket���˵�ʱ����Ҫ����, Ҳ���ǵ��ú����SplitInsert
  {
    p->WUnlatch();
    table_latch_.RUnlock();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
    return SplitInsert(transaction , key , value);      //Ͱ����Ҫ����
  }

  //2 �統ǰͰδ������ֱ�����Ͱҳ������ֵ�ԣ����ͷ�����ҳ�漴�ɡ�
  bool res = bucket->Insert(key , value , comparator_);
  p->WUnlatch();
  table_latch_.RUnlock();

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
  return res;

}

/*
���ȣ���ȡĿ¼ҳ�沢��ȫ��д���������ȫ��д�������������߳̾��������ˣ���˿��Է��ĵĲ������ݳ�Ա��
����ע�⵽����Insert���ͷŶ�����SplitInsert���ͷ�д������ڿ�϶�������߳̿����ڸÿ�϶�б����ȣ��Ӷ��ı�Ͱҳ���Ŀ¼ҳ�����ݡ�
��ˣ���������Ҫ������Ŀ¼ҳ���л�ȡ��ϣ������Ӧ��Ͱҳ�棨������Insert���ж�������ҳ�治��ͬһҳ�棩��������Ӧ��Ͱҳ���Ƿ�������
��Ͱҳ����Ȼ�����ģ��������Ͱ����ȡԭͰҳ���Ԫ���ݡ�������Ͱ���Ѻ�����������Ͱ�Կ��������ģ���������������ѭ���Խ�������⡣
*/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();     //��д��
  while (true)
  {
    page_id_t bucket_page_id = KeyToPageId(key , dir_page);     //��ȡ�����Ӧ��Ͱpage_id��
    uint32_t bucket_idx = KeyToDirectoryIndex(key , dir_page);
    HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage();

    if(bucket ->IsFull())
    {
      uint32_t global_depth = dir_page->GetGlobalDepth();
      uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
      page_id_t new_bucket_id = 0 ; 
      HASH_TABLE_BUCKET_TYPE * new_bucket = reinterpret_cast<HASH_TABLE_TYPE*>(buffer_pool_manager_->NewPage(&new_bucket_id));
      assert(new_bucket != nullptr);
      
      if(global_depth == local_depth)       //��Ҫ���б���չ��Ͱ����
      {
        // if i == ij, extand the bucket dir, and split the bucket
        uint32_t bucket_num = 1 << global_depth;    //��������
        for(uint32_t i = 0; i<bucket_num ; i++)
        {
          dir_page->SetBucketPageId(i+bucket_num , dir_page->GetBucketPageId(i));
          dir_page->SetLocalDepth(i+bucket_num , dir_page->GetLocalDepth(i));
        } 
        dir_page->IncrGlobalDepth();
        dir_page->SetBucketPageId(bucket_idx  + bucket_num , new_bucket_id);
        dir_page->IncrLocalDepth(bucket_idx);
        dir_page->IncrLocalDepth(bucket_idx + bucket_num);
        global_depth++;
      }
      else      //  �������Ͱ����
      {
        // if i > ij, split the bucket
        // more than one records point to the bucket
        // the records' low ij bits are same
        // and the high (i - ij) bits are index of the records point to the same bucket
        uint32_t mask = (1<< local_depth)-1;
        uint32_t base_idx = mask & bucket_idx;
        uint32_t records_num = 1 << (global_depth - local_depth - 1);
        uint32_t step = (1<< local_depth);
        uint32_t idx = base_idx;
        for(uint32_t i = 0 ; i<records_num ; i++)
        {
          dir_page->IncrLocalDepth(idx);
          idx += step * 2;
        }
        idx = base_idx +  step;
        for(uint32_t i = 0 ; i<records_num ; i++)
        {
          dir_page->SetBucketPageId(idx , new_bucket_id);
          dir_page->IncrLocalDepth(idx);
          idx += step * 2;
        }
      }
      // rehash all records in bucket j
      //�����Ͱ���Ѻ�Ӧ����ԭͰҳ���еļ�¼���²����ϣ�����ڼ�¼�ĵ�i-1λ����ԭͰҳ�����Ͱҳ���Ӧ��
      //��˼�¼�����Ͱҳ�������ΪԭͰҳ�����Ͱҳ������ѡ�������²������¼���ͷ���Ͱҳ���ԭͰҳ�档
      for(uint32_t = 0 ; i<BUCKET_ARRAY_SIZE ; i++)
      {
        KeyType j_key = bucket->KeyAt(i);
        ValueType j_value = bucket->ValueAt(i);
        bucket->RemoveAt(i);
        if(KeyToPageId(j_key , dir_page) == bucket_page_id)
        {
          bucket->Insert(j_key , j_value, comparator_);
        }
        else
        {
          new_bucket->Insert(j_key , j_value , comparator_);
        }
      }

      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
      assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
    }
    else    //����ǰ��ֵ���������Ͱҳ��ǿգ��������߳��޸Ļ�Ͱ���Ѻ���������ֱ�Ӳ����ֵ�ԣ����ͷ�����ҳ�棬��������������
    {
      bool res = bucket->Insert(key , value , comparator_);
      table_latch_.WUnlock();
      assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
      return res;
    }
  }
  return false;
}





/*****************************************************************************
 * REMOVE
 * �ӹ�ϣ����ɾ����Ӧ�ļ�ֵ�ԣ����Ż�˼����Insert��ͬ������Ͱ�ĺϲ�����Ƶ���������ɾ����ֵ��ʱ����ȡȫ�ֶ�����
 * ֻ����Ҫ�ϲ�Ͱʱ��ȡȫ��д������ɾ����ͰΪ����Ŀ¼��ľֲ���Ȳ�Ϊ��ʱ���ͷŶ���������Merge���Ժϲ�ҳ�棬����ͷ�����ҳ�沢���ء�
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.RLock();
  page_id_t buck_page_id = KeyToPageId(key , dir_page);
  uint32_t bucket_idx = KeyToDirectoryIndex(key , dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(buck_page_id);
  Page * p = reinterpret_cast<Page*>(bucket);

  p->WLatch();
  bool res = bucket->Remove(key , value , comparator_);
  p->WUnlatch();
  if(bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) !=0 )
  {
    table_latch_.RUnlock();
    this->Merge(transaction,key , value);
  }
  else
  {
    table_latch_.RUnlock();
  }

  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));

  return res;
}




/*****************************************************************************
 * MERGE
 * ������ȡд������Ҫ�����ж��Ƿ�����ϲ��������Է�ֹ���ͷ����Ŀ�϶ʱҳ�汻���ģ��ںϲ���ִ��ʱ��
 * ��Ҫ�жϵ�ǰĿ¼ҳ���Ƿ����������������������������ݼ�ȫ����ȼ����������������ͷ�ҳ���д��
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) 
  {
    uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
    uint32_t global_depth = dir_page->GetGlobalDepth();
    // How to find the bucket to Merge?
    // Answer: After Merge, the records, which pointed to the Merged Bucket,
    // have low (local_depth - 1) bits same
    // therefore, reverse the low local_depth can get the idx point to the bucket to Merge
    uint32_t merged_bucket_idx = bucket_idx ^ (1 << (local_depth - 1));
    page_id_t merged_page_id = dir_page->GetBucketPageId(merged_bucket_idx);
    HASH_TABLE_BUCKET_TYPE *merged_bucket = FetchBucketPage(merged_page_id);
    if (dir_page->GetLocalDepth(merged_bucket_idx) == local_depth && merged_bucket->IsEmpty()) 
    {
      local_depth--;
      uint32_t mask = (1 << local_depth) - 1;
      uint32_t idx = mask & bucket_idx;
      uint32_t records_num = 1 << (global_depth - local_depth);
      uint32_t step = (1 << local_depth);

      for (uint32_t i = 0; i < records_num; i++) 
      {
        dir_page->SetBucketPageId(idx, bucket_page_id);
        dir_page->DecrLocalDepth(idx);
        idx += step;
      }
      buffer_pool_manager_->DeletePage(merged_page_id);
    }
    if (dir_page->CanShrink()) 
    {
      dir_page->DecrGlobalDepth();
    }
    assert(buffer_pool_manager_->UnpinPage(merged_page_id, true, nullptr));
  }
  table_latch_.WUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
