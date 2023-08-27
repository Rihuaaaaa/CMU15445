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



//为哈希表分配一个目录页面和桶页面，并设置目录页面的page_id成员、将哈希表的首个目录项指向该桶。最后，不要忘记调用UnpinPage向缓冲池告知页面的使用完毕。
template <typename KeyType, typename ValueType, typename KeyComparator>   //构造
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
: buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {

  //new出一个目录页  此处是将 Page* 类型强制转换为 HashTableDirectoryPage* 类型
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


// 将键映射到目录索引
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hashed_key = Hash(key);
  uint32_t mask = dir_page->GetGlobalDepthMask();
  return mask & hashed_key;
}

//获取与键对应的桶page_id。
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t idx = KeyToDirectoryIndex(key , dir_page);
  return dir_page->GetBucketPageId(idx);
}


//从缓冲池管理器获取目录页。
template <typename KeyType, typename ValueType, typename KeyComparator>   
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  // HashTableDirectoryPage类型强制转换为 Page类型
  return reinterpret_cast<HashTableDirectoryPage*>(buffer_pool_manager_->FetchPage(directory_page_id_));
}



//使用桶的page_id从缓冲池管理器获取桶页面。
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(buffer_pool_manager_->FetchPage(bucket_page_id));
}



/*****************************************************************************
 * SEARCH     从哈希表中读取与键匹配的所有值结果
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {

  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  table_latch_.RLock();   // 哈希表的读锁保护目录页面(读锁就是S锁)

  page_id_t bucket_page_id = KeyToPageId(key , dir_page);
  HASH_TABLE_BUCKET_TYPE * bucket = FetchBucketPage(bucket_page_id);

  Page *p = reinterpret_cast<Page*>(bucket);      //HASH_TABLE_BUCKET_TYPE类型强制转换为 Page*
  p->RLatch();      //使用桶的读锁保护桶页面

  bool res  = bucket->GetValue(key , comparator_ , result);
  p->RUnlatch();    //释放页面读锁
  table_latch_.RUnlock();   // Release a read latch.

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));

  return res;

}



/*****************************************************************************
 * 向哈希表插入键值对，这可能会导致桶的分裂和表的扩张，因此需要保证目录页面的读线程安全，
 * 一种比较简单的保证线程安全的方法为：在操作目录页面前对目录页面加读锁。但这种加锁方式使得Insert函数阻塞了整个哈希表，
 * 这严重影响了哈希表的并发性。可以注意到，表的扩张的发生频率并不高，对目录页面的操作属于读多写少的情况，
 * 因此可以使用乐观锁的方法优化并发性能，其在Insert被调用时仅保持读锁，只在需要桶分裂时重新获得读锁。
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {

  //1 获取目录页面和桶页面，在加全局读锁和桶写锁后检查桶是否已满，如已满则释放锁，并调用UnpinPage释放页面，然后调用SplitInsert实现桶分裂和插入；
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();  
  table_latch_.RLock();   //表上读锁
  page_id_t buck_page_id = KeyToPageId(key , dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(buck_page_id);
  Page *p = reinterpret_cast<Page*>(bucket);

  p->WLatch();      //页上写锁
  if(bucket->IsFull())    //如果bucket满了的时候需要扩容, 也就是调用后面的SplitInsert
  {
    p->WUnlatch();
    table_latch_.RUnlock();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
    return SplitInsert(transaction , key , value);      //桶满了要调用
  }

  //2 如当前桶未满，则直接向该桶页面插入键值对，并释放锁和页面即可。
  bool res = bucket->Insert(key , value , comparator_);
  p->WUnlatch();
  table_latch_.RUnlock();

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
  return res;

}

/*
首先，获取目录页面并加全局写锁，在添加全局写锁后，其他所有线程均被阻塞了，因此可以放心的操作数据成员。
不难注意到，在Insert中释放读锁和SplitInsert中释放写锁间存在空隙，其他线程可能在该空隙中被调度，从而改变桶页面或目录页面数据。
因此，在这里需要重新在目录页面中获取哈希键所对应的桶页面（可能与Insert中判断已满的页面不是同一页面），并检查对应的桶页面是否已满。
如桶页面仍然是满的，则分配新桶和提取原桶页面的元数据。在由于桶分裂后仍所需插入的桶仍可能是满的，因此在这这里进行循环以解决该问题。
*/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();     //表写锁
  while (true)
  {
    page_id_t bucket_page_id = KeyToPageId(key , dir_page);     //获取与键对应的桶page_id。
    uint32_t bucket_idx = KeyToDirectoryIndex(key , dir_page);
    HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage();

    if(bucket ->IsFull())
    {
      uint32_t global_depth = dir_page->GetGlobalDepth();
      uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
      page_id_t new_bucket_id = 0 ; 
      HASH_TABLE_BUCKET_TYPE * new_bucket = reinterpret_cast<HASH_TABLE_TYPE*>(buffer_pool_manager_->NewPage(&new_bucket_id));
      assert(new_bucket != nullptr);
      
      if(global_depth == local_depth)       //需要进行表扩展和桶分裂
      {
        // if i == ij, extand the bucket dir, and split the bucket
        uint32_t bucket_num = 1 << global_depth;    //扩容两倍
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
      else      //  仅需进行桶分裂
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
      //在完成桶分裂后，应当将原桶页面中的记录重新插入哈希表，由于记录的低i-1位仅与原桶页面和新桶页面对应，
      //因此记录插入的桶页面仅可能为原桶页面和新桶页面两个选择。在重新插入完记录后，释放新桶页面和原桶页面。
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
    else    //若当前键值对所插入的桶页面非空（被其他线程修改或桶分裂后结果），则直接插入键值对，并释放锁和页面，并将插入结果返回
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
 * 从哈希表中删除对应的键值对，其优化思想与Insert相同，由于桶的合并并不频繁，因此在删除键值对时仅获取全局读锁，
 * 只在需要合并桶时获取全局写锁。当删除后桶为空且目录项的局部深度不为零时，释放读锁并调用Merge尝试合并页面，随后释放锁和页面并返回。
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
 * 函数获取写锁后，需要重新判断是否满足合并条件，以防止在释放锁的空隙时页面被更改，在合并被执行时，
 * 需要判断当前目录页面是否可以收缩，如可以搜索在这里仅需递减全局深度即可完成收缩，最后释放页面和写锁
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
