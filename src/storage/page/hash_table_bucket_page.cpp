//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {    


//遍历整个bucket,Readable()的位置, 用cmp比较一下key值, 相同的话就存入*result里 ， 提取bucket中 键为key的所有值
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {

  //遍历所有occupied_为1的位，并将键匹配的值插入result数组即可
  bool res = false;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) 
  {
    if (!IsOccupied(bucket_idx))  //occupied 数组代表bucket中的slot是否被访问过(不为1)
    {      
      break;
    }
    if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0)     //readable 数组保存该bucket的slot是否是一个空位(没有被占用)
    {
      result->push_back(array_[bucket_idx].second);     // 放进实值 :  
      res = true;
    }
  }
  return res;
}
  


//找一个空位插入即可, 是不允许相同的(key, val), 允许只有相同的key, 当然, 没有空位了就直接返回吧
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {

  size_t slot_idx = 0;
  bool slot_found = false;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) 
  {
    if (!slot_found && (!IsReadable(bucket_idx) || !IsOccupied(bucket_idx)))    //  !slot_found && (未被占用 || 未被使用过)
    {
      slot_found = true;
      slot_idx = bucket_idx;
    }
    if (!IsOccupied(bucket_idx))    //如果未被使用过直接跳出循环，执行73行插入操作
    {
      break;
    }
    if (IsReadable(bucket_idx))     //已经被占用  
    {
      if(cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx))
      {
        return false;
      }
    }
  }

  if (slot_found) 
  {
    SetReadable(slot_idx);
    SetOccupied(slot_idx);
    array_[slot_idx] = MappingType(key, value);
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) 
  {
    if (!IsOccupied(bucket_idx))    //如果该位未被使用过
    {    
      break;
    }
    if (IsReadable(bucket_idx) )    //如果该位被占用了
    {
      if(cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx))  //是不允许相同的(key, val), 允许只有相同的key
      {
        RemoveAt(bucket_idx);   //删除该下标的键值对
        return true;
      }
    }
  return false;
}



template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;          // array_[] 是保存map<>键值对的一个数 直接访问key（first）即可
}



template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}



template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] &= ~(1 << (7 - (bucket_idx % 8)));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return (occupied_[bucket_idx / 8] & (1 << (7 - (bucket_idx % 8)))) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / 8] |= 1 << (7 - (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return (readable_[bucket_idx / 8] & (1 << (7 - (bucket_idx % 8)))) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] |= 1 << (7 - (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}


//返回桶中的键值对个数，遍历即可。IsFull()和IsEmpty()直接复用NumReadable()实现。
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  
  uint32_t ret = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) 
  {
    if(!IsOccupied(bucket_idx))    //如果该位未被使用过
    {
      break;
    }
    if (IsReadable(bucket_idx))   //如果该位已经被占用(代表有键值对存在)
    {
      ret++;
    }
  }
  return ret;
}




template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}



template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
}