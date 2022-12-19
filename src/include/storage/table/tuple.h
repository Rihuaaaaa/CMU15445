//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple.h
//
// Identification: src/include/storage/table/tuple.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "catalog/schema.h"
#include "common/rid.h"
#include "type/value.h"

namespace bustub {

/**
 * Tuple format:
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */



//相当于表里面的一行，存储了一行value。长度由shema决定，每个column可以在tuple中对应一个value 
class Tuple {  
  friend class TablePage;
  friend class TableHeap;
  friend class TableIterator;


 public:
  // Default constructor (to create a dummy tuple)
  Tuple() = default;

  // constructor for table heap tuple
  explicit Tuple(RID rid) : rid_(rid) {}

  // constructor for creating a new tuple based on input value
  Tuple(std::vector<Value> values, const Schema *schema);

  // copy constructor, deep copy
  Tuple(const Tuple &other);

  // assign operator, deep copy
  Tuple &operator=(const Tuple &other);

  ~Tuple() {
    if (allocated_) {
      delete[] data_;
    }
    allocated_ = false;
    data_ = nullptr;
  }

  // 就是将数据二进制化或者反过来，用于存储 
  // serialize tuple data 序列化数据元组
  void SerializeTo(char *storage) const;

  // deserialize tuple data(deep copy)   反序列化元组数据(深度复制)
  void DeserializeFrom(const char *storage);

  // return RID of current tuple
  inline RID GetRid() const { return rid_; }

  // Get the address of this tuple in the table's backing store 在表的后台存储区中获取此元组的地址  // 返回数据指针
  inline char *GetData() const { return data_; }

  // Get length of the tuple, including varchar legth // 返回tuple的长度（bits）
  inline uint32_t GetLength() const { return size_; }

  // Get the value of a specified column (const)
  // checks the schema to see how to return the Value. // 返回指定colum_idx位置的值
  Value GetValue(const Schema *schema, uint32_t column_idx) const;

  // Generates a key tuple given schemas and attributes
  Tuple KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs);

  // Is the column value null ?
  inline bool IsNull(const Schema *schema, uint32_t column_idx) const {
    Value value = GetValue(schema, column_idx);
    return value.IsNull();
  }
  inline bool IsAllocated() { return allocated_; }

  std::string ToString(const Schema *schema) const;

 private:
  // Get the starting storage address of specific column
  const char *GetDataPtr(const Schema *schema, uint32_t column_idx) const;

  bool allocated_{false};  // is allocated?
  RID rid_{};              // if pointing to the table heap, the rid is valid
  uint32_t size_{0};
  char *data_{nullptr};
};

}  // namespace bustub
