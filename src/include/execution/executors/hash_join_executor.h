//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"


namespace bustub
{

  struct HashJoinKey{

    Value value_;
    bool operator==(const HashJoinKey &other) const { 
      return value_.CompareEquals(other.value_) == CmpBool::CmpTrue; 
    }
  };
  
  
} // namespace bustub


//对于哈希函数，使用bustub中内置的HashUtil::HashValue即可。在这里，通过阅读代码可以发现，bustub中连接操作的连接键仅由元组的一个属性列组成，
//因此在连接键中仅需存放单个属性列的具体值Value，而不需同聚合操作一样存放属性列的组合Vector<Value>。连接键通过Value的CompareEquals进行比较。
namespace std
{
  template<>
  struct hash<bustub::HashJoinKey>{

    std::size_t operator()(const bustub::HashJoinKey &key) const { 
      return bustub::HashUtil::HashValue(&key.value_); 
    
    }
  };
  




} // namespace std





namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;

  std::unique_ptr<AbstractExecutor> right_child_;


  //使用unordered_multimap直接存放对于连接键的元组，从原理上其与使用普通哈希表并遍历桶的过程上是等价的，但使用multimap会使实现代码更加简单。
  std::unordered_multimap<HashJoinKey, Tuple> hash_map_{};    //key存放连接点， value 放左表的一整行(tuple)

  std::vector<Tuple> output_buffer_;    
};

}  // namespace bustub
