//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * InsertExecutor executes an insert on a table.    InsertExecutor在表上执行插入操作。
 *
 * Unlike UPDATE and DELETE, inserted values may either be embedded in the plan itself or be pulled from a child executor.
  与UPDATE和DELETE不同，插入的值可以嵌入到计划本身中，也可以从子执行器中提取。
 */

class InsertExecutor : public AbstractExecutor {

 public:
  /**
   * Construct a new InsertExecutor instance.   构造一个新的InsertExecutor实例
   * 
   * @param exec_ctx The executor context     执行程序上下文
   * @param plan The insert plan to be executed   要执行的插入计划
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`) 从其中提取插入元组的子执行器(可能是' nullptr ')
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);



  /** Initialize the insert */
  void Init() override;



  /**
   * Yield the next tuple from the insert.
   * @param[out] tuple The next tuple produced by the insert
   * @param[out] rid The next tuple RID produced by the insert
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: InsertExecutor::Next() does not use the `tuple` out-parameter.
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   */
  bool Next([[maybe_unused]] Tuple *tuple, RID *rid) override;



  /** @return The output schema for the insert */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };






 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> child_executor_;    //子执行器 , 独占智能指针，只让一个指针管理这块内存

  TableInfo *table_info_;  // 要插入的表的info

  bool is_raw_;

  uint32_t size_;

  std::vector<IndexInfo *> indexes_;  // 和这个表有关的所有索引, 更新索引避不开的
};

}  // namespace bustub
