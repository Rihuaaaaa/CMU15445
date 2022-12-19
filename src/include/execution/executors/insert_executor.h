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
 * InsertExecutor executes an insert on a table.    InsertExecutor�ڱ���ִ�в��������
 *
 * Unlike UPDATE and DELETE, inserted values may either be embedded in the plan itself or be pulled from a child executor.
  ��UPDATE��DELETE��ͬ�������ֵ����Ƕ�뵽�ƻ������У�Ҳ���Դ���ִ��������ȡ��
 */

class InsertExecutor : public AbstractExecutor {

 public:
  /**
   * Construct a new InsertExecutor instance.   ����һ���µ�InsertExecutorʵ��
   * 
   * @param exec_ctx The executor context     ִ�г���������
   * @param plan The insert plan to be executed   Ҫִ�еĲ���ƻ�
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`) ��������ȡ����Ԫ�����ִ����(������' nullptr ')
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

  std::unique_ptr<AbstractExecutor> child_executor_;    //��ִ���� , ��ռ����ָ�룬ֻ��һ��ָ���������ڴ�

  TableInfo *table_info_;  // Ҫ����ı��info

  bool is_raw_;

  uint32_t size_;

  std::vector<IndexInfo *> indexes_;  // ��������йص���������, ���������ܲ�����
};

}  // namespace bustub
