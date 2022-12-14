//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:

  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);



  /** Initialize the sequential scan */
  void Init() override;



  /**
   * Yield the next tuple from the sequential scan. 从顺序扫描中生成下一个元组
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;



  /** @return The output schema for the sequential scan  顺序扫描的输出模式 */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }




 private:


  /** The sequential scan plan node to be executed  需要执行的顺序扫描计划节点 */
  const SeqScanPlanNode *plan_;

  //增加TableInfo、及迭代器私有成员，用于访问表信息和遍历表。
  TableInfo *table_info_;
  TableIterator iter_;
  TableIterator end_;

};
}  // namespace bustub
