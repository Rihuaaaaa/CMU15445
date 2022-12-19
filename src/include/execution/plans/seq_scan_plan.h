//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_plan.h
//
// Identification: src/include/execution/plans/seq_scan_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * The SeqScanPlanNode represents a sequential table scan operation.
 * SeqScanPlanNode表示顺序表扫描操作。
 * 
 * It identifies a table to be scanned and an optional predicate.
 * 它标识一个要扫描的表和一个可选谓词。
 */
class SeqScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new SeqScanPlanNode instance.
   * @param output The output schema of this sequential scan plan node
   * @param predicate The predicate applied during the scan operation
   * @param table_oid The identifier of table to be scanned
   */
  SeqScanPlanNode(const Schema *output, const AbstractExpression *predicate, table_oid_t table_oid)
      : AbstractPlanNode(output, {}), predicate_{predicate}, table_oid_{table_oid} {}



  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::SeqScan; }



  /** @return The predicate to test tuples against; tuples should only be returned if they evaluate to true */
  const AbstractExpression *GetPredicate() const { return predicate_; }



  /** @return The identifier of the table that should be scanned  // 结合Catalog可以得到当前Executor需要的表格内容  */
  table_oid_t GetTableOid() const { return table_oid_; }



 private:
  /** The predicate that all returned tuples must satisfy   所有返回元组必须满足的谓词  */
  const AbstractExpression *predicate_;

  /** The table whose tuples should be scanned */
  table_oid_t table_oid_;       //要扫描其元组的表

  
};

}  // namespace bustub
