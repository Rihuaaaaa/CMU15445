//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {


//实际的每一行元组 保存在 TableHeap类中 , 此类包含了用于插入、查找、更改、删除元组的所有函数接口，并且可以通过TableIterator迭代器顺序遍历其中的元组

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
: AbstractExecutor(exec_ctx), plan_(plan) , iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) 
{
    table_oid_t oid = plan->GetTableOid();
    table_info_ = exec_ctx->GetCatalog()->GetTable(oid);
    iter_ = table_info_->table_->Begin(exec_ctx->GetTransaction());
    end_ = table_info_->table_->End();

}


//执行计划节点所需的初始化操作，在这里重新设定表的迭代器，使得查询计划可以重新遍历表：
void SeqScanExecutor::Init() {
    iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
    end_ = table_info_->table_->End();
}


//  顺序扫描一张表, 每次调用Next()返回一个tuple即可,调用next方法的是 扫描算子的父节点，因此返回的tuple也是返回调用此next方法的父节点

/*
值得注意的是，表中的元组应当以out_schema的模式被重组。
在bustub中，所有查询计划节点的输出元组均通过out_schema中各列Column的ColumnValueExpression中的各种“Evaluate”方法构造，如Evaluate、EvaluateJoin、EvaluateAggregate。
*/

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    
    const Schema *out_schema = this->GetOutputSchema();
    auto exec_ctx = GetExecutorContext();
    Transaction *txn = exec_ctx->GetTransaction();
    TransactionManager *txn_mgr = exec_ctx->GetTransactionManager();
    LockManager *lock_mgr = exec_ctx->GetLockManager();
    Schema table_schema = table_info_->schema_;

    while (iter_ != end_) 
    {
        Tuple table_tuple = *iter_;
        *rid = table_tuple.GetRid();
        if (txn->GetSharedLockSet()->count(*rid) == 0U && txn->GetExclusiveLockSet()->count(*rid) == 0U) 
        {
            if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && !lock_mgr->LockShared(txn, *rid)) 
            {
                txn_mgr->Abort(txn);
            }
        }
        std::vector<Value> values;
        for (const auto &col : GetOutputSchema()->GetColumns()) 
        {
            values.emplace_back(col.GetExpr()->Evaluate(&table_tuple, &table_schema));
        }
        *tuple = Tuple(values, out_schema);
        auto *predicate = plan_->GetPredicate();
        if (predicate == nullptr || predicate->Evaluate(tuple, out_schema).GetAs<bool>()) 
        {
            ++iter_;
            if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) 
            {
                lock_mgr->Unlock(txn, *rid);
            }
            return true;
        }
        if (txn->GetSharedLockSet()->count(*rid) != 0U && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) 
        {
            lock_mgr->Unlock(txn, *rid);
        }
        
        ++iter_;
    }
    return false;

}
    
}  // namespace bustub
