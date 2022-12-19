//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {
    


/*
在InsertExecutor中，其向特定的表中插入元组，元组的来源可能为其他计划节点或自定义的元组数组。其具体来源可通过IsRawInsert()提取。
在构造函数中，提取其所要插入表的TableInfo，元组来源，以及与表中的所有索引：
*/
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan), child_executor_(child_executor.release() ) {
        table_oid_t oid = plan->TableOid();     //先拿到插入元组的表的标识符id
        table_info_ = exec_ctx->GetCatalog()->GetTable(oid);        //拿到要插入表的info信息

        is_raw_ = plan->IsRawInsert();  //' true '如果将插入值直接嵌入到计划中，' false '如果有一个提供元组的子计划

        if (is_raw_ == true) 
        {
            size_ = plan->RawValues().size();     
        }
        indexes_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}


//当其元组来源为其他计划节点时，执行对应计划节点的Init()方法：
void InsertExecutor::Init() {

    if(!is_raw_)
    {
        child_executor_->Init();
    }
}


//根据不同的元组来源实施不同的插入策略

/*
Insert节点不应向外输出任何元组，所以其总是返回假，即所有的插入操作均应当在一次Next中被执行完成。

在插入过程中，应当使用InsertEntry更新表中的所有索引，InsertEntry的参数应由KeyFromTuple方法构造。(一次性进行全部操作, 然后直接返回false)
*/

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  Tuple tmp_tuple;
  RID tmp_rid;

  //当来源为自定义的元组数组时，根据列构造对应的元组，并插入表中；
  if (is_raw_) 
  {
    for (uint32_t idx = 0; idx < size_; idx++) 
    {
      const std::vector<Value> &raw_value = plan_->RawValuesAt(idx);
      tmp_tuple = Tuple(raw_value, &table_info_->schema_);
      if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) 
      {
        for (auto indexinfo : indexes_) 
        {
          indexinfo->index_->InsertEntry(tmp_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),tmp_rid, txn);
        }
      }
    }
    return false;
  }
  

  //当来源为其他计划节点时，通过子节点获取所有元组并插入表。
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) 
  {
    if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) 
    {
      for (auto indexinfo : indexes_)   //更新索引
      {
        indexinfo->index_->InsertEntry(tmp_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(),indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),tmp_rid, txn);
      }
    }
  }

  return false;

}


/*
需要注意，Insert节点不应向外输出任何元组，所以其总是返回假，即所有的插入操作均应当在一次Next中被执行完成。
当来源为自定义的元组数组时，根据表模式构造对应的元组，并插入表中；当来源为其他计划节点时，通过子节点获取所有元组并插入表。
在插入过程中，应当使用InsertEntry更新表中的所有索引，InsertEntry的参数应由KeyFromTuple方法构造。
*/
}  // namespace bustub
