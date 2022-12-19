//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan), child_executor_(child_executor.release()) {

      table_oid_t oid = plan->TableOid();     //先拿到表的id
      auto catalog = exec_ctx->GetCatalog();
      table_info_ = catalog->GetTable(oid);
      indexes_ = catalog->GetTableIndexes(table_info_->name_);
    }


void UpdateExecutor::Init() {

  child_executor_->Init();
}



bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
  
  Tuple src_tuple;      //一行元组 
  auto *txn =  this->GetExecutorContext()->GetTransaction();    //拿到正在运行的事务

  while(child_executor_->Next(&src_tuple , rid))  
  {
    *tuple = this->GenerateUpdatedTuple(src_tuple);  // 利用GenerateUpdatedTuple方法将源元组(原来的行)更新为新元组



    //在更新索引时，删除表中与源元组对应的所有索引记录，并增加与新元组对应的索引记录。
    if(table_info_->table_->UpdateTuple(*tuple , *rid , txn))
    {
      for(auto indexinfo : indexes_)
      {
        indexinfo->index_->DeleteEntry( tuple->KeyFromTuple(*child_executor_->GetOutputSchema() , indexinfo->key_schema_, indexinfo-> index_->GetKeyAttrs())
          , *rid , txn);
        indexinfo->index_->InsertEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema() , indexinfo->key_schema_, indexinfo-> index_->GetKeyAttrs())
          , *rid , txn);
      }
    }
  }
  
  return false; 
  
}





Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}



}  // namespace bustub
