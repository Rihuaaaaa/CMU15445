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
��InsertExecutor�У������ض��ı��в���Ԫ�飬Ԫ�����Դ����Ϊ�����ƻ��ڵ���Զ����Ԫ�����顣�������Դ��ͨ��IsRawInsert()��ȡ��
�ڹ��캯���У���ȡ����Ҫ������TableInfo��Ԫ����Դ���Լ�����е�����������
*/
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan), child_executor_(child_executor.release() ) {
        table_oid_t oid = plan->TableOid();     //���õ�����Ԫ��ı�ı�ʶ��id
        table_info_ = exec_ctx->GetCatalog()->GetTable(oid);        //�õ�Ҫ������info��Ϣ

        is_raw_ = plan->IsRawInsert();  //' true '���������ֱֵ��Ƕ�뵽�ƻ��У�' false '�����һ���ṩԪ����Ӽƻ�

        if (is_raw_ == true) 
        {
            size_ = plan->RawValues().size();     
        }
        indexes_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}


//����Ԫ����ԴΪ�����ƻ��ڵ�ʱ��ִ�ж�Ӧ�ƻ��ڵ��Init()������
void InsertExecutor::Init() {

    if(!is_raw_)
    {
        child_executor_->Init();
    }
}


//���ݲ�ͬ��Ԫ����Դʵʩ��ͬ�Ĳ������

/*
Insert�ڵ㲻Ӧ��������κ�Ԫ�飬���������Ƿ��ؼ٣������еĲ��������Ӧ����һ��Next�б�ִ����ɡ�

�ڲ�������У�Ӧ��ʹ��InsertEntry���±��е�����������InsertEntry�Ĳ���Ӧ��KeyFromTuple�������졣(һ���Խ���ȫ������, Ȼ��ֱ�ӷ���false)
*/

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  Tuple tmp_tuple;
  RID tmp_rid;

  //����ԴΪ�Զ����Ԫ������ʱ�������й����Ӧ��Ԫ�飬��������У�
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
  

  //����ԴΪ�����ƻ��ڵ�ʱ��ͨ���ӽڵ��ȡ����Ԫ�鲢�����
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) 
  {
    if (table_info_->table_->InsertTuple(tmp_tuple, &tmp_rid, txn)) 
    {
      for (auto indexinfo : indexes_)   //��������
      {
        indexinfo->index_->InsertEntry(tmp_tuple.KeyFromTuple(*child_executor_->GetOutputSchema(),indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs()),tmp_rid, txn);
      }
    }
  }

  return false;

}


/*
��Ҫע�⣬Insert�ڵ㲻Ӧ��������κ�Ԫ�飬���������Ƿ��ؼ٣������еĲ��������Ӧ����һ��Next�б�ִ����ɡ�
����ԴΪ�Զ����Ԫ������ʱ�����ݱ�ģʽ�����Ӧ��Ԫ�飬��������У�����ԴΪ�����ƻ��ڵ�ʱ��ͨ���ӽڵ��ȡ����Ԫ�鲢�����
�ڲ�������У�Ӧ��ʹ��InsertEntry���±��е�����������InsertEntry�Ĳ���Ӧ��KeyFromTuple�������졣
*/
}  // namespace bustub
