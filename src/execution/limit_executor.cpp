//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {


//�����������Ԫ�����������ƻ��ڵ��ж����˾����������������Init()Ӧ�������Ӽƻ��ڵ��Init()�����������õ�ǰ����������

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan_) ,  child_executor_(child_executor.release()){  //�ͷ��κδ洢ָ�������Ȩ��
    limit_ = plan_->GetLimit();
    }  

void LimitExecutor::Init() {
    child_executor_->Init();
    limit_ = plan_->GetLimit();

}


//// Next()�������Ӽƻ��ڵ��Ԫ�鷵�أ�ֱ����������Ϊ0��
bool LimitExecutor::Next(Tuple *tuple, RID *rid) { 
    if(limit_ == 0 || !child_executor_->Next(tuple , rid))
    {
        return false;
    }

    limit_ --;
    return true;

}


    

}  // namespace bustub
