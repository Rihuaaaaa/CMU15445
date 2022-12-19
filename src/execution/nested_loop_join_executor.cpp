//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {


//NestedLoopJoinExecutor将两个子计划节点中的所有元组进行连接操作，每次调用Next()方法其向父节点返回一个符合连接谓词的连接元组： 
//就是将两个孩子节点传回来的tuples两两经过谓词判断（就是判断指定columns位置的value是不是相等），将配对的tuple根据outputSchema组合成输出tuple。

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),
      left_executor_(left_executor.release()),
      right_executor_(right_executor.release()){}



/*
Init()函数完成所有的连接操作，并将得到的所有连接元组存放在缓冲区buffer_中。
其通过子计划节点的Next()方法得到子计划节点的元组，通过双层循环遍历每一对元组组合，当内层计划节点返回假时调用其Init()使其初始化。
在得到子计划节点元组后，如存在谓词，则调用谓词的EvaluateJoin验证其是否符合谓词。
如不存在谓词或符合谓词，则通过调用out_schema各Column的EvaluateJoin得到输出元组，并将其置入buffer_。
*/
void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    buffer_.clear();
    const Schema *left_schema = plan_->GetLeftPlan()->OutputSchema();   //拿到左边表的属性列
    const Schema *right_schema = plan_->GetRightPlan()->OutputSchema();     //拿到右边表的属性列
    const Schema *out_schema = this->GetOutputSchema();         

    Tuple left_tuple;
    Tuple right_tuple;
    RID rid;
    while (left_executor_->Next(&left_tuple, &rid))         //先遍历外表left
    {
        right_executor_->Init();
        while (right_executor_->Next(&right_tuple, &rid))       //再遍历内标right
        {
            auto *predicate = plan_->Predicate();
            if (predicate == nullptr || predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) 
            {
                std::vector<Value> values;
                for (const auto &col : out_schema->GetColumns()) 
                {
                    values.emplace_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema));
                }
                
            buffer_.emplace_back(values, out_schema);
            }
        }
    }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) { 
    if(!buffer_.empty())
    {
        *tuple = buffer_.back();
        buffer_.pop_back();
        return true;
    }
    return false;
}

}  // namespace bustub
