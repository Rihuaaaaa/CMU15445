//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {
    
/*
HashJoinExecutor使用基础哈希连接算法进行连接操作，其原理为将元组的连接键（即某些属性列的组合）作为哈希表的键，并使用其中一个子计划节点的元组构造哈希表。
由于具有相同连接键的元组一定具有相同的哈希键值，因此另一个子计划节点中的元组仅需在该元组映射的桶中寻找可与其连接的元组



*/
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) , plan_(plan), left_child_(left_child.release()), right_child_(right_child.release()){}


//init函数 遍历左子计划节点的元组，以完成哈希表的构建操作。
void HashJoinExecutor::Init() {
    left_child_->Init();
    right_child_->Init();
    hash_map_.clear();
    output_buffer_.clear();

    Tuple left_tuple;
    const Schema *left_schema = left_child_->GetOutputSchema();
    RID rid;

    while(left_child_->Next(&left_tuple, &rid))   //一遍一遍的调用左孩子的next方法,拿到所有的左孩子的每一行tuple，然后进行hashjoin存储(构建外表的哈希表)
    {
        HashJoinKey left_key;
        left_key.value_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_schema);
        hash_map_.emplace(left_key, left_tuple);
    }
}

//调用完左孩子，构建好hash表后，就开始调用右孩子的next()方法，拿到右孩子的tuple后就进行匹配，如果匹配成功(join成功)，则向上吐一个tuple

/*
使用右子计划节点的元组作为"探针"，以寻找与其连接键相同的左子计划节点的元组，一个右节点的元组可能有多个左节点的元组与其对应，但一次Next()操作仅返回一个结果连接元组。
因此在一次Next()中应当将连接得到的所有结果元组存放在output_buffer_缓冲区中，使得在下一次Next()中仅需从缓冲区中提取结果元组，而不调用子节点的Next()方法。
*/
bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
    if (!output_buffer_.empty()) 
    {
        *tuple = output_buffer_.back();
        output_buffer_.pop_back();
        return true;
    }
    Tuple right_tuple;
    const Schema *left_schema = left_child_->GetOutputSchema();
    const Schema *right_schema = right_child_->GetOutputSchema();
    const Schema *out_schema = GetOutputSchema();
    while (right_child_->Next(&right_tuple, rid)) 
    {
        HashJoinKey right_key;
        //使用右子计划节点的元组作为"探针"，以寻找与其连接键相同的左子计划节点的元组。
        right_key.value_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_schema);
        auto iter = hash_map_.find(right_key);
        uint32_t num = hash_map_.count(right_key);
        for (uint32_t i = 0; i < num; ++i, ++iter) 
        {
            std::vector<Value> values;
            for (const auto &col : out_schema->GetColumns()) 
            {
                values.emplace_back(col.GetExpr()->EvaluateJoin(&iter->second, left_schema, &right_tuple, right_schema));
            }
            output_buffer_.emplace_back(values, out_schema);
        }
        if (!output_buffer_.empty()) 
        {
            *tuple = output_buffer_.back();
            output_buffer_.pop_back();
            return true;
        }
    }
    return false;
}

}  // namespace bustub
