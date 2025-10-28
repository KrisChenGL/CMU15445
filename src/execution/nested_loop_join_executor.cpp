//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_executor_(std::move(left_executor)),
    right_executor_(std::move(right_executor))   
{
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  right_executor_->Init();
  RID left_rid{};
  left_tuple_in_use_ = left_executor_->Next(&left_tuple_, &left_rid);
  left_join_has_matched_ = false;
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join
 * @param[out] rid The next tuple RID produced, not used by nested loop join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  Tuple right_tuple;
  RID right_rid;
  while(left_tuple_in_use_){
        if (right_executor_->Next(&right_tuple, &right_rid)){
             Value val = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
             if (!val.IsNull()&& val.GetAs<bool>()) {
                 CombineTuples(&left_tuple_, &right_tuple, tuple);
                 left_join_has_matched_ = true;
                 return true;
             }
             continue;
        }
        if (plan_->GetJoinType() == JoinType::LEFT && !left_join_has_matched_){
             CombineTuples(&left_tuple_, nullptr, tuple);
             left_join_has_matched_ = true;
             return true;
        }
        RID left_rid;
        left_tuple_in_use_ = left_executor_->Next(&left_tuple_, &left_rid);
        left_join_has_matched_ = false;
        right_executor_->Init();
        
  }
  return false;
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}
void NestedLoopJoinExecutor::CombineTuples(const Tuple *left_tuple, const Tuple *right_tuple, Tuple *output_tuple){
    std::vector<Value> values;
    const auto &output_schema = GetOutputSchema();
    values.reserve(output_schema.GetColumnCount());
    for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
    }
    if (right_tuple != nullptr) {
 
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            values.push_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(), i));
        }
    } else {
        // LEFT JOIN 未匹配的情况，用 NULL 填充右侧
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            const auto &col = right_executor_->GetOutputSchema().GetColumn(i);
            values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
    }
     *output_tuple = Tuple(values, &output_schema);
}

}  // namespace bustub
