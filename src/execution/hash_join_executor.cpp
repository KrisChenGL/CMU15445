//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) 
{
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the join */
void HashJoinExecutor::Init() { 
  left_child_->Init();
  right_child_->Init();
  jht_.clear();
  Tuple right_tuple;
  RID right_rid;
  while (right_child_->Next(&right_tuple, &right_rid)) {
        Value key = plan_->RightJoinKeyExpressions()[0]->Evaluate(&right_tuple, right_child_->GetOutputSchema());
        jht_[key].push_back(right_tuple);
  }
  
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while(true){
    if (right_match_idx_ < right_matches_.size()) {
      const auto &right_tuple = right_matches_[right_match_idx_];
      *tuple = CombineTuples(&left_tuple_, &right_tuple);
      right_match_idx_++;
      left_join_has_matched_ = true;
      return true;

  }
  if (plan_->GetJoinType() == JoinType::LEFT && !left_join_has_matched_ && left_tuple_.GetRid().GetPageId() != INVALID_PAGE_ID) {
      *tuple = CombineTuples(&left_tuple_, nullptr);
    
      left_tuple_ = Tuple::Empty();
      return true;
    }
  if (!left_child_->Next(&left_tuple_, rid)) {
     
      return false;
  }
  right_matches_.clear();
  right_match_idx_ = 0;
  left_join_has_matched_ = false;
  Value key = plan_->LeftJoinKeyExpressions()[0]->Evaluate(&left_tuple_, left_child_->GetOutputSchema());
  auto it = jht_.find(key);
   if (it != jht_.end()) {
      right_matches_ = it->second;
    }
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}

}
auto HashJoinExecutor::CombineTuples(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple {
     std::vector<Value> values;
     values.reserve(GetOutputSchema().GetColumnCount());
     for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(left_tuple->GetValue(&left_child_->GetOutputSchema(), i));
     }
     if (right_tuple != nullptr){
        for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
            values.push_back(right_tuple->GetValue(&right_child_->GetOutputSchema(), i));
        }
     }
     else{
          for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); ++i) {
            // 用 NULL 填充右侧所有列
            values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
        }

     }
     return {values, &GetOutputSchema()};
     
}
} // namespace bustub
