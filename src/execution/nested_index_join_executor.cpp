//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor))
{
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

void NestedIndexJoinExecutor::Init() { 
   child_executor_->Init();
   auto index_oid = plan_->GetIndexOid();
   auto inner_table_oid  = plan_->GetInnerTableOid();
   index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_oid);
   auto inner_table_info = exec_ctx_->GetCatalog()->GetTable(inner_table_oid);
   inner_table_heap_ = inner_table_info->table_.get();
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}

auto NestedIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while(true){
       if (inner_rid_idx_ < inner_rids_.size()){
           RID inner_rid = inner_rids_[inner_rid_idx_];
           auto [inner_meta, inner_tuple] = inner_table_heap_->GetTuple(inner_rid);
           inner_rid_idx_++;
           if (inner_meta.is_deleted_) {
                continue;
           }
           join_has_matched_ = true;
           CombineTuples(&outer_tuple_, &inner_tuple, tuple);
           return true;
       }
       RID outer_rid;
       if (!child_executor_->Next(&outer_tuple_,&outer_rid)){
            return false;
       }
      Value key_to_lookup = plan_->KeyPredicate()->Evaluate(&outer_tuple_, child_executor_->GetOutputSchema());
      std::vector<Value> key_values{key_to_lookup};
      Tuple key_tuple(key_values, &index_info_->key_schema_);
      inner_rids_.clear(); 
      index_info_->index_->ScanKey(key_tuple,&inner_rids_,exec_ctx_->GetTransaction());
      inner_rid_idx_ = 0;
      join_has_matched_ = false;
      if (inner_rids_.empty() && plan_->GetJoinType() == JoinType::LEFT) {
          CombineTuples(&outer_tuple_, nullptr, tuple);
          return true;
      }
      
  }
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}
void NestedIndexJoinExecutor::CombineTuples(const Tuple *left_tuple, const Tuple *right_tuple, Tuple *output_tuple) {
    std::vector<Value> values;
    const auto &output_schema = GetOutputSchema();
    values.reserve(output_schema.GetColumnCount());
    const auto &left_schema = child_executor_->GetOutputSchema();
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(left_tuple->GetValue(&left_schema, i));
    }
    const auto &right_schema = plan_->InnerTableSchema();
    if (right_tuple != nullptr) {
       
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
            values.push_back(right_tuple->GetValue(&right_schema, i));
        }
    } else {
        
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
            const auto &col = right_schema.GetColumn(i);
            values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
    }
    *output_tuple = Tuple(values, &output_schema);
}

}  // namespace bustub
