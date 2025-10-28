//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "concurrency/lock_manager.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor))
{
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the update */
void UpdateExecutor::Init() { 
  done_ = false;
  child_executor_->Init();
  auto *txn = exec_ctx_->GetTransaction();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  table_indexs_.clear();
  for (const auto& index : table_indexes) {
    table_indexs_.push_back(index.get());
  }
  if(txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE){
      exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid());
  }


  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
   if(done_){
     return false;
   }
  Tuple old_tuple;
  RID old_rid;
  int32_t updated_count = 0;
  auto *txn = exec_ctx_->GetTransaction();
  while(child_executor_->Next(&old_tuple,&old_rid)){
       std::vector<Value>new_values;  
       if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE){
          exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), old_rid);
       }
       else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED ||   txn->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION){
          exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), old_rid);
       }
       new_values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
       for(auto &expr : plan_->target_expressions_){
          new_values.push_back(expr->Evaluate(&old_tuple,child_executor_->GetOutputSchema()));
       }
       Tuple new_tuple(new_values,&child_executor_->GetOutputSchema());
       bool success = table_info_->table_->UpdateTupleInPlace(TupleMeta{0, false}, new_tuple, old_rid);
       if (success){
          for(auto *index_info:table_indexs_){
              auto index = index_info->index_.get();
              Tuple old_key = old_tuple.KeyFromTuple(table_info_->schema_,index_info->key_schema_,index->GetKeyAttrs());
              Tuple new_key = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
              index->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
              index->InsertEntry(new_key, old_rid, exec_ctx_->GetTransaction());              
          }
          updated_count++;
       }
  }
  std::vector<Value> result_values;
  result_values.emplace_back(TypeId::INTEGER, updated_count);
  *tuple = Tuple(result_values, &GetOutputSchema());
  done_ = true;
  return true;
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}
}  // namespace bustub
