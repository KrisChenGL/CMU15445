//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/macros.h"
#include "execution/executors/filter_executor.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
AbstractExecutor(exec_ctx),
plan_(plan)
{
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() { 
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  auto table_oid = plan_->GetTableOid();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(table_oid);
  table_heap_ = table_info->table_.get();
  auto txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::SHARED, table_oid);
  }

  table_iterator_.emplace(table_heap_->MakeIterator());
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 

  while (!table_iterator_->IsEnd()){
         auto cur_rid = table_iterator_->GetRID();
         auto [meta, tuple_data] = table_heap_->GetTuple(cur_rid);
         if (!meta.is_deleted_){
              const auto *predicate = plan_->filter_predicate_.get();
              bool pass = true;
              if (predicate != nullptr) {
                pass = predicate->Evaluate(&tuple_data, GetOutputSchema()).GetAs<bool>();
              }
              if (pass) {
                *tuple = tuple_data;
                *rid = cur_rid;
                ++(*table_iterator_);
                return true;
              }
          }
           ++(*table_iterator_);
        }
        //  const Tuple current_tuple = *table_iterator_;
        //  const auto *predicate = plan_->GetPredicate();
        //  if (predicate != nullptr){
        //     Value result = predicate->Evaluate(&current_tuple, GetOutputSchema());
        //     if (!result.GetAs<bool>()) {
        //            ++table_iterator_; 
        //       continue;
        //    }
        //  }
        //  *tuple = current_tuple;
        //  *rid = tuple->GetRid();
        //   ++table_iterator_;
        //   return true;
   
   return false;
}

}  // namespace bustub
