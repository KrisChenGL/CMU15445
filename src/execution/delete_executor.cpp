//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "type/value_factory.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : 
    AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor))
{
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the delete */
void DeleteExecutor::Init() { 

  child_executor_->Init();
  Transaction* txn = exec_ctx_->GetTransaction(); 
  table_oid_t oid  = plan_->GetTableOid();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE){
      exec_ctx_->GetLockManager()->LockTable(txn,LockManager::LockMode::INTENTION_EXCLUSIVE,oid);   
  }
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  if (child_executor_==nullptr){
    return false;
  }
  Transaction* txn = exec_ctx_->GetTransaction();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  auto table_heap = table_info->table_.get();
  
  int delete_count = 0;
  Tuple delete_tuple;
  RID delete_rid;
  while(child_executor_->Next(&delete_tuple,&delete_rid)){
        exec_ctx_ -> GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE,plan_->GetTableOid(), delete_rid);
        
        // Get current tuple meta and mark it as deleted
        auto [meta, tuple] = table_heap->GetTuple(delete_rid);
        meta.is_deleted_ = true;
        table_heap->UpdateTupleMeta(meta, delete_rid);
        
        delete_count++;
        for(auto & index_info : catalog->GetTableIndexes(table_info->name_)){
            auto key = delete_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,index_info->index_->GetKeyAttrs());
            index_info->index_->DeleteEntry(key, delete_rid, txn);
        }
  }
        
        std::vector<Value> values;
        values.push_back(ValueFactory::GetIntegerValue(delete_count));
        *tuple = Tuple(values, &GetOutputSchema());
        *rid = RID();

        child_executor_.reset();
        return true;
  }

}  // namespace bustub
