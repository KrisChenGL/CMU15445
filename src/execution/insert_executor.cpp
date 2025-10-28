//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "type/value_factory.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), insert_done_(false) {}
  // UNIMPLEMENTED("TODO(P3): Add implementation.");

/** Initialize the insert */
void InsertExecutor::Init() { 

  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  insert_done_ = false;
  Transaction* txn = exec_ctx_->GetTransaction();
  table_oid_t table_oid = plan_->GetTableOid();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::SNAPSHOT_ISOLATION) {
    exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid);
  }
  
  else if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
        exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid);
  }
  // child_executor_ is already initialized in constructor
  if (child_executor_ != nullptr){
       child_executor_->Init();
  }  
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  if(insert_done_){
      return false;
  }
  Transaction* txn = exec_ctx_->GetTransaction();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog-> GetTable(plan_->GetTableOid());
  auto table_heap = table_info->table_.get();
  int insert_count = 0;
 
  
  if (child_executor_ != nullptr){
      RID temp_rid;
      Tuple to_insert;
      while (child_executor_->Next(&to_insert, &temp_rid)){
           TupleMeta meta;
           meta.ts_ = txn->GetTransactionId(); 
           meta.is_deleted_ = false;
           auto opt_rid = table_heap->InsertTuple(meta, to_insert,exec_ctx_->GetLockManager(), txn ,plan_->GetTableOid());
           if(opt_rid.has_value()){
               RID new_rid = *opt_rid;
               exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE,plan_->GetTableOid(), new_rid);
               
               for(auto & index_info: catalog->GetTableIndexes(table_info->name_)){
                    auto key_tuple = to_insert.KeyFromTuple(table_info->schema_, index_info-> key_schema_, index_info->index_->GetKeyAttrs());
                    index_info->index_->InsertEntry(key_tuple,new_rid,txn);

               }
               insert_count++;
           }          
      }
  }
  else{
       // 如果没有子执行器，说明是直接插入值的情况，但当前的 InsertPlanNode 没有 Tuples() 方法
       // 这种情况下应该从子执行器获取数据，所以这个分支可能不需要
       // 暂时注释掉这个分支
       /*
       for(const auto& tuple: plan_->Tuples()){
           RID opt_rid;
           TupleMeta meta;  
           Tuple to_insert(tuple, &table_info->schema_);
          
           meta.ts_ = txn->GetTransactionId(); 
           opt_rid = table_heap->InsertTuple( meta, to_insert, exec_ctx_->GetLockManager(), txn, plan_->TableOid());
           
           if(opt_rid.has_value()){
               RID new_rid = opt_rid.value();
               exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE,plan_->TableOid(), new_rid);
               for(auto &  index_info: catalog->GetTableIndexes(table_info->name_)){
                   auto  key_tuple = to_insert.KeyFromTuple(table_info->schema_, index_info-> key_schema_, index_info->index_->GetKeyAttrs());
                   index_info->index_->InsertEntry(key_tuple,new_rid,txn);
               }
               insert_count++;
           }
           
       }    
       */
  }
  
  // table_oid_t table_oid = plan_->GetTableOid();
  std::vector<Value>values;
  values.push_back(ValueFactory::GetIntegerValue(insert_count));


  *tuple = Tuple(values, &GetOutputSchema());
  *rid = RID();
  insert_done_ = true;
  return true;
}

}  // namespace bustub
