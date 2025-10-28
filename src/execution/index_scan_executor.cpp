//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/table_iterator.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan)
{
  // UNIMPLEMENTED("TODO(P3): Add implementation.");

}

void IndexScanExecutor::Init() { 
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid()).get();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_).get();
  tree_ =static_cast<BPlusTreeIndexForTwoIntegerColumn*>(index_info_->index_.get());
  iter_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
while(iter_!=tree_->GetEndIterator()){
     const RID found_rid = (*iter_).second;
     auto [tuple_meta, found_tuple] = table_info_->table_->GetTuple(found_rid);
     if (tuple_meta.is_deleted_){
       ++iter_;
      continue;
     }
     *tuple = found_tuple;
      *rid = found_rid;
      ++iter_;
     return true;
}
return false;

}

}  // namespace bustub
