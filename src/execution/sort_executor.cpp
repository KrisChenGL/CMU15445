//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.cpp
//
// Identification: src/execution/sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/sort_executor.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new SortExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sort plan to be executed
 */
SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor))
{}

/** Initialize the sort */
void SortExecutor::Init() { 
    child_executor_->Init();
    sorted_entries_.clear();
    Tuple old_tuple;
    RID old_rid;

    while(child_executor_->Next(&old_tuple,&old_rid)){
          SortKey sort_key = GenerateSortKey(old_tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
          sorted_entries_.emplace_back(sort_key, old_tuple);
    }
    std::sort(sorted_entries_.begin(),sorted_entries_.end(),TupleComparator(plan_->GetOrderBy()));
    current_iter_ = sorted_entries_.begin();
    // throw NotImplementedException("SortExecutor is not implemented"); 
}

/**
 * Yield the next tuple from the sort.
 * @param[out] tuple The next tuple produced by the sort
 * @param[out] rid The next tuple RID produced by the sort
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (current_iter_ == sorted_entries_.end()){
         return false; 
    }
    *tuple = current_iter_->second;
    *rid = tuple->GetRid();
     ++current_iter_;
     return true;
}

}  // namespace bustub
