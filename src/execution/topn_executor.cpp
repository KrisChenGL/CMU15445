//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.cpp
//
// Identification: src/execution/topn_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_executor.h"

namespace bustub {

/**
 * Construct a new TopNExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The TopN plan to be executed
 */
TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the TopN */
void TopNExecutor::Init() { 
     child_executor_->Init();
     std::vector<Tuple> all_tuples;
     Tuple tuple;
     RID rid;
     while(child_executor_->Next(&tuple, &rid)){
        all_tuples.push_back(tuple);
     }
     
     auto cmp = [&](const Tuple& tuple_a, const Tuple& tuple_b){
        for (const auto &order : plan_->GetOrderBy()) {
            Value va = order.second->Evaluate(&tuple_a, child_executor_->GetOutputSchema());
            Value vb = order.second->Evaluate(&tuple_b, child_executor_->GetOutputSchema());
            CmpBool res = va.CompareEquals(vb); 
            
            if (res == CmpBool::CmpTrue) {
                continue;
            }
            else {
                if (order.first == OrderByType::ASC) {
                    return va.CompareLessThan(vb) == CmpBool::CmpTrue;
                } else { 
                    return va.CompareGreaterThan(vb) == CmpBool::CmpTrue;
                }
            }
        }
         return false;
     };
      
     std::sort(all_tuples.begin(), all_tuples.end(), cmp);
     size_t n = plan_->GetN();
     if (n < all_tuples.size()) {
        top_tuples_ = std::vector<Tuple>(all_tuples.begin(), all_tuples.begin() + n);
     } else {
        top_tuples_ =  std::move(all_tuples);
     }
    current_iter_ = top_tuples_.begin();
    

    // throw NotImplementedException("TopNExecutor is not implemented"); 
}

/**
 * Yield the next tuple from the TopN.
 * @param[out] tuple The next tuple produced by the TopN
 * @param[out] rid The next tuple RID produced by the TopN
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (current_iter_ == top_tuples_.end()) {
        return false;
    }
    *tuple = *current_iter_;
    *rid = tuple->GetRid();
    ++current_iter_;
    return true;
    // return false; 
}

auto TopNExecutor::GetNumInHeap() -> size_t { 
    // throw NotImplementedException("TopNExecutor is not implemented"); 
    return top_tuples_.size();
}

    
}  // namespace bustub
