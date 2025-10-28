//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_per_group_executor.cpp
//
// Identification: src/execution/topn_per_group_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_per_group_executor.h"

namespace bustub {

/**
 * Construct a new TopNPerGroupExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The TopNPerGroup plan to be executed
 */
TopNPerGroupExecutor::TopNPerGroupExecutor(ExecutorContext *exec_ctx, const TopNPerGroupPlanNode *plan,
                                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the TopNPerGroup */
void TopNPerGroupExecutor::Init() { 
    // throw NotImplementedException("TopNPerGroupExecutor is not implemented"); 
    child_executor_->Init();
    using GroupKey = std::vector<Value>;
    struct GroupKeyHash{
         size_t operator()(const GroupKey& key) const{
             size_t h = 0;
             for(const auto& v :key){
                h ^= std::hash<std::string>{}(v.ToString()) + 0x9e3779b9 + (h << 6) + (h >> 2);
             }
             return h;
         }   
    };
    struct GroupKeyEqual{
        bool operator()(const GroupKey& a, const GroupKey& b) const{
           if (a.size() != b.size()) return false;
           for (size_t i = 0; i < a.size(); i++){
               if (a[i].CompareNotEquals(b[i]) == CmpBool::CmpTrue) return false;
           }
           return true;
        }
    };
    std::unordered_map<GroupKey, std::vector<Tuple>, GroupKeyHash, GroupKeyEqual> group_tuples;
    Tuple tuple;
    RID rid;
    auto &schema = child_executor_->GetOutputSchema();
    while(child_executor_->Next(&tuple, &rid)){
        // TopNPerGroup executor doesn't need transaction isolation handling
        // as it operates on tuples already retrieved by child executor
        
        GroupKey key;
        key.reserve(plan_->GetGroupBy().size());
        for(auto &expr : plan_->GetGroupBy()){
            key.push_back(expr->Evaluate(&tuple, schema));
        }
        group_tuples[key].push_back(tuple);
    }
 
    auto cmp = [&](const Tuple &a, const Tuple &b) {
        for (const auto &order : plan_->GetOrderBy()) {
            Value va = order.second->Evaluate(&a, schema);
            Value vb = order.second->Evaluate(&b, schema);

            if (va.CompareEquals(vb) == CmpBool::CmpTrue) {
                continue;
            }

            if (order.first == OrderByType::ASC) {
                return va.CompareLessThan(vb) == CmpBool::CmpTrue;
            } else {
                return va.CompareGreaterThan(vb) == CmpBool::CmpTrue;
            }
        }
        return false;
    };
    n_ = plan_->GetN();
    for (auto &kv : group_tuples) {
        auto &tuples = kv.second;
        std::sort(tuples.begin(), tuples.end(), cmp);
        if (tuples.size() > n_) {
            tuples.resize(n_);
        }
        
        for (auto &t : tuples) {
            result_tuples_.push_back(t);
        }
    }
    current_iter_ = result_tuples_.begin();
}

/**
 * Yield the next tuple from the TopNPerGroup.
 * @param[out] tuple The next tuple produced by the TopNPerGroup
 * @param[out] rid The next tuple RID produced by the TopNPerGroup
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto TopNPerGroupExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    // return false;
    if (current_iter_ == result_tuples_.end()) { return false; }
    *tuple = *current_iter_;
    *rid = tuple->GetRid();
    ++current_iter_;
    return true; 
}
    
}  // namespace bustub
