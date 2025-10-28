//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)),
    aht_(plan->GetAggregates(),plan->GetAggregateTypes()),
    aht_iterator_(aht_.Begin())
    {
    
    // UNIMPLEMENTED("TODO(P3): Add implementation.");

}

/** Initialize the aggregation */
void AggregationExecutor::Init() { 
  child_executor_->Init();
  aht_.Clear();
  Tuple tuple;
  RID rid;
  if (plan_->GetGroupBys().empty()){
     AggregateKey agg_key{{}};
     aht_.InsertCombine(agg_key, aht_.GenerateInitialAggregateValue());
     while(child_executor_->Next(&tuple,&rid)){
         auto agg_val = MakeAggregateValue(&tuple);
         aht_.InsertCombine(agg_key, agg_val); 
     }
  }
  else{
      while(child_executor_->Next(&tuple,&rid)){
         auto agg_key = MakeAggregateKey(&tuple);
         auto agg_val = MakeAggregateValue(&tuple);
         aht_.InsertCombine(agg_key,agg_val);

      }

  }
  aht_iterator_ = aht_.Begin();
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 

}

/**
 * Yield the next tuple from the insert.
 * @param[out] tuple The next tuple produced by the aggregation
 * @param[out] rid The next tuple RID produced by the aggregation
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (aht_iterator_!=aht_.End()){
        const auto &agg_key = aht_iterator_.Key();
        const auto &agg_val = aht_iterator_.Val();
         std::vector<Value> values;
        for (const auto &key : agg_key.group_bys_) {
            values.push_back(key);
        }
        for (const auto &val : agg_val.aggregates_) {
            values.push_back(val);
        }
        *tuple = Tuple(values, &GetOutputSchema());
        *rid = RID();
        ++aht_iterator_;
        return true;
  }
  // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  return false;
}

/** Do not use or remove this function, otherwise you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
