//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  
  if(plan->GetType() != PlanType::SeqScan){
    std::vector<AbstractPlanNodeRef> children;
    for(const auto& child : plan->GetChildren()){
         children.emplace_back(OptimizeSeqScanAsIndexScan(child));
    }
    return plan->CloneWithChildren(std::move(children));
  }
  
  // For SeqScan nodes, we could potentially optimize to IndexScan
  // but for now, just recursively optimize children and return the plan
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  return optimized_plan;
}

}  // namespace bustub
