//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_limit_as_topn.cpp
//
// Identification: src/optimizer/sort_limit_as_topn.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

namespace bustub {

/**
 * @brief optimize sort + limit as top N
 */
auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
     std::vector<AbstractPlanNodeRef>child_node;
     for(const auto& child : plan->GetChildren()){
         child_node.push_back(OptimizeSortLimitAsTopN(child));
     }
     auto optimized_plan = plan->CloneWithChildren(std::move(child_node));
     if (optimized_plan->GetType()  == PlanType::Limit){
         const auto & limit_plan = static_cast<const LimitPlanNode&>(*optimized_plan);
         const auto child_plan = limit_plan.GetChildAt(0);

         if (child_plan->GetType() == PlanType::Sort){
              const auto &sort_plan = static_cast<const SortPlanNode &>(*child_plan);
              const auto &order_child = sort_plan.GetOrderBy();
            //   const auto& sort_child = sort_plan.GetChildAt(0);
              return std::make_shared<TopNPlanNode>(std::make_shared<Schema>(limit_plan.OutputSchema()), child_plan, order_child, limit_plan.GetLimit()); 
              
         }
     }
      
     return optimized_plan;
}

}  // namespace bustub
