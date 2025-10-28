//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "common/util/hash_util.h"

namespace bustub {

struct ValueHash {
  std::size_t operator()(const Value &val) const {
    return HashUtil::HashValue(&val);
  }
};
struct ValueEqual {
  bool operator()(const Value &a, const Value &b) const {
    // 使用 Value 类提供的比较方法
    return a.CompareEquals(b) == CmpBool::CmpTrue;
  }
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };
  

 private:
  /** The HashJoin plan node to be executed. */
  auto CombineHashes(const Tuple*left_child_, const Tuple* right_child_)->Tuple;
  auto CombineTuples(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple;
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::unordered_map<Value, std::vector<Tuple>, ValueHash, ValueEqual> jht_{};
  Tuple left_tuple_{};
  std::vector<Tuple> right_matches_{};
  size_t right_match_idx_{0};
  bool left_join_has_matched_{false};
};

}  // namespace bustub
