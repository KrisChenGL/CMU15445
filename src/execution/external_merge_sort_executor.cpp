//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <vector>
#include "common/macros.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      order_bys_(plan->GetOrderBy()),
      bpm_(exec_ctx->GetBufferPoolManager()),
      schema_(child_executor_->GetOutputSchema()),
      tuple_size_(schema_.GetInlinedStorageSize()) 
{
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}


/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  child_executor_->Init();
  std::vector<Tuple> buffer;
  size_t max_tuples_in_buffer = (K > 1 ? K - 1 : 1) * (BUSTUB_PAGE_SIZE / tuple_size_);
  if (max_tuples_in_buffer == 0) { max_tuples_in_buffer = 1; }

  // Create a lambda function for tuple comparison
  auto tuple_cmp = [this](const Tuple &a, const Tuple &b) -> bool {
    for (const auto &order : order_bys_) {
      Value val_a = order.second->Evaluate(&a, schema_);
      Value val_b = order.second->Evaluate(&b, schema_);
      
      CmpBool res = val_a.CompareLessThan(val_b);
      if (res == CmpBool::CmpTrue) {
        return order.first == OrderByType::ASC;
      }
      if (res == CmpBool::CmpFalse) {
        return order.first == OrderByType::DESC;
      }
    }
    return false;
  };

  Tuple tuple;
  RID rid;
  // 生成初始有序顺串
  while (child_executor_->Next(&tuple, &rid)) {
    buffer.push_back(tuple);
    if (buffer.size() == max_tuples_in_buffer) {
      std::sort(buffer.begin(), buffer.end(), tuple_cmp);
      
      std::vector<page_id_t> run_pages;
      page_id_t new_page_id = bpm_->NewPage();
      auto new_page_guard = bpm_->WritePage(new_page_id);
      auto sort_page = reinterpret_cast<SortPage *>(new_page_guard.GetDataMut());
      sort_page->Init();
      run_pages.push_back(new_page_id);

      for (const auto &t : buffer) {
        if (!sort_page->writetuple(t, schema_)) {
          new_page_guard.Drop();
          new_page_id = bpm_->NewPage();
          new_page_guard = bpm_->WritePage(new_page_id);
          sort_page = reinterpret_cast<SortPage *>(new_page_guard.GetDataMut());
          sort_page->Init();
          run_pages.push_back(new_page_id);
          sort_page->writetuple(t, schema_);
        }
      }
      new_page_guard.Drop();
      runs_.push({run_pages, bpm_, &schema_, tuple_size_});
      buffer.clear();
    }
  }
  if (!buffer.empty()) {
    std::sort(buffer.begin(), buffer.end(), tuple_cmp);
    std::vector<page_id_t> run_pages;
    page_id_t new_page_id = bpm_->NewPage();
    auto new_page_guard = bpm_->WritePage(new_page_id);
    auto sort_page = reinterpret_cast<SortPage *>(new_page_guard.GetDataMut());
    sort_page->Init();
    run_pages.push_back(new_page_id);
    for (const auto &t : buffer) {
        if (!sort_page->writetuple(t, schema_)) {
            new_page_guard.Drop();
            new_page_id = bpm_->NewPage();
            new_page_guard = bpm_->WritePage(new_page_id);
            sort_page = reinterpret_cast<SortPage *>(new_page_guard.GetDataMut());
            sort_page->Init();
            run_pages.push_back(new_page_id);
            sort_page->writetuple(t, schema_);
        }
    }
    new_page_guard.Drop();
    runs_.push({run_pages, bpm_, &schema_, tuple_size_});
  }
  // 2路归并所有顺串
  while (runs_.size() > 1) {
    MergeSortRun run1 = runs_.front();
    runs_.pop();
    MergeSortRun run2 = runs_.front();
    runs_.pop();
    runs_.push(MergeTwoRuns(run1, run2));
  }

  // 准备最终的迭代器
  if (!runs_.empty()) {
    final_iter_ = std::make_unique<MergeSortRun::Iterator>(runs_.front().Begin());
  }
}

/**
 * Yield the next tuple from the external merge sort.
 * @param[out] tuple The next tuple produced by the external merge sort.
 * @param[out] rid The next tuple RID produced by the external merge sort.
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (final_iter_ == nullptr || *final_iter_ == runs_.front().End()) {
    return false;
  }
  *tuple = **final_iter_;
  *rid = tuple->GetRid();
  ++(*final_iter_);
  return true;
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

template<size_t K>
auto ExternalMergeSortExecutor<K>::MergeTwoRuns(const MergeSortRun &run1, const MergeSortRun &run2) -> MergeSortRun {
  std::vector<page_id_t> new_run_pages;
  auto iter1 = run1.Begin();
  auto end1 = run1.End();
  auto iter2 = run2.Begin();
  auto end2 = run2.End();

  // Create a lambda function for tuple comparison
  auto tuple_cmp = [this](const Tuple &a, const Tuple &b) -> int {
    for (const auto &order : order_bys_) {
      Value val_a = order.second->Evaluate(&a, schema_);
      Value val_b = order.second->Evaluate(&b, schema_);
      
      CmpBool res = val_a.CompareLessThan(val_b);
      if (res == CmpBool::CmpTrue) {
        return order.first == OrderByType::ASC ? -1 : 1;
      }
      if (res == CmpBool::CmpFalse) {
        return order.first == OrderByType::ASC ? 1 : -1;
      }
    }
    return 0;
  };

  page_id_t current_page_id = bpm_->NewPage();
  auto current_page_guard = bpm_->WritePage(current_page_id);
  new_run_pages.push_back(current_page_id);
  auto sort_page = reinterpret_cast<SortPage *>(current_page_guard.GetDataMut());
  sort_page->Init();

  auto write_to_page = [&](const Tuple &t) {
    if (!sort_page->writetuple(t, schema_)) {
      current_page_guard.Drop();
      current_page_id = bpm_->NewPage();
      current_page_guard = bpm_->WritePage(current_page_id);
      new_run_pages.push_back(current_page_id);
      sort_page = reinterpret_cast<SortPage *>(current_page_guard.GetDataMut());
      sort_page->Init();
      BUSTUB_ENSURE(sort_page->writetuple(t, schema_), "Tuple is too large for a page.");
    }
  };
  while (iter1 != end1 && iter2 != end2) {
    if (tuple_cmp(*iter1, *iter2) <= 0) {
      write_to_page(*iter1);
      ++iter1;
    } else {
      write_to_page(*iter2);
      ++iter2;
    }
  }
  while (iter1 != end1) {
    write_to_page(*iter1);
    ++iter1;
  }
  while (iter2 != end2) {
    write_to_page(*iter2);
    ++iter2;
  }
  current_page_guard.Drop();
  for (auto page_id : run1.Getpages()) { bpm_->DeletePage(page_id); }
  for (auto page_id : run2.Getpages()) { bpm_->DeletePage(page_id); }

  // return {new_run_pages, bpm_, &schema_, tuple_size_}; //same 
  return MergeSortRun(new_run_pages, bpm_, &schema_, tuple_size_);
  
}


// template class ExternalMergeSortExecutor<2>;
template class ExternalMergeSortExecutor<2>;
template class ExternalMergeSortExecutor<64>;

}  // namespace bustub
