//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
#include "common/config.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Spring 2025.
 */
class SortPage {
 public:
  /**
   * TODO(P3): Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
   void Init() {
     num_tuples_ = 0;
   }
   
   auto writetuple(const Tuple &tuple,const Schema &schema) -> bool {
        // Calculate serialized size
        uint32_t tuple_size = sizeof(uint32_t) + tuple.GetLength();
        uint32_t offset = METADATA_SIZE + tuple_size * num_tuples_;
        if (tuple_size + offset <= BUSTUB_PAGE_SIZE){
           tuple.SerializeTo(data_ + offset);
           num_tuples_++;
           return true;
        } 
        return false;
   }
  auto ReadTuple(size_t index, const Schema &schema) const -> Tuple {
    if (index >= num_tuples_) {
      return Tuple();
    }
    uint32_t tuple_size = sizeof(uint32_t) + Tuple().GetLength();
    uint32_t offset = METADATA_SIZE + index * tuple_size;
    Tuple tuple;
    tuple.DeserializeFrom(data_ + offset);
    return tuple;
  }
  auto GetTupleCount() -> size_t { return num_tuples_; }

 private:
      static const size_t METADATA_SIZE = sizeof(uint32_t);
      uint32_t num_tuples_{0};
      char data_[BUSTUB_PAGE_SIZE];
      
  /**
   * TODO(P3): Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */

};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
  // MergeSortRun 代表一个有序的元组序列（顺串）。
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm, const Schema *schema, size_t tuple_size) 
    : pages_(std::move(pages)), bmp_(bpm), schema_(schema), tuple_size_(tuple_size) {}

  auto GetPageCount() -> size_t { return pages_.size(); }
  auto Getpages() const -> const std::vector<page_id_t>& { return pages_; }
  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     */
    auto operator++() -> Iterator & { 
      if (current_page_ == nullptr) {
        return *this;
      }
      current_tuple_idx_++;
      if (current_tuple_idx_ >= current_page_->GetTupleCount()) {
        current_page_idx_++;
        if (current_page_idx_ < run_->pages_.size()) {
          bpm_->UnpinPage(run_->pages_[current_page_idx_ - 1], false);
          auto page_guard = bpm_->ReadPage(run_->pages_[current_page_idx_]);
          current_page_ = reinterpret_cast<SortPage *>(const_cast<char*>(page_guard.GetData()));
          current_tuple_idx_ = 0;
        } else {
          bpm_->UnpinPage(run_->pages_[current_page_idx_ - 1], false);
          current_page_ = nullptr;
        }
      }
      return *this;
      // UNIMPLEMENTED("TODO(P3): Add implementation."); 
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     */
    auto operator*() -> Tuple { 

       if (current_page_ == nullptr) {
        return Tuple();
      }
      return current_page_->ReadTuple(current_tuple_idx_, *run_->schema_);
      // UNIMPLEMENTED("TODO(P3): Add implementation."); 

    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     */
    auto operator==(const Iterator &other) const -> bool { 
      return run_ == other.run_ && current_page_idx_ == other.current_page_idx_ && current_tuple_idx_ == other.current_tuple_idx_;
      // UNIMPLEMENTED("TODO(P3): Add implementation."); 
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool { 
      return !(*this == other);
      // UNIMPLEMENTED("TODO(P3): Add implementation."); 
    }

   private:
    explicit Iterator(const MergeSortRun *run,size_t page_idx , size_t tuple_idx) : 
    run_(run),
    bpm_(run->bmp_),
    current_page_idx_(page_idx),
    current_tuple_idx_(tuple_idx)
    {
      if (current_page_idx_ < run_->pages_.size()) {
        current_page_id_ = run_->pages_[current_page_idx_];
        ReadCurrentTuple();
      } 
      else{
        current_page_id_ = INVALID_PAGE_ID;
        current_tuple_idx_ = 0;

      }
    }
    void ReadCurrentTuple() {
        auto page_guard = bpm_->ReadPage(current_page_id_);
        auto sort_page = reinterpret_cast<const SortPage *>(page_guard.GetData());
        current_tuple_ = sort_page->ReadTuple(current_tuple_idx_, *run_->schema_);
    }
  
    /** The sorted run that the iterator is iterating on. */
    [[maybe_unused]] const MergeSortRun *run_{nullptr};
    BufferPoolManager *bpm_{nullptr};
    const Schema *schema_{nullptr};
    page_id_t current_page_id_{INVALID_PAGE_ID};
    SortPage *current_page_{nullptr};
    size_t current_page_idx_{0};
    size_t current_tuple_idx_{0};
    Tuple current_tuple_{};
    /**
     * TODO(P3): Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   */
  auto Begin() const -> Iterator { 
    return Iterator(this,0,0);
    // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   */
  auto End()const-> Iterator { 
    return Iterator(this, pages_.size(),0);
    // UNIMPLEMENTED("TODO(P3): Add implementation."); 
  }

 private:
  
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  [[maybe_unused]] BufferPoolManager *bmp_;
  const Schema *schema_;
  size_t tuple_size_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
   auto MergeTwoRuns(const MergeSortRun &run1, const MergeSortRun &run2) -> MergeSortRun;
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** Compares tuples based on the order-bys */
  std::vector<OrderBy> order_bys_;
  BufferPoolManager *bpm_;
  const Schema &schema_;
  size_t tuple_size_;
  std::queue<MergeSortRun> runs_;
  std::unique_ptr<MergeSortRun::Iterator> final_iter_;
  /** TODO(P3): You will want to add your own private members here. */
};

}  // namespace bustub
