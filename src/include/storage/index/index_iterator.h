//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_internal_page.h" 
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index);

  ~IndexIterator();  // NOLINT

  // Delete copy constructor and copy assignment operator
  IndexIterator(const IndexIterator &) = delete;
  auto operator=(const IndexIterator &) -> IndexIterator & = delete;

  // Enable move constructor and move assignment operator
  IndexIterator(IndexIterator &&) = default;
  auto operator=(IndexIterator &&) -> IndexIterator & = default;

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { 
    // UNIMPLEMENTED("TODO(P2): Add implementation.");
     return index_ == itr.index_;
   }

  auto operator!=(const IndexIterator &itr) const -> bool { 
    // UNIMPLEMENTED("TODO(P2): Add implementation."); 
    return !(*this == itr);
  }

 private:
  // add your own private member variables here
   BufferPoolManager *bmp_{nullptr};
   page_id_t page_id_{INVALID_PAGE_ID};
   int index_{-1};
};

}  // namespace bustub
