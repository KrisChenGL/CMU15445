//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) { 
  // UNIMPLEMENTED("TODO(P2): Add implementation."); 
  // 设置页面类型为叶子节点
  SetPageType(IndexPageType::LEAF_PAGE);
  // 初始时页面为空，当前大小设为 0
  SetSize(0);
  // 设置最大大小为传入参数
  SetMaxSize(max_size);
  // 初始时没有下一个叶子节点，设为无效页面 ID
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { 
  // UNIMPLEMENTED("TODO(P2): Add implementation."); 
   return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { 
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  if (index < 0 || index >= GetSize()){
     throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of bounds in LeafPage KeyAt()");
  }
  return key_array_[index];
}

/*
 * Helper method to find and return the value associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  if (index < 0 || index >= GetSize()){
     throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of bounds in LeafPage ValueAt()");
  }
  return rid_array_[index];
}

/*
 * Insert a key-value pair into the leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool {
  // Check if page is full
  if (GetSize() >= GetMaxSize()) {
    return false;
  }

  // Find the insertion position using binary search
  int left = 0, right = GetSize() - 1;
  int insert_pos = GetSize(); // Default to end if not found

  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp_result = comparator(key_array_[mid], key);
    
    if (cmp_result == 0) {
      // Key already exists
      return false;
    } else if (cmp_result < 0) {
      left = mid + 1;
    } else {
      insert_pos = mid;
      right = mid - 1;
    }
  }

  // Shift elements to make room for new key-value pair
  for (int i = GetSize(); i > insert_pos; i--) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }

  // Insert the new key-value pair
  key_array_[insert_pos] = key;
  rid_array_[insert_pos] = value;
  
  // Increment size
  ChangeSizeBy(1);
  
  return true;
}

/*
 * Look up a key in the leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const -> bool {
  // Binary search for the key
  int left = 0, right = GetSize() - 1;
  
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp_result = comparator(key_array_[mid], key);
    
    if (cmp_result == 0) {
      // Key found
      *value = rid_array_[mid];
      return true;
    } else if (cmp_result < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  
  // Key not found
  return false;
}

/*
 * Move half of the key-value pairs to the new leaf page (used during split)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int total_size = GetSize();
  int move_size = total_size / 2;
  int start_index = total_size - move_size;
  
  // Copy the second half to recipient
  for (int i = 0; i < move_size; i++) {
    recipient->key_array_[i] = key_array_[start_index + i];
    recipient->rid_array_[i] = rid_array_[start_index + i];
  }
  
  // Update sizes
  recipient->SetSize(move_size);
  SetSize(start_index);
}

/*
 * Move all key-value pairs to another leaf page (used during merge)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  int recipient_size = recipient->GetSize();
  int my_size = GetSize();
  
  // Copy all my data to the end of recipient
  for (int i = 0; i < my_size; i++) {
    recipient->key_array_[recipient_size + i] = key_array_[i];
    recipient->rid_array_[recipient_size + i] = rid_array_[i];
  }
  
  // Update recipient size
  recipient->SetSize(recipient_size + my_size);
  // Clear my size
  SetSize(0);
}

/*
 * Move the first key-value pair to the end of another leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  int recipient_size = recipient->GetSize();
  
  // Move first element to recipient's end
  recipient->key_array_[recipient_size] = key_array_[0];
  recipient->rid_array_[recipient_size] = rid_array_[0];
  recipient->SetSize(recipient_size + 1);
  
  // Shift remaining elements left
  for (int i = 0; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  
  // Update my size
  ChangeSizeBy(-1);
}

/*
 * Move the last key-value pair to the front of another leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  int recipient_size = recipient->GetSize();
  int last_index = GetSize() - 1;
  
  // Shift recipient's elements right to make room
  for (int i = recipient_size; i > 0; i--) {
    recipient->key_array_[i] = recipient->key_array_[i - 1];
    recipient->rid_array_[i] = recipient->rid_array_[i - 1];
  }
  
  // Move last element to recipient's front
  recipient->key_array_[0] = key_array_[last_index];
  recipient->rid_array_[0] = rid_array_[last_index];
  recipient->SetSize(recipient_size + 1);
  
  // Update my size
  ChangeSizeBy(-1);
}

/*
 * Remove and delete a key-value pair from the leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int {
  // Find the key using binary search
  int left = 0, right = GetSize() - 1;
  int found_index = -1;
  
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp_result = comparator(key_array_[mid], key);
    
    if (cmp_result == 0) {
      found_index = mid;
      break;
    } else if (cmp_result < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  
  // If key not found, return current size
  if (found_index == -1) {
    return GetSize();
  }
  
  // Shift elements left to fill the gap
  for (int i = found_index; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  
  // Update size
  ChangeSizeBy(-1);
  return GetSize();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
