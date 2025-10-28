//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/**
 * @brief Populate a new root page with the first key-value pair
 * @param old_value The page id of the left child
 * @param new_key The key to separate left and right children
 * @param new_value The page id of the right child
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
  // For a new root, we need to set up the first two children
  page_id_array_[0] = old_value;  // Left child
  key_array_[1] = new_key;        // Separator key
  page_id_array_[1] = new_value;  // Right child
  SetSize(2);  // Root has 2 children initially
}

/**
 * @brief Move half of the key-value pairs to the new internal page (used during split)
 * @param recipient The new internal page to move data to
 * @param buffer_pool_manager Buffer pool manager for updating child parent pointers
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int total_size = GetSize();
  int move_size = total_size / 2;
  int start_index = total_size - move_size;
  
  // Copy the second half to recipient
  for (int i = 0; i < move_size; i++) {
    recipient->key_array_[i] = key_array_[start_index + i];
    recipient->page_id_array_[i] = page_id_array_[start_index + i];
  }
  
  // Update parent pointers for moved children
  for (int i = 0; i < move_size; i++) {
    auto child_guard = buffer_pool_manager->WritePage(recipient->page_id_array_[i]);
    auto child_page = child_guard.template AsMut<BPlusTreePage>();
    child_page->SetParentPageId(recipient->GetPageId());
  }
  
  // Update sizes
  recipient->SetSize(move_size);
  SetSize(start_index);
}

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) { 
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetMaxSize(max_size);
    SetSize(1);
  // UNIMPLEMENTED("TODO(P2): Add implementation."); 
  
}

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
    if (index < 0 || index >= GetMaxSize()){
       throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
    }
    return key_array_[index]; 
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  if (index < 0 || index >= GetMaxSize()){
      throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
  }
  key_array_[index] = key;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  if (index < 0 || index >= GetMaxSize()){
    throw Exception(ExceptionType::OUT_OF_RANGE, "Index out of range");
  }
  return page_id_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const->int{
    for(int i = 0; i<GetSize();++i){
        if (page_id_array_[i] == value){
            return i;
        }
    }
    return -1;
}

/**
 * @brief Find the child page id for the given key
 * 
 * @param key The key to search for
 * @param comparator The key comparator
 * @return The page id of the child page that should contain the key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // Binary search to find the appropriate child page
  // The first key is invalid, so we start from index 1
  int left = 1;
  int right = GetSize() - 1;
  
  // If there's only one child (size == 1), return the first page
  if (GetSize() == 1) {
    return page_id_array_[0];
  }
  
  // Find the rightmost key that is <= the search key
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) <= 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  
  // Return the page_id at index right
  // If right < 0, return the first page
  return page_id_array_[std::max(0, right)];
}

/**
 * @brief Remove and return the only child page id when the internal page has only one child
 * This is used during root adjustment when the root has only one child
 * 
 * @return The page id of the only child
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  // This method should only be called when the internal page has exactly one child
  if (GetSize() != 1) {
    throw Exception(ExceptionType::INVALID, "RemoveAndReturnOnlyChild called on page with size != 1");
  }
  
  // Return the only child page id (at index 0)
  ValueType only_child_id = page_id_array_[0];
  
  // Clear the page
  SetSize(0);
  
  return only_child_id;
}

/**
 * @brief Move all key-value pairs to another internal page (used during merge)
 * @param recipient The internal page to move all data to
 * @param middle_key The key from parent to be inserted between the two pages
 * @param buffer_pool_manager Buffer pool manager for updating child parent pointers
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  int recipient_size = recipient->GetSize();
  int my_size = GetSize();
  
  // Set the middle key at the first position of the data to be moved
  key_array_[0] = middle_key;
  
  // Copy all my data to the end of recipient
  for (int i = 0; i < my_size; i++) {
    recipient->key_array_[recipient_size + i] = key_array_[i];
    recipient->page_id_array_[recipient_size + i] = page_id_array_[i];
  }
  
  // Update parent pointers for all moved children
  for (int i = 0; i < my_size; i++) {
    auto child_guard = buffer_pool_manager->WritePage(page_id_array_[i]);
    auto child_page = child_guard.template AsMut<BPlusTreePage>();
    child_page->SetParentPageId(recipient->GetPageId());
  }
  
  // Update recipient size
  recipient->SetSize(recipient_size + my_size);
  // Clear my size
  SetSize(0);
}

/**
 * @brief Remove a key-value pair at the specified index
 * @param index The index of the key-value pair to remove
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::INVALID, "Remove index out of bounds");
  }
  
  // Shift elements left to fill the gap
  for (int i = index; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    page_id_array_[i] = page_id_array_[i + 1];
  }
  
  // Decrease size
  ChangeSizeBy(-1);
}

/**
 * @brief Move the last key-value pair to the front of another internal page
 * @param recipient The internal page to move data to
 * @param middle_key The key from parent to be inserted
 * @param buffer_pool_manager Buffer pool manager for updating child parent pointers
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  int recipient_size = recipient->GetSize();
  int last_index = GetSize() - 1;
  
  // Shift recipient's elements right to make room
  for (int i = recipient_size; i > 0; i--) {
    recipient->key_array_[i] = recipient->key_array_[i - 1];
    recipient->page_id_array_[i] = recipient->page_id_array_[i - 1];
  }
  
  // Move last element to recipient's front
  recipient->key_array_[0] = middle_key;
  recipient->page_id_array_[0] = page_id_array_[last_index];
  recipient->SetSize(recipient_size + 1);
  
  // Update parent pointer for moved child
  auto child_guard = buffer_pool_manager->WritePage(page_id_array_[last_index]);
  auto child_page = child_guard.template AsMut<BPlusTreePage>();
  child_page->SetParentPageId(recipient->GetPageId());
  
  // Update my size
  ChangeSizeBy(-1);
}

/**
 * @brief Move the first key-value pair to the end of another internal page
 * @param recipient The internal page to move data to
 * @param middle_key The key from parent to be inserted
 * @param buffer_pool_manager Buffer pool manager for updating child parent pointers
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  int recipient_size = recipient->GetSize();
  
  // Move first element to recipient's end
  recipient->key_array_[recipient_size] = middle_key;
  recipient->page_id_array_[recipient_size] = page_id_array_[0];
  recipient->SetSize(recipient_size + 1);
  
  // Update parent pointer for moved child
  auto child_guard = buffer_pool_manager->WritePage(page_id_array_[0]);
  auto child_page = child_guard.template AsMut<BPlusTreePage>();
  child_page->SetParentPageId(recipient->GetPageId());
  
  // Shift remaining elements left
  for (int i = 0; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    page_id_array_[i] = page_id_array_[i + 1];
  }
  
  // Update my size
  ChangeSizeBy(-1);
}

/**
 * @brief Insert a new key-value pair after the specified old page id
 * @param old_value The page id to insert after
 * @param new_key The new key to insert
 * @param new_value The new page id to insert
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
  // Find the index of the old value
  int index = ValueIndex(old_value);
  if (index == -1) {
    throw Exception("Old value not found in internal page");
  }
  
  // Insert after the found index
  int insert_index = index + 1;
  
  // Shift elements to the right to make space
  for (int i = GetSize(); i > insert_index; i--) {
    key_array_[i] = key_array_[i - 1];
    page_id_array_[i] = page_id_array_[i - 1];
  }
  
  // Insert the new key-value pair
  key_array_[insert_index] = new_key;
  page_id_array_[insert_index] = new_value;
  
  // Increase the size
  ChangeSizeBy(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
