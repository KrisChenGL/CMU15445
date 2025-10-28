//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.h
//
// Identification: src/include/storage/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <string>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SLOT_CNT \
  ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / ((int)(sizeof(KeyType) + sizeof(ValueType))))  // NOLINT

/**
 * Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: Since the number of keys does not equal to number of child pointers,
 * the first key in key_array_ always remains invalid. That is to say, any search / lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ------------------------------------------
 * | KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
 *  ------------------------------------------
 *  ---------------------------------------------
 * | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
 *  ---------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  void Init(int max_size = INTERNAL_PAGE_SLOT_CNT);

  auto KeyAt(int index) const -> KeyType;

  void SetKeyAt(int index, const KeyType &key);

  /**
   * @param value The value to search for
   * @return The index that corresponds to the specified value
   */
  auto ValueIndex(const ValueType &value) const -> int;

  auto ValueAt(int index) const -> ValueType;

  /**
   * @brief Find the child page id for the given key
   * 
   * @param key The key to search for
   * @param comparator The key comparator
   * @return The page id of the child page that should contain the key
   */
  auto Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType;

  /**
   * @brief Remove and return the only child page id when the internal page has only one child
   * This is used during root adjustment when the root has only one child
   * 
   * @return The page id of the only child
   */
  auto RemoveAndReturnOnlyChild() -> ValueType;

  /**
   * @brief Move all key-value pairs to another internal page (used during merge)
   * @param recipient The internal page to move all data to
   * @param middle_key The key from parent to be inserted between the two pages
   * @param buffer_pool_manager Buffer pool manager for updating child parent pointers
   */
  void MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager);

  /**
   * @brief Remove a key-value pair at the specified index
   * @param index The index of the key-value pair to remove
   */
  void Remove(int index);

  /**
   * @brief Move the last key-value pair to the front of another internal page
   * @param recipient The internal page to move data to
   * @param middle_key The key from parent to be inserted
   * @param buffer_pool_manager Buffer pool manager for updating child parent pointers
   */
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager);

  /**
   * @brief Move the first key-value pair to the end of another internal page
   * @param recipient The internal page to move data to
   * @param middle_key The key from parent to be inserted
   * @param buffer_pool_manager Buffer pool manager for updating child parent pointers
   */
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager);

  /**
   * @brief Insert a new key-value pair after the specified old page id
   * @param old_value The page id to insert after
   * @param new_key The new key to insert
   * @param new_value The new page id to insert
   */
  void InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

  /**
   * @brief Move half of the key-value pairs to the new internal page (used during split)
   * @param recipient The new internal page to move data to
   * @param buffer_pool_manager Buffer pool manager for updating child parent pointers
   */
  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);

  /**
   * @brief Populate a new root page with the first key-value pair
   * @param old_value The page id of the left child
   * @param new_key The key to separate left and right children
   * @param new_value The page id of the right child
   */
  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return The string representation of all keys in the current internal page
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // First key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  // Array members for page data.
  KeyType key_array_[INTERNAL_PAGE_SLOT_CNT];
  ValueType page_id_array_[INTERNAL_PAGE_SLOT_CNT];
  
  // (Spring 2025) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
