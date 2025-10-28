//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
   Context ctx;
   auto leaf_page_guard = FindLeafPage(key, false, nullptr, &ctx);
   auto leaf_page = leaf_page_guard.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
   ValueType value;
   if (leaf_page->Lookup(key, &value, comparator_)) {
     result->push_back(value);
     return true;
   }
   return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }

  auto leaf_page_guard = FindLeafPage(key, true, nullptr, &ctx);
  auto leaf_page = leaf_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  ValueType temp_value;
  if (leaf_page->Lookup(key, &temp_value, comparator_)) {
    return false;
  }
  leaf_page->Insert(key, value, comparator_);

  if (leaf_page->GetSize() > leaf_max_size_) {
    auto *new_leaf_page = Split(leaf_page);
    KeyType middle_key = new_leaf_page->KeyAt(0);
    InsertIntoParent(leaf_page, middle_key, new_leaf_page, nullptr, &ctx);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value){
     page_id_t root_page_id = bpm_->NewPage();
     auto root_page_guard = bpm_->WritePage(root_page_id);
     auto root_page_ptr = root_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
     root_page_ptr->Init(leaf_max_size_);
     root_page_ptr->Insert(key, value, comparator_);
     auto header_guard = bpm_->WritePage(header_page_id_);
     header_guard.template AsMut<BPlusTreeHeaderPage>()->root_page_id_ = root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool exclusive, Transaction *txn, Context *ctx)
    -> WritePageGuard {
  // 这个函数实现了“锁存器蟹行”协议来安全地遍历B+树
  // 1. 获取头页面读锁，找到根页面ID
  auto header_guard = bpm_->ReadPage(header_page_id_);
  page_id_t page_id = header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  header_guard.Drop(); // 立即释放头页面读锁

  // 2. 从根节点开始向下遍历，ctx.write_set_ 记录了持有写锁的路径
  auto page_guard = bpm_->WritePage(page_id);
  ctx->write_set_.push_back(std::move(page_guard));

  while (true) {
    auto page = ctx->write_set_.back().AsMut<BPlusTreePage>();

    if (page->IsLeafPage()) {
      // 已经到达叶子节点，返回其写守卫
      return std::move(ctx->write_set_.back());
    }

    // 是内部节点，查找下一个子节点
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    page_id_t child_page_id = internal_page->Lookup(key, comparator_);

    // 获取子节点的写锁
    auto child_page_guard = bpm_->WritePage(child_page_id);
    auto child_page = child_page_guard.template As<BPlusTreePage>();

    // “蟹行”的关键一步：检查子节点是否“安全”（即插入不会导致分裂）
    if (child_page->GetSize() < child_page->GetMaxSize() - 1) {
      // 如果子节点是安全的，则可以释放路径上所有祖先节点的锁
      ctx->write_set_.clear();
    }
    
    // 将子节点的锁加入路径，然后继续向下
    ctx->write_set_.push_back(std::move(child_page_guard));
  }
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  // 正确的页面创建方式
  page_id_t new_page_id = bpm_->NewPage();
  auto new_page_guard = bpm_->WritePage(new_page_id);
  auto new_node = new_page_guard.template AsMut<N>();

  // 初始化新节点
  new_node->Init(node->IsLeafPage() ? leaf_max_size_ : internal_max_size_);
  
  // 将旧节点的一半数据移动到新节点
  node->MoveHalfTo(new_node, bpm_);
  
  // 如果是叶子节点，需要更新兄弟指针
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    auto *new_leaf_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(new_node);
    new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_page_id);
  }
  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node, Transaction *txn, Context *ctx) {
  // 如果旧节点是根节点，需要创建新的根节点
  if (old_node->IsRootPage()) {
    page_id_t new_root_id = bpm_->NewPage();
    auto new_root_guard = bpm_->WritePage(new_root_id);
    auto new_root = new_root_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    
    new_root->Init(internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    
    // 更新头页面的根页面ID
    auto header_guard = bpm_->WritePage(header_page_id_);
    header_guard.template AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
    
    // 更新子节点的父页面ID
    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    return;
  }
  
  // 获取父节点
  auto parent_guard = bpm_->WritePage(old_node->GetParentPageId());
  auto parent = parent_guard.template AsMut<InternalPage>();
  
  // 在父节点中插入新的键值对
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  new_node->SetParentPageId(parent->GetPageId());
  
  // 如果父节点溢出，需要分裂
  if (parent->GetSize() > internal_max_size_) {
    auto *new_parent = Split(parent);
    KeyType middle_key = new_parent->KeyAt(0);
    InsertIntoParent(parent, middle_key, new_parent, txn, ctx);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  if (IsEmpty()) {
        return;
    }
  Context ctx;
  auto leaf_page_guard = FindLeafPage(key, true, nullptr, &ctx);

  auto leaf_page = leaf_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  int size_before_remove = leaf_page->GetSize();
  int size_after_remove = leaf_page->RemoveAndDeleteRecord(key, comparator_);
  if (size_after_remove == size_before_remove) {
    return;
  }
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    // 如果下溢，则调用辅助函数进行重分配或合并
    CoalesceOrRedistribute(leaf_page, nullptr, &ctx);
  }
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *txn, Context *ctx) {
    // 如果当前节点是根节点
    if (node->IsRootPage()) {
        // 如果根节点在调整后变为空（例如，内部节点的最后一个指针被移除），则树变为空
        AdjustRoot(node);
        return;
    }

    // 获取父节点
    auto parent_page_guard = bpm_->WritePage(node->GetParentPageId());
    auto parent_page = parent_page_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    
    // 在父节点中找到当前节点的位置
    int node_idx_in_parent = parent_page->ValueIndex(node->GetPageId());
    
    // 尝试获取左兄弟节点
    if (node_idx_in_parent > 0) {
        page_id_t left_sibling_id = parent_page->ValueAt(node_idx_in_parent - 1);
        auto left_sibling_guard = bpm_->WritePage(left_sibling_id);
        auto left_sibling = left_sibling_guard.template AsMut<N>();
        // 如果左兄弟可以“借”出一个元素
        if (left_sibling->GetSize() > left_sibling->GetMinSize()) {
            Redistribute(left_sibling, node, parent_page, node_idx_in_parent);
            return;
        }
    }
    
    // 尝试获取右兄弟节点
    if (node_idx_in_parent < parent_page->GetSize() - 1) {
        page_id_t right_sibling_id = parent_page->ValueAt(node_idx_in_parent + 1);
        auto right_sibling_guard = bpm_->WritePage(right_sibling_id);
        auto right_sibling = right_sibling_guard.template AsMut<N>();
        // 如果右兄弟可以“借”出一个元素
        if (right_sibling->GetSize() > right_sibling->GetMinSize()) {
            Redistribute(node, right_sibling, parent_page, node_idx_in_parent + 1);
            return;
        }
    }

    // 如果两边都借不了，只能进行合并
    if (node_idx_in_parent > 0) {
        // 与左兄弟合并
        page_id_t left_sibling_id = parent_page->ValueAt(node_idx_in_parent - 1);
        auto left_sibling_guard = bpm_->WritePage(left_sibling_id);
        auto left_sibling = left_sibling_guard.template AsMut<N>();
        Coalesce(&left_sibling, &node, &parent_page, node_idx_in_parent, txn, ctx);
    } else {
        // 与右兄弟合并
        page_id_t right_sibling_id = parent_page->ValueAt(node_idx_in_parent + 1);
        auto right_sibling_guard = bpm_->WritePage(right_sibling_id);
        auto right_sibling = right_sibling_guard.template AsMut<N>();
        Coalesce(&node, &right_sibling, &parent_page, node_idx_in_parent + 1, txn, ctx);
    }
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                               int index, Transaction *txn, Context *ctx) {
    // 合并 `node` 到 `neighbor_node` (左邻居)
    KeyType middle_key = (*parent)->KeyAt(index);
    if ((*node)->IsLeafPage()){
        // 叶子节点的合并
        auto *leaf_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(*node);
        auto *neighbor_leaf_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(*neighbor_node);
        leaf_node->MoveAllTo(neighbor_leaf_node);
        neighbor_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    } else {
        // 内部节点的合并
        auto *internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(*node);
        auto *neighbor_internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(*neighbor_node);
        // 需要把父节点的分隔键也拉下来
        internal_node->MoveAllTo(neighbor_internal_node, middle_key, bpm_);
    }

    // 从父节点中移除指向 `node` 的指针
    (*parent)->Remove(index);
    bpm_->DeletePage((*node)->GetPageId());

    // 递归检查父节点是否也需要合并/重分配
    if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
        CoalesceOrRedistribute(*parent, txn, ctx);
    }
}
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
                                  int index) {
    if (node->IsLeafPage()) {
        auto leaf_node = reinterpret_cast<LeafPage*>(node);
        auto neighbor_leaf_node = reinterpret_cast<LeafPage*>(neighbor_node);
        if (neighbor_node < node) { // neighbor是左兄弟
            // 从左兄弟借最后一个元素
            neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
            // 更新父节点的分隔键
            parent->SetKeyAt(index, leaf_node->KeyAt(0));
        } else { // neighbor是右兄弟
            // 从右兄弟借第一个元素
            neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
            // 更新父节点的分隔键
            parent->SetKeyAt(index, neighbor_leaf_node->KeyAt(0));
        }
    } else { // 内部节点
        // 内部节点的重分配更复杂，需要“旋转”父节点中的分隔键
        auto internal_node = reinterpret_cast<InternalPage*>(node);
        auto neighbor_internal_node = reinterpret_cast<InternalPage*>(neighbor_node);
        if (neighbor_node < node) { // neighbor是左兄弟
            neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), bpm_);
            parent->SetKeyAt(index, internal_node->KeyAt(0));
        } else { // neighbor是右兄弟
            neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(index), bpm_);
            parent->SetKeyAt(index, neighbor_internal_node->KeyAt(0));
        }
    }
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    // 如果根节点不是叶子节点，并且其子节点数量只剩下一个
    if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
        auto root_page = reinterpret_cast<InternalPage *>(old_root_node);
        page_id_t new_root_id = root_page->RemoveAndReturnOnlyChild();
        // 将唯一的子节点设为新的根
        auto header_guard = bpm_->WritePage(header_page_id_);
        header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_id;
        
        // 更新新根的父页面ID为无效
        auto new_root_guard = bpm_->WritePage(new_root_id);
        new_root_guard.AsMut<BPlusTreePage>()->SetParentPageId(INVALID_PAGE_ID);
    } else if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
        // 如果根是叶子节点且已空，则树变为空
        auto header_guard = bpm_->WritePage(header_page_id_);
        header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
    }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
 //all
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  if (IsEmpty()) {
    return End();
  }
  // 从根开始，一路向左下找到最左边的叶子
  auto guard = bpm_->ReadPage(GetRootPageId());
  auto page = guard.template As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page);
    guard = bpm_->ReadPage(internal_page->ValueAt(0));
    page = guard.template As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_, guard.GetPageId(), 0);

  // UNIMPLEMENTED("TODO(P2): Add implementation."); 
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
 //range and root
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  if (IsEmpty()) {
    return End();
  }
  Context ctx;
  auto leaf_page_guard = FindLeafPage(key, false, nullptr, &ctx);
  auto leaf_page = leaf_page_guard.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  // Binary search to find the key index
  int left = 0, right = leaf_page->GetSize() - 1;
  int index = leaf_page->GetSize(); // Default to end if not found
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator_(leaf_page->KeyAt(mid), key) == 0) {
      index = mid;
      break;
    } else if (comparator_(leaf_page->KeyAt(mid), key) < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return INDEXITERATOR_TYPE(bpm_, leaf_page_guard.GetPageId(), index);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { 
   return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, 0);
  // UNIMPLEMENTED("TODO(P2): Add implementation."); 
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  ReadPageGuard page_guard = bpm_->ReadPage(header_page_id_);
  return page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  // UNIMPLEMENTED("TODO(P2): Add implementation."); 
}

}  // namespace bustub

// Template instantiations
template class bustub::BPlusTree<bustub::GenericKey<4>, bustub::RID, bustub::GenericComparator<4>>;
template class bustub::BPlusTree<bustub::GenericKey<8>, bustub::RID, bustub::GenericComparator<8>>;
template class bustub::BPlusTree<bustub::GenericKey<16>, bustub::RID, bustub::GenericComparator<16>>;
template class bustub::BPlusTree<bustub::GenericKey<32>, bustub::RID, bustub::GenericComparator<32>>;
template class bustub::BPlusTree<bustub::GenericKey<64>, bustub::RID, bustub::GenericComparator<64>>;
