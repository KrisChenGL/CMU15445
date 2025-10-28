//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}


auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
     switch(txn -> GetIsolationLevel()){
          case IsolationLevel::READ_UNCOMMITTED:{
               return true;
          }     
          case IsolationLevel::SNAPSHOT_ISOLATION:{
              for (const auto& write_pair : txn->GetWriteSets() ){
              table_oid_t oid =  write_pair.first;
              auto table_heap = catalog_->GetTable(oid)->table_.get();
              for(const auto&rid :write_pair.second){
                auto [meta, tuple] = table_heap->GetTuple(rid);
                if (meta.ts_ > txn->read_ts_){
                      return false;
                }
              }            
            }
             return true;

          }
          
          case IsolationLevel::SERIALIZABLE:{
            for (const auto& write_pair: txn->GetWriteSets() ){
              table_oid_t oid =  write_pair.first;
              auto table_heap = catalog_->GetTable(oid)->table_.get();
              for(const auto&rid :write_pair.second){
                auto [meta, tuple] = table_heap->GetTuple(rid);
                if (meta.ts_ > txn->read_ts_){
                      return false;
                }
              }
             
           }
             return true;

          }
        
     }

     UNREACHABLE("Unknown IsolationLevel");
     return false;
     
  
}
/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */
// auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { 
  
//   // return true; 
// }

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  timestamp_t commit_ts_ = last_commit_ts_ + 1;
  txn->commit_ts_ = commit_ts_;
    if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }
  // TODO(fall2023): acquire commit ts!
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      break;

    case IsolationLevel::SNAPSHOT_ISOLATION:
    case IsolationLevel::SERIALIZABLE:
      if (!VerifyTxn(txn)) {
        commit_lck.unlock();
        Abort(txn);
        return false;
      }
      break;
  }

  // TODO(fall2023): Implement the commit logic!
  for(const auto &write_pair: txn->GetWriteSets()){
     table_oid_t oid =  write_pair.first;
     auto table_heap  = catalog_->GetTable(oid)->table_.get();
     for(const auto &rid: write_pair.second){
         auto [meta, tuple] = table_heap->GetTuple(rid);
         meta.ts_ = commit_ts_;
        table_heap->UpdateTupleMeta(meta, rid);
     }
  }
  
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  last_commit_ts_ = commit_ts_;
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }
  
  // TODO(fall2023): Implement the abort logic!
  for (const auto& [table_oid, rid_set] : txn->write_set_) {
    auto table_heap = catalog_->GetTable(table_oid)->table_.get();
    
    for (const auto& rid : rid_set) {
      // 获取当前页面上被"污染"的元组元数据
      auto [current_meta, current_tuple] = table_heap->GetTuple(rid);
      
      // 从 undo_logs_ 中找到对应的 undo log
      // 这里需要根据实际的逻辑来恢复元组
      // 暂时跳过具体的恢复逻辑，因为需要更多上下文信息
    }
  }

  txn->write_set_.clear();
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() { 
  auto low_watermark = GetWatermark();
  auto table_oids = catalog_->GetTableOids();
  for(const auto &oid: table_oids){
    auto table_heap = catalog_->GetTable(oid)->table_.get();
    // table_heap->ApplyGarbageCollection(low_watermark);
    for(auto it = table_heap->MakeIterator(); !it.IsEnd();++it){
        auto current_rid = it.GetRID();
        auto [current_meta,current_tupel] = it.GetTuple();
        auto current_undo_link = GetUndoLink(current_rid);
        std::vector<UndoLink>version_chain;
        while(current_undo_link.has_value() && current_undo_link->IsValid()){
              version_chain.push_back(*current_undo_link);
              auto undo_log = GetUndoLog(*current_undo_link);
              current_undo_link = undo_log.prev_version_;

              
        }
        std::optional<UndoLink> last_live_link = std::nullopt;
        for (const auto &undo_link : version_chain) {
            auto undo_log = GetUndoLog(undo_link);
            if (undo_log.ts_ >= low_watermark) {
                // 这是第一个"存活"的旧版本，它以及比它更新的旧版本都必须保留
                // 我们记录下这个链接，并停止搜索
                last_live_link = undo_link;
                break;
            }
          
         }
        auto first_dead_link_iter = std::find_if(version_chain.begin(), version_chain.end(), 
            [&](const UndoLink& link) { return GetUndoLog(link).ts_ < low_watermark; });
        if (first_dead_link_iter != version_chain.end()) {
            if (first_dead_link_iter == version_chain.begin()) {
                // 所有旧版本都"死亡"了，直接切断链头
                UpdateUndoLink(current_rid, std::nullopt);
            } else {
                // 将最后一个"存活"版本的 prev_version 指针清空
                // `std::prev` 获取前一个迭代器
                auto last_live_link_iter = std::prev(first_dead_link_iter);
                auto last_live_log = GetUndoLog(*last_live_link_iter);
                last_live_log.prev_version_ = {}; // 清空
               
                // ModifyUndoLog(*last_live_link_iter, last_live_log);
            }
        }
        
  }
   


}

}  
}// namespace bustub
