//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include <condition_variable>
namespace bustub {

/**
 * Acquire a lock on table_oid_t in the given lock_mode.
 * If the transaction already holds a lock on the table, upgrade the lock
 * to the specified lock_mode (if possible).
 *
 * This method should abort the transaction and throw a
 * TransactionAbortException under certain circumstances.
 * See [LOCK_NOTE] in header file.
 *
 * @param txn the transaction requesting the lock upgrade
 * @param lock_mode the lock mode for the requested lock
 * @param oid the table_oid_t of the table to be locked in lock_mode
 * @return true if the upgrade is successful, false otherwise
 */
// --- 锁兼容性矩阵 ---
//       |  IS |  IX |  S  | SIX |  X  |
// ------|-----|-----|-----|-----|-----|
// IS    |  T  |  T  |  T  |  T  |  F  |
// IX    |  T  |  T  |  F  |  F  |  F  |
// S     |  T  |  F  |  T  |  F  |  F  |
// SIX   |  T  |  F  |  F  |  F  |  F  |
// X     |  F  |  F  |  F  |  F  |  F  |

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
    if (l1 == LockMode::EXCLUSIVE || l2 == LockMode::EXCLUSIVE) {
        return false;
    }
    if (l1 == LockMode::SHARED_INTENTION_EXCLUSIVE || l2 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return l1 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_SHARED;
    }
    if (l1 == LockMode::SHARED && l2 == LockMode::INTENTION_EXCLUSIVE) {
        return false;
    }
    if (l1 == LockMode::INTENTION_EXCLUSIVE && l2 == LockMode::SHARED) {
        return false;
    }
    return true;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
    if (curr_lock_mode == LockMode::INTENTION_SHARED) {
        return requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
               requested_lock_mode == LockMode::INTENTION_EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    } 
    if (curr_lock_mode == LockMode::SHARED) {
        return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    }
    if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
        return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    }
    if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return requested_lock_mode == LockMode::EXCLUSIVE;
    }
    return false;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool {
    // Check if transaction has appropriate table lock for the requested row lock
    // For now, assume any table lock is sufficient since we don't have access to table lock sets
    // In a complete implementation, you would check lock compatibility
    return true;
}
auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode)->bool{
    
  if (txn->GetTransactionState() == TransactionState::COMMITTED || txn->GetTransactionState() == TransactionState::ABORTED) {
       return false;
    }
    auto iso = txn->GetIsolationLevel();
    switch(iso){
      case IsolationLevel::READ_UNCOMMITTED:
               if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
                   lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
                         txn->SetTainted();
                         return false;
              }
              break;
    
      case IsolationLevel::SNAPSHOT_ISOLATION:
      case IsolationLevel::SERIALIZABLE:
        break;
    }

    if (txn->GetTransactionState() == TransactionState::SHRINKING) {
      txn->SetTainted();
      return false;
    }

     return true;
}
void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue){
    if (lock_request_queue->request_queue_.empty()) {
        return;
    }
    auto &first_req = lock_request_queue->request_queue_.front();
    if(lock_request_queue->request_queue_.size() == 1){
        first_req->granted_ = true;
        return;
    }
    if (first_req->lock_mode_ == LockMode::EXCLUSIVE){
         bool has_other_granted = false;
         for(const auto& req:lock_request_queue->request_queue_){
             if(req!= first_req && req->granted_){
                has_other_granted = true;
                break; 
             }
             
         }
         if (!has_other_granted){
            first_req->granted_ = true;
         }

    }

}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool { 
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
 }
  std::shared_ptr<LockRequestQueue>queue;
  // table_lock_map_latch_.lock();
{
  std::lock_guard<std::mutex> guard(table_lock_map_latch_);
  if (table_lock_map_.find(oid) == table_lock_map_.end()){
      table_lock_map_[oid] = std::make_shared<LockRequestQueue>();   
  }
  queue = table_lock_map_[oid]; 

}
  //是使用share_ptr管理的LockRequestQueue类，线程安全
  // table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(queue->latch_);
  for(const auto& req:queue->request_queue_){
    if(req->txn_id_ == txn->GetTransactionId() && req->granted_){
      if( req ->lock_mode_ == lock_mode){
          return true;
      }
      if (!CanLockUpgrade(req->lock_mode_, lock_mode)) {
        txn->SetTainted();
        return false;
      }
      queue->upgrading_ = txn->GetTransactionId();
      req->lock_mode_ = lock_mode;
      break;
    }
  }
  
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(),lock_mode,oid);
  queue->request_queue_.push_back(request);
  while (!request->granted_) {
         GrantNewLocksIfPossible(queue.get());
         if (!request->granted_){
            queue->cv_.wait(lock);
            if(txn->GetTransactionState() == TransactionState::ABORTED){
                queue->request_queue_.remove(request);
                queue->cv_.notify_all();
                return false;
            }
            
         }
  }
  // Remove the line: txn->GetTableLockSet()->insert(oid);
  return true; 
}

/**
 * Release the lock held on a table by the transaction.
 *
 * This method should abort the transaction and throw a
 * TransactionAbortException under certain circumstances.
 * See [UNLOCK_NOTE] in header file.
 *
 * @param txn the transaction releasing the lock
 * @param oid the table_oid_t of the table to be unlocked
 * @return true if the unlock is successful, false otherwise
 */
auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
    row_lock_map_latch_.lock();
    for(const auto& pair: row_lock_map_){
        std::unique_lock<std::mutex>lock(pair.second->latch_);
        for(const auto& req: pair.second->request_queue_){
           if( req->oid_ ==oid && req->txn_id_ == txn->GetTransactionId()&& req->granted_){
               row_lock_map_latch_.unlock();
               txn->SetTainted();
               return false;
           }
        }     
    }
   row_lock_map_latch_.unlock();
   
   table_lock_map_latch_.lock();
   if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetTainted();
    return false;
   }
  auto queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock(); 
  
  std::unique_lock<std::mutex>lock(queue->latch_);
  bool found = false;
  for(auto it = queue->request_queue_.begin(); it != queue->request_queue_.end();++it){
      if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_){
          queue->request_queue_.erase(it);
          found = true;
          break;
      }
  }
  if (!found) {
    txn->SetTainted();
    return false;
  }
  GrantNewLocksIfPossible(queue.get());
  queue->cv_.notify_all(); 
  return true;
  
  // return true; 
}

/**
 * Acquire a lock on rid in the given lock_mode.
 * If the transaction already holds a lock on the row, upgrade the lock
 * to the specified lock_mode (if possible).
 *
 * This method should abort the transaction and throw a
 * TransactionAbortException under certain circumstances.
 * See [LOCK_NOTE] in header file.
 *
 * @param txn the transaction requesting the lock upgrade
 * @param lock_mode the lock mode for the requested lock
 * @param oid the table_oid_t of the table the row belongs to
 * @param rid the RID of the row to be locked
 * @return true if the upgrade is successful, false otherwise
 */
auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
   if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetTainted();
    return false;
  }
  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }
  if (!CheckAppropriateLockOnTable(txn,oid,lock_mode)){
        txn->SetTainted();
        return false;
  }
  std::shared_ptr<LockRequestQueue>queue;
  {
    std::unique_lock<std::mutex> lock(row_lock_map_latch_);
    if (row_lock_map_.find(rid) == row_lock_map_.end()) {
      row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    }
    queue = row_lock_map_[rid];
  }
  std::unique_lock<std::mutex> lock(queue->latch_);
  for (auto it = queue->request_queue_.begin(); it != queue->request_queue_.end(); ++it) {
      if ((*it)->txn_id_ == txn->GetTransactionId()) {
          if((*it)->lock_mode_ == lock_mode) {
              return true; // 已持有相同锁
          }
          
          throw NotImplementedException("Row Lock Upgrade is not implemented.");
      }
  }
  auto request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  queue->request_queue_.push_back(request);
  while (!request->granted_) {
    GrantNewLocksIfPossible(queue.get());
    if (!request->granted_) {
      queue->cv_.wait(lock);
      if (txn->GetTransactionState() == TransactionState::ABORTED) {
        queue->request_queue_.remove(request);
        queue->cv_.notify_all();
        return false;
      }
    }
  }
  
  return true;
}

/**
 * Release the lock held on a row by the transaction.
 *
 * This method should abort the transaction and throw a
 * TransactionAbortException under certain circumstances.
 * See [UNLOCK_NOTE] in header file.
 *
 * @param txn the transaction releasing the lock
 * @param rid the RID that is locked by the transaction
 * @param oid the table_oid_t of the table the row belongs to
 * @param rid the RID of the row to be unlocked
 * @param force unlock the tuple regardless of isolation level, not changing the transaction state
 * @return true if the unlock is successful, false otherwise
 */
auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetTainted();
    return false;
  }
  auto queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(queue->latch_);
  bool found = false;
  for(auto it = queue->request_queue_.begin(); it != queue->request_queue_.end();++it){
      if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_){
          queue->request_queue_.erase(it);
          found = true;
          break;
      }
  }
  if (!found) {
    txn->SetTainted();
    return false;
  }
  GrantNewLocksIfPossible(queue.get());
  queue->cv_.notify_all();
  return true;
}

void LockManager::UnlockAll() {
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  table_lock_map_.clear();
  row_lock_map_.clear();
  waits_for_.clear();
  // You probably want to unlock all table and txn locks here.
}

/**
 * Adds an edge from t1 -> t2 from waits for graph.
 * @param t1 transaction waiting for a lock
 * @param t2 transaction being waited for
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
    std::lock_guard<std::mutex>lock(waits_for_latch_);
    auto &t1_waits = waits_for_[t1];
    if (std::find(t1_waits.begin(), t1_waits.end(), t2) == t1_waits.end()) {
    t1_waits.push_back(t2);
  }
}

/**
 * Removes an edge from t1 -> t2 from waits for graph.
 * @param t1 transaction waiting for a lock
 * @param t2 transaction being waited for
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    std::lock_guard<std::mutex> lock(waits_for_latch_);
    if (waits_for_.count(t1) > 0) {
    auto &t1_waits = waits_for_[t1];
    t1_waits.erase(std::remove(t1_waits.begin(), t1_waits.end(), t2), t1_waits.end());
    if (t1_waits.empty()) {
      waits_for_.erase(t1);
    }
  }
}

/**
 * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
 * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
 * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
 */
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { 
  // return false; 
  std::unordered_set<txn_id_t>visited_nodes;
  std::unordered_set<txn_id_t>on_path_nodes;
  std::vector<txn_id_t>path;
  std::vector<txn_id_t>all_txns;
  for( const auto &pair:waits_for_){
      all_txns.push_back(pair.first);
  }
  
  std::sort(all_txns.begin(),all_txns.end());
  for(const auto txn_node : all_txns){
      if (visited_nodes.find(txn_node) == visited_nodes.end()){
          if(FindCycle(txn_node, path, on_path_nodes, visited_nodes, txn_id)){
             return true;
          }

      }
  }
  return false;

}
auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                           std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
    // Mark current transaction as visited and on path
    visited.insert(source_txn);
    on_path.insert(source_txn);
    path.push_back(source_txn);

    // Check all outgoing edges from current transaction
    if (waits_for_.find(source_txn) != waits_for_.end()) {
        for (auto next_txn : waits_for_[source_txn]) {
            if (on_path.find(next_txn) != on_path.end()) {
                // Found a cycle, find the youngest transaction to abort
                *abort_txn_id = next_txn;
                for (auto txn_in_cycle : path) {
                    if (on_path.find(txn_in_cycle) != on_path.end() && txn_in_cycle > *abort_txn_id) {
                        *abort_txn_id = txn_in_cycle;
                    }
                }
                return true;
            }
            
            if (visited.find(next_txn) == visited.end()) {
                if (FindCycle(next_txn, path, on_path, visited, abort_txn_id)) {
                    return true;
                }
            }
        }
    }

    // Remove from path when backtracking
    on_path.erase(source_txn);
    path.pop_back();
    return false;
}

/**
 * @return all edges in current waits_for graph
 */
auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  std::lock_guard<std::mutex> lock(waits_for_latch_);
  for(const auto &pair:waits_for_){
      for(const auto& wait :pair.second){
          edges.emplace_back(pair.first,wait);
      }

  }
  return edges;
}

/**
 * Runs cycle detection in the background.
 */
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  
      // TODO(students): detect deadlock
       BuildWaitsForGraph();
       txn_id_t abort_txn_id;
       while(HasCycle(&abort_txn_id)){
           std::shared_lock<std::shared_mutex> lock(txn_manager_->txn_map_mutex_);
           auto it = txn_manager_->txn_map_.find(abort_txn_id);
           if (it != txn_manager_->txn_map_.end()) {
               auto *txn_to_abort = it->second.get();
               lock.unlock();
               txn_manager_->Abort(txn_to_abort);
           }
       }
      // waits_for_.clear();

    }
  }
}
void LockManager::BuildWaitsForGraph(){
    std::lock_guard<std::mutex> waits_for_guard(waits_for_latch_);
    waits_for_.clear();

    auto build_from_map = [&](auto &lock_map, std::mutex &map_latch){
          std::lock_guard<std::mutex> map_guard(map_latch);
          for(const auto&pair:lock_map){
              std::lock_guard<std::mutex> queue_guard(pair.second->latch_);
              std::vector<txn_id_t> granted_txns;
              for (const auto &req : pair.second->request_queue_) {
                if (req->granted_) {
                    granted_txns.push_back(req->txn_id_);
                }
              }
              for (const auto &req : pair.second->request_queue_){
                  if(!req->granted_){
                        for (const auto &granted_txn_id : granted_txns) {
                            AddEdge(req->txn_id_, granted_txn_id);
                        }
                  }
              }
          }
    };
    
    build_from_map(table_lock_map_, table_lock_map_latch_);
    build_from_map(row_lock_map_, row_lock_map_latch_);
}

}  
// namespace bustub
