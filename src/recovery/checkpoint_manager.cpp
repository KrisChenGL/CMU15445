//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// checkpoint_manager.cpp
//
// Identification: src/recovery/checkpoint_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "concurrency/transaction_manager.h"
#include "recovery/log_manager.h" 
#include "recovery/checkpoint_manager.h"

namespace bustub {

static std::unique_lock<std::shared_mutex> checkpoint_latch;
void CheckpointManager::BeginCheckpoint() {
  // Block all the transactions and ensure that both the WAL and all dirty buffer pool pages are persisted to disk,
  // creating a consistent checkpoint. Do NOT allow transactions to resume at the end of this method, resume them
  // in CheckpointManager::EndCheckpoint() instead. This is for grading purposes.
   checkpoint_latch = std::unique_lock(transaction_manager_->txn_map_mutex_);

   while(!transaction_manager_->running_txns_.current_reads_.empty()){
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
   }
   auto begin_lsn = log_manager_->AppendLogRecord(new LogRecord(LogRecordType::BeginCheckpoint));
   while(log_manager_->GetPersistentLSN()<begin_lsn){
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
   }

   buffer_pool_manager_->FlushAllPages();
}

void CheckpointManager::EndCheckpoint() {
  // Allow transactions to resume, completing the checkpoint.
  auto end_lsn = log_manager_->AppendLogRecord(new LogRecord(LogRecordType::EndCheckpoint));
  while (log_manager_->GetPersistentLSN() < end_lsn) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
   checkpoint_latch.unlock();

} 
} // namespace bustub
