//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"
#include <cstring>

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
     if(enable_logging){
         enable_flushing_ = true;
         flush_thread_ = new std::thread([&](){
             while(enable_flushing_){
                 std::unique_lock<std::mutex>lock(latch_);
                 if(!cv_.wait_for(lock,log_timeout,[&](){
                      return log_buffer_offset_ > 0;
                 })){
                     if (log_buffer_offset_ == 0) {
                        continue;
                    }
                 }
                 size_t size_to_flush = log_buffer_offset_;
                 std::swap(log_buffer_,flush_buffer_);
                 log_buffer_offset_ = 0;
                 lsn_t lsn_to_presist = next_lsn_ - 1;
                 lock.unlock();
                 append_cv_.notify_all();
                 disk_manager_->WriteLog(flush_buffer_, size_to_flush);
                 SetPersistentLSN(lsn_to_presist);

             }
         });
     }
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
      if (!enable_flushing_) {
        return;
      }
      enable_flushing_ = false;
      cv_.notify_one();
      if(flush_thread_!=nullptr){
         flush_thread_->join();
         delete flush_thread_;
         flush_thread_ = nullptr;
         
      }

}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
auto LogManager::AppendLogRecord(LogRecord *log_record) -> lsn_t { 
    std::unique_lock<std::mutex> lock(latch_);
    log_record->lsn_=next_lsn_++;
    while (log_buffer_offset_ + log_record->GetSize() > LOG_BUFFER_SIZE){
          cv_.notify_one();
          append_cv_.wait(lock);
    }
    memcpy(log_buffer_ + log_buffer_offset_, log_record, LogRecord::HEADER_SIZE);
    int pos = log_buffer_offset_ + LogRecord::HEADER_SIZE;
    if (log_record->log_record_type_ == LogRecordType::INSERT) {
    memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
    }
    log_buffer_offset_ += log_record->GetSize();
    return log_record->lsn_;
    // return INVALID_LSN; 
 }

}  // namespace bustub
