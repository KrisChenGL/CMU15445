//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"
#include <exception>
#include <algorithm>
#include <limits>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
   if (current_reads_.empty()) {
    watermark_ = read_ts;
  }
  current_reads_[read_ts]++;
  watermark_ = std::min(watermark_, read_ts);
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
    if(current_reads_.find(read_ts)== current_reads_.end()){
        return;
    }
    current_reads_[read_ts]--;
    if (current_reads_[read_ts] == 0) {
       current_reads_.erase(read_ts);
    }
    if (read_ts == watermark_) {
    if (current_reads_.empty()) {
        watermark_ = commit_ts_;
      }
      else{
          watermark_ = std::min_element(current_reads_.begin(), current_reads_.end(),[](const auto&a ,const auto& b){return a.first < b.first;})->first;
      }
    }
}  
}// namespace bustub
