//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : node_store_(), current_timestamp_(0), replacer_size_(num_frames), k_(k), current_size_(0), is_accessible_(), latch_(), use_count_(), history_list_(), history_map_(), cache_list_(), cache_map_() {
 
    is_accessible_.resize(num_frames,false);
    
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
//标准lru_k算法
auto LRUKReplacer::Evict(frame_id_t* frame_id) -> std::optional<frame_id_t> { 
    std::lock_guard<std::mutex> guard(latch_);
    if (current_size_ == 0) {
        return std::nullopt;
    }
    
    // 淘汰策略：
    // 1. 优先淘汰 history_list_（访问次数 < K 的页面）
    for (auto it = history_list_.begin(); it != history_list_.end(); ++it) {
        frame_id_t fid = *it;
        // 检查frame_id是否在有效范围内，并且是可驱逐的
        if (fid >= 0 && static_cast<size_t>(fid) < replacer_size_ && is_accessible_[fid]) {
            // Found evictable frame in history list
            history_list_.erase(it);
            history_map_.erase(fid);
            node_store_.erase(fid);
            use_count_.erase(fid);
            is_accessible_[fid] = false;
            current_size_--;
            if (frame_id != nullptr) {
                *frame_id = fid;
            }
            return fid;
        }
    }
    
    // 2. 如果 history_list_ 中没有可驱逐的页面，再淘汰 cache_list_（访问次数 >= K，LRU顺序）
    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
        frame_id_t fid = *it;
        // 检查frame_id是否在有效范围内，并且是可驱逐的
        if (fid >= 0 && static_cast<size_t>(fid) < replacer_size_ && is_accessible_[fid]) {
            // Found evictable frame in cache list
            cache_list_.erase(it);
            cache_map_.erase(fid);
            node_store_.erase(fid);
            use_count_.erase(fid);
            is_accessible_[fid] = false;
            current_size_--;
            if (frame_id != nullptr) {
                *frame_id = fid;
            }
            return fid;
        }
    }
    
    return std::nullopt;
}

//使用哈希+list实现的lru_k算法
// auto LRUKReplacer::Evict(frame_id_t *frame_id) -> std::optional<frame_id_t> {
//     std::lock_guard<std::mutex> guard(latch_);
//     if (current_size_ == 0) {
//       return std::nullopt;
//     }
  
//     // 淘汰策略：
//     // 1. 优先淘汰 history_list_（访问次数 < K 的页面）
//     if (!history_list_.empty()) {
//       frame_id_t victim = history_list_.front();
//       history_list_.pop_front();
//       history_map_.erase(victim);
//       is_accessible_[victim] = false;
//       current_size_--;
//       if (frame_id != nullptr) {
//         *frame_id = victim;
//       }
//       return victim;
//     }
  
//     // 2. 如果 history_list_ 空，再淘汰 cache_list_（访问次数 >= K，LRU顺序）
//     if (!cache_list_.empty()) {
//       frame_id_t victim = cache_list_.front();
//       cache_list_.pop_front();
//       cache_map_.erase(victim);
//       is_accessible_[victim] = false;
//       current_size_--;
//       if (frame_id != nullptr) {
//         *frame_id = victim;
//       }
//       return victim;
//     }
  
//     return std::nullopt;
//   }
// void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
//     std::lock_guard<std::mutex> guard(latch_);
//     if (frame_id >= static_cast<int>(replacer_size_)) {
//       throw Exception("Invalid frame id in RecordAccess");
//     }
  
//     current_timestamp_++;
//     size_t &count = use_count_[frame_id];
//     count++;
  
//     // 新页面，第一次访问 → 放到 history_list_
//     if (count == 1) {
//       history_list_.push_back(frame_id);
//       history_map_[frame_id] = --history_list_.end();
//       is_accessible_[frame_id] = false; // 默认不可驱逐，SetEvictable 决定
//     }
//     // 访问次数到 K → 从 history_list_ 移到 cache_list_
//     else if (count == k_) {
//       if (history_map_.count(frame_id)) {
//         history_list_.erase(history_map_[frame_id]);
//         history_map_.erase(frame_id);
//       }
//       cache_list_.push_back(frame_id);
//       cache_map_[frame_id] = --cache_list_.end();
//     }
//     // 访问次数 > K → 仅在 cache_list_ 中更新 LRU 位置
//     else if (count > k_ && cache_map_.count(frame_id)) {
//       cache_list_.erase(cache_map_[frame_id]);
//       cache_list_.push_back(frame_id);
//       cache_map_[frame_id] = --cache_list_.end();
//     }
//   }
  
//   void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
//     std::lock_guard<std::mutex> guard(latch_);
//     if (frame_id >= static_cast<int>(replacer_size_)) {
//       throw Exception("Invalid frame id in SetEvictable");
//     }
//     // Frame 不存在，直接返回
//     if (use_count_.find(frame_id) == use_count_.end()) {
//       return;
//     }
//     bool was = is_accessible_[frame_id];
//     if (!was && set_evictable) {
//       current_size_++;
//     } else if (was && !set_evictable) {
//       current_size_--;
//     }
//     is_accessible_[frame_id] = set_evictable;
//   }
  
//   void LRUKReplacer::Remove(frame_id_t frame_id) {
//     std::lock_guard<std::mutex> guard(latch_);
//     if (frame_id >= static_cast<int>(replacer_size_)) {
//       throw Exception("Invalid frame id in Remove");
//     }
//     auto it = use_count_.find(frame_id);
//     if (it == use_count_.end()) {
//       return; // 不存在
//     }
//     if (!is_accessible_[frame_id]) {
//       throw Exception("Cannot remove a non-evictable frame");
//     }
//     // 从双队列中删除
//     if (history_map_.count(frame_id)) {
//       history_list_.erase(history_map_[frame_id]);
//       history_map_.erase(frame_id);
//     } else if (cache_map_.count(frame_id)) {
//       cache_list_.erase(cache_map_[frame_id]);
//       cache_map_.erase(frame_id);
//     }
//     use_count_.erase(it);
//     is_accessible_[frame_id] = false;
//     current_size_--;
//   }
  


/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex> guard(latch_);
    if (frame_id >= static_cast<int>(replacer_size_)) {
        throw std::exception();
    }
    
    current_timestamp_++;
    size_t &count = use_count_[frame_id];
    count++;
    
    // 新页面，第一次访问 → 放到 history_list_
    if (count == 1) {
        if (node_store_.find(frame_id) == node_store_.end()) {
            node_store_.emplace(frame_id, LRUKNode());
        }
        history_list_.push_back(frame_id);
        history_map_[frame_id] = --history_list_.end();
        is_accessible_[frame_id] = false; // 默认不可驱逐，SetEvictable 决定
    }
    // 访问次数到 K → 从 history_list_ 移到 cache_list_
    else if (count == k_) {
        if (history_map_.count(frame_id)) {
            history_list_.erase(history_map_[frame_id]);
            history_map_.erase(frame_id);
        }
        cache_list_.push_back(frame_id);
        cache_map_[frame_id] = --cache_list_.end();
    }
    // 访问次数 > K → 仅在 cache_list_ 中更新 LRU 位置
    else if (count > k_ && cache_map_.count(frame_id)) {
        cache_list_.erase(cache_map_[frame_id]);
        cache_list_.push_back(frame_id);
        cache_map_[frame_id] = --cache_list_.end();
    }
}
 


/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> guard(latch_);
    if (frame_id >= static_cast<int>(replacer_size_)) {
        throw std::exception();
    }
    // Frame 不存在，直接返回
    if (use_count_.find(frame_id) == use_count_.end()) {
        return;
    }
    bool was = is_accessible_[frame_id];
    if (!was && set_evictable) {
        current_size_++;
    } else if (was && !set_evictable) {
        current_size_--;
    }
    is_accessible_[frame_id] = set_evictable;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);
    if (frame_id >= static_cast<int>(replacer_size_)) {
        throw std::exception();
    }
    auto it = use_count_.find(frame_id);
    if (it == use_count_.end()) {
        return; // 不存在
    }
    if (!is_accessible_[frame_id]) {
        throw std::exception(); // Cannot remove a non-evictable frame
    }
    // 从双队列中删除
    if (history_map_.count(frame_id)) {
        history_list_.erase(history_map_[frame_id]);
        history_map_.erase(frame_id);
    } else if (cache_map_.count(frame_id)) {
        cache_list_.erase(cache_map_[frame_id]);
        cache_map_.erase(frame_id);
    }
    node_store_.erase(frame_id);
    use_count_.erase(it);
    is_accessible_[frame_id] = false;
    current_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return current_size_; }

}  // namespace bustub
