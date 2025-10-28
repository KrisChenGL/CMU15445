//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      pool_size_(num_frames),
      next_page_id_(0),
      pages_(new Page[num_frames]),
      disk_manager_(disk_manager),
      log_manager_(log_manager),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_unique<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)) {
  
  frames_.resize(num_frames);
  for(size_t i = 0; i < num_frames; ++i){
      frames_[i] = std::make_shared<FrameHeader>(static_cast<frame_id_t>(i));
      free_list_.emplace_back(static_cast<frame_id_t>(i));
  }

  // // Not strictly necessary...
  // std::scoped_lock latch(*bpm_latch_);

  // // Initialize the monotonically increasing counter at 0.
  // next_page_id_.store(0);

  // // Allocate all of the in-memory frames up front.
  // frames_.reserve(num_frames_);

  // // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  // page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  // for (size_t i = 0; i < num_frames_; i++) {
  //   frames_.push_back(std::make_shared<FrameHeader>(i));
  //   free_frames_.push_back(static_cast<int>(i));
  // }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager(){ FlushAllPages();};

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { 
  return page_table_.size();
  // return num_frames_; 
  
}

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  std::lock_guard<std::mutex> guard(*bpm_latch_);
  
  // 分配新页面ID
  page_id_t new_page_id = next_page_id_++;
  
  // 将新页面写入磁盘（创建空页面）
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  char empty_data[BUSTUB_PAGE_SIZE] = {0};
  DiskRequest request{
    .is_write_ = true,
    .data_ = empty_data,
    .page_id_ = new_page_id,
    .callback_ = std::move(promise)
  };
  disk_scheduler_->Schedule(std::move(request));
  future.get();
  
  return new_page_id;
}
/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
     if (page_id == INVALID_PAGE_ID){
       return true;
     }
     std::lock_guard<std::mutex> guard(*bpm_latch_);
     auto it = page_table_.find(page_id);
     frame_id_t frame_id = it->second;
     auto frame = frames_[frame_id];
     if (frame->pin_count_>0){
        return false;
     }
     if (frame->is_dirty_){
         DiskRequest req;
         req.is_write_ = true;
         req.data_ = frame->GetDataMut();
         req.page_id_ = page_id;
         req.callback_ = disk_scheduler_->CreatePromise();
         auto future = req.callback_.get_future();
         disk_scheduler_->Schedule(std::move(req));
         frame->is_dirty_=false;
     }
     frame->Reset();
     page_table_.erase(page_id);
     replacer_->Remove(frame_id);
     free_list_.push_back(frame_id);
     return true;
  // UNIMPLEMENTED("TODO(P1): Add implementation."); }

}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  frame_id_t frame_id;
  FrameHeader *frame = nullptr;
  
  {
    std::lock_guard<std::mutex> guard(*bpm_latch_);
    auto it = page_table_.find(page_id);
    
    if (it != page_table_.end()) {
      // 页面已在内存中
      frame_id = it->second;
    } else {
      // 页面不在内存中，需要分配帧并加载页面
      if (!free_list_.empty()) {
        // 有空闲帧
        frame_id = free_list_.back();
        free_list_.pop_back();
      } else {
        // 没有空闲帧，需要驱逐页面
        if (!replacer_->Evict(&frame_id)) {
          // 所有页面都被pin住，无法驱逐
          return std::nullopt;
        }
        
        // 处理被驱逐的页面
        auto evicted_frame = frames_[frame_id].get();
        page_id_t old_page_id = INVALID_PAGE_ID;
        for (auto &entry : page_table_) {
          if (entry.second == frame_id) {
            old_page_id = entry.first;
            break;
          }
        }
        
        // 如果被驱逐的页面是脏页，写回磁盘
        if (evicted_frame->is_dirty_) {
          auto promise = disk_scheduler_->CreatePromise();
          auto future = promise.get_future();
          DiskRequest request{
            .is_write_ = true,
            .data_ = const_cast<char*>(evicted_frame->GetData()),
            .page_id_ = old_page_id,
            .callback_ = std::move(promise)
          };
          disk_scheduler_->Schedule(std::move(request));
          future.get();
          evicted_frame->is_dirty_ = false;
        }
        page_table_.erase(old_page_id);
      }
      
      // 将新页面加载到帧中
      page_table_[page_id] = frame_id;
      auto new_frame = frames_[frame_id].get();
      new_frame->Reset();
      disk_manager_->ReadPage(page_id, new_frame->GetDataMut());
    }
    
    frame = frames_[frame_id].get();
    frame->pin_count_++;
    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);
  }
  
  // 在释放全局锁后获取页面锁，避免死锁
  frame->rwlatch_.lock();
  
  return WritePageGuard(page_id, frames_[frame_id], std::shared_ptr<LRUKReplacer>(replacer_.get(), [](LRUKReplacer*){}), bpm_latch_, disk_scheduler_);
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  frame_id_t frame_id;
  FrameHeader *frame = nullptr;
  
  {
    std::lock_guard<std::mutex> guard(*bpm_latch_);
    auto it = page_table_.find(page_id);
    
    if (it != page_table_.end()) {
      // 页面已在内存中
      frame_id = it->second;
    } else {
      // 页面不在内存中，需要分配帧并加载页面
      if (!free_list_.empty()) {
        // 有空闲帧
        frame_id = free_list_.back();
        free_list_.pop_back();
      } else {
        // 没有空闲帧，需要驱逐页面
        if (!replacer_->Evict(&frame_id)) {
          // 所有页面都被pin住，无法驱逐
          return std::nullopt;
        }
        
        // 处理被驱逐的页面
        auto evicted_frame = frames_[frame_id];
        page_id_t old_page_id = INVALID_PAGE_ID;
        for (auto &entry : page_table_) {
          if (entry.second == frame_id) {
            old_page_id = entry.first;
            break;
          }
        }
        
        // 如果被驱逐的页面是脏页，写回磁盘
        if (evicted_frame->is_dirty_) {
          auto promise = disk_scheduler_->CreatePromise();
          auto future = promise.get_future();
          DiskRequest request{
            .is_write_ = true,
            .data_ = const_cast<char*>(evicted_frame->GetData()),
            .page_id_ = old_page_id,
            .callback_ = std::move(promise)
          };
          disk_scheduler_->Schedule(std::move(request));
          future.get();
          evicted_frame->is_dirty_ = false;
        }
        page_table_.erase(old_page_id);
      }
      
      // 将新页面加载到帧中
      page_table_[page_id] = frame_id;
      auto new_frame = frames_[frame_id];
      new_frame->Reset();
      disk_manager_->ReadPage(page_id, new_frame->GetDataMut());
    }
    
    frame = frames_[frame_id].get();
    frame->pin_count_++;
    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);
  }
  
  // 在释放全局锁后获取页面锁，避免死锁
  frame->rwlatch_.lock_shared();
  
  return ReadPageGuard(page_id, frames_[frame_id], std::shared_ptr<LRUKReplacer>(replacer_.get(), [](LRUKReplacer*){}), bpm_latch_, disk_scheduler_);
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);
  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }
  
  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);
  
  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  
  std::lock_guard<std::mutex> guard(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
      return false;
  }
  
  frame_id_t frame_id = it->second;
  auto frame = frames_[frame_id];
  
  if (is_dirty) {
    frame->is_dirty_ = true;
  }
  
  replacer_->RecordAccess(frame_id, access_type);
  
  if (frame->pin_count_ > 0) {
    frame->pin_count_--;
    
    if (frame->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
  
  return false;
}

auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool { 
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  
  frame_id_t frame_id = it->second;
  auto frame = frames_[frame_id];
  
  // 将页面写回磁盘
  disk_manager_->WritePage(page_id, frame->GetData());
  
  // 重置脏标志
  frame->is_dirty_ = false;
  
  return true;
  // UNIMPLEMENTED("TODO(P1): Add implementation."); 
}


/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  // UNIMPLEMENTED("TODO(P1): Add implementation."); 
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  
  std::lock_guard<std::mutex> guard(*bpm_latch_);
  
  // 检查页面是否在缓冲池中
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  
  frame_id_t frame_id = it->second;
  auto frame = frames_[frame_id];
  
  // 将页面写回磁盘
  std::promise<bool> promise;
  auto future = promise.get_future();
  DiskRequest request{
    .is_write_ = true,
    .data_ = const_cast<char*>(frame->GetData()),
    .page_id_ = page_id,
    .callback_ = std::move(promise)
  };
  disk_scheduler_->Schedule(std::move(request));
  future.get();
  
  // 重置脏标志
  frame->is_dirty_ = false;
  
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() { 
  // UNIMPLEMENTED("TODO(P1): Add implementation."); 
  for (const auto &entry : page_table_) {
    page_id_t page_id = entry.first;
    frame_id_t frame_id = entry.second;
    auto frame = frames_[frame_id];
    
    if (frame->is_dirty_) {
      disk_manager_->WritePage(page_id, frame->GetData());
      frame->is_dirty_ = false;
    }
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> guard(*bpm_latch_);
  
  for (const auto &entry : page_table_) {
    page_id_t page_id = entry.first;
    frame_id_t frame_id = entry.second;
    auto frame = frames_[frame_id];
    
    if (frame->is_dirty_) {
      DiskRequest request;
      request.is_write_ = true;
      request.data_ = const_cast<char*>(frame->GetData());
      request.page_id_ = page_id;
      // 创建promise并获取future
      std::promise<bool> promise;
      auto future = promise.get_future();
      request.callback_ = std::move(promise);
      
      disk_scheduler_->Schedule(std::move(request));
      // 等待写入完成
      future.get();
      frame->is_dirty_ = false;
    }
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::lock_guard<std::mutex> guard(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  frame_id_t frame_id = it->second;
  auto frame = frames_[frame_id];
  
  return frame->pin_count_;

  // UNIMPLEMENTED("TODO(P1): Add implementation.");

}


}  // namespace bustub
