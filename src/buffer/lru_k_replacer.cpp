//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (curr_size_ <= 0) {
    LOG_INFO("The evictable size is zero \n");
    frame_id = nullptr;
    return false;
  }
  if (!hist_list_.empty()) {
    for (auto rit = hist_list_.rbegin(); rit != hist_list_.rend(); ++rit) {
      auto id = *rit;
      auto frame_entity = frame_entities_.find(id);
      if (frame_entity == frame_entities_.end()) {
        LOG_INFO("can't find the frame_id: %d \n", id);
        frame_id = nullptr;
        return false;
      }
      if (frame_entity->second.evictable_) {
        hist_list_.erase(frame_entity->second.it_);
        frame_entities_.erase(id);
        curr_size_--;
        *frame_id = id;
        return true;
      }
    }
  }
  if (!cache_list_.empty()) {
    for (auto rit = cache_list_.rbegin(); rit != cache_list_.rend(); ++rit) {
      auto id = *rit;
      auto frame_entity = frame_entities_.find(id);
      if (frame_entity == frame_entities_.end()) {
        LOG_INFO("can't find the frame_id: %d \n", id);
        frame_id = nullptr;
        return false;
      }
      if (frame_entity->second.evictable_) {
        cache_list_.erase(frame_entity->second.it_);
        frame_entities_.erase(id);
        curr_size_--;
        *frame_id = id;
        return true;
      }
    }
  }
  frame_id = nullptr;
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    LOG_INFO("The frame_id: %d is bigger than replacer_size: %zu \n", frame_id, replacer_size_);
    return;
  }
  auto entity = frame_entities_.find(frame_id);
  if (entity == frame_entities_.end()) {
    LOG_INFO("add the new frame_entity %d \n", frame_id);
    hist_list_.emplace_front(frame_id);
    frame_entities_.insert(std::make_pair(frame_id, FrameEntity{0, true, hist_list_.begin()}));
    curr_size_++;
    entity = frame_entities_.find(frame_id);
  }
  entity->second.hit_count_++;
  if (entity->second.hit_count_ == k_) {
    hist_list_.erase(entity->second.it_);
    cache_list_.emplace_front(frame_id);
    entity->second.it_ = cache_list_.begin();
  } else if (entity->second.hit_count_ > k_) {
    cache_list_.erase(entity->second.it_);
    cache_list_.emplace_front(frame_id);
    entity->second.it_ = cache_list_.begin();
  }
  LOG_INFO("The frame_entity: %d hit_count add one \n", frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    LOG_INFO("The frame_id: %d is bigger than replacer_size: %zu \n", frame_id, replacer_size_);
    return;
  }
  auto entity = frame_entities_.find(frame_id);
  if (entity == frame_entities_.end()) {
    LOG_INFO("can't find the frame_id: %d \n", frame_id);
    return;
  }
  if (entity->second.evictable_ && !set_evictable) {
    LOG_INFO("The frame_id: %d is set non-evictable \n", frame_id);
    curr_size_--;
  } else if (!entity->second.evictable_ && set_evictable) {
    LOG_INFO("The frame_id: %d is set evictable \n", frame_id);
    curr_size_++;
  }
  entity->second.evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    LOG_INFO("The frame_id: %d is bigger than replacer_size: %zu \n", frame_id, replacer_size_);
    return;
  }
  auto frame_entity = frame_entities_.find(frame_id);
  if (frame_entity == frame_entities_.end()) {
    LOG_INFO("can't find the frame_id: %d \n", frame_id);
    return;
  }
  if (!frame_entity->second.evictable_) {
    LOG_INFO("The frame_id: %d is non-evictable \n", frame_id);
    return;
  }
  if (frame_entity->second.hit_count_ < k_) {
    hist_list_.erase(frame_entity->second.it_);
  } else if (frame_entity->second.hit_count_ >= k_) {
    cache_list_.erase(frame_entity->second.it_);
  }
  curr_size_--;
  frame_entities_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
