//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // dir_.resize(num_buckets_, std::make_shared<Bucket>(bucket_size_));
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  // LOG_INFO("The %d idx depth is %d \n", (int)dir_index, (int)dir_[dir_index]->GetDepth());
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  auto bucket = dir_[idx];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  auto bucket = dir_[idx];
  return bucket->Remove(key);
}

// template<typename K, typename V>
// auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {

// }

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  size_t idx = IndexOf(key);
  auto bucket = dir_[idx];
  if (bucket->Insert(key, value)) {
    return;
  }

  // local depth is equal to global depth
  if (GetGlobalDepthInternal() == GetLocalDepthInternal(idx)) {
    global_depth_++;
    size_t bucket_num = dir_.size();
    for (size_t i = 0; i < bucket_num; ++i) {
      dir_.push_back(dir_[i]);
    }
  }

  int new_depth = bucket->GetDepth() + 1;
  size_t base_mask = (1 << bucket->GetDepth()) - 1;
  size_t split_mask = (1 << new_depth) - 1;

  auto new_bucket_1 = std::make_shared<Bucket>(bucket_size_, new_depth);
  auto new_bucket_2 = std::make_shared<Bucket>(bucket_size_, new_depth);

  size_t low_index = idx & base_mask;
  for (const auto &ele : bucket->GetItems()) {
    size_t split_index = IndexOf(ele.first);
    if ((split_index & split_mask) == low_index) {
      new_bucket_1->GetItems().emplace_back(ele);
      continue;
    }
    new_bucket_2->GetItems().emplace_back(ele);
  }

  for (size_t i = 0; i < dir_.size(); i++) {
    if ((i & base_mask) == low_index) {
      if ((i & split_mask) == low_index) {
        dir_[i] = new_bucket_1;
        continue;
      }
      dir_[i] = new_bucket_2;
    }
  }
  num_buckets_++;
  return InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  return InsertInternal(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &node : list_) {
    if (node.first == key) {
      value = node.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end();) {
    if (key == (*it).first) {
      // LOG_INFO("The key %d has been removed \n", (int)key);
      list_.erase(it++);
      return true;
    }
    ++it;
  }
  // LOG_INFO("The key %d remove error \n", (int)key);
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &node : list_) {
    if (node.first == key) {
      // LOG_INFO("The value: %s has update %s \n", (std::string)node.second, (std::string)node.first);
      // LOG_INFO("The key: %d has been update \n", (int)key);
      node.second = value;
      return true;
    }
  }
  if (IsFull()) {
    // LOG_INFO("The key %d bucket is full \n", (int)key);
    return false;
  }
  // LOG_INFO("The list append the key %d \n", (int)key);
  // LOG_INFO("The list append %s \n", value.c_str());
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
