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
#include "common/exception.h"
#include <algorithm>
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_++;
  if (node_store_.empty()) {
    return false;
  }
  std::vector<frame_id_t> evictable;
  for(auto& pair: node_store_) {
    if (pair.second.IsEvictable()) {
      evictable.push_back(pair.first);
    }
  }
  std::vector<frame_id_t> less_than_k;
  for(auto id: evictable) {
    if(node_store_.at(id).LessThank()) {
      less_than_k.push_back(id);
    }
  }

  if(!less_than_k.empty()) {
    auto id = *std::min_element(less_than_k.begin(), less_than_k.end(), [&](const auto a, const auto b) {
      return node_store_[a].Earliest() < node_store_[b].Earliest();
              });
    node_store_.erase(id);
    curr_size_--;
    *frame_id = id;
    return true;
  } else {
    auto id = *std::min_element(evictable.begin(), evictable.end(), [&](const auto a, const auto b) {
      return node_store_[a].KDistance() > node_store_[b].KDistance();
    });

    node_store_.erase(id);
    curr_size_--;
    *frame_id = id;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "frame id is bigger than replacer size");

  if (node_store_.find(frame_id) == node_store_.end() && node_store_.size() < replacer_size_) {
    auto node = LRUKNode();
    node.SetK(k_);
    node.setFrameId(frame_id);
    node_store_.insert(std::pair<frame_id_t, LRUKNode>(frame_id, node));
  }
  node_store_[frame_id].Record(current_timestamp_++);

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (node_store_.find(frame_id) == node_store_.end()) {
    throw std::invalid_argument("frame id is not valid");
  }
  auto& node = node_store_[frame_id];
  if (node.IsEvictable()) {
    if (!set_evictable) {
      node.setEvictable(set_evictable);
      curr_size_ -= 1;
    }
  } else {
    if (set_evictable) {
      node.setEvictable(set_evictable);
      curr_size_ += 1;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  if (!node_store_[frame_id].IsEvictable()) {
    throw std::invalid_argument("frame id not evictable");
  }
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
