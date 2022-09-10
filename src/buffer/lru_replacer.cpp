//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    std::lock_guard<std::mutex> lock(replacer_mutex);

    if(used_list.empty()) return false;
    auto victim = used_list.front();
    used_list.pop_front();

    auto map_victim = map.find(victim);
    if(map_victim == map.end()) return false;
    map.erase(map_victim);

    *frame_id = victim;
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(replacer_mutex);
    auto pinned_map = map.find(frame_id);
    if(pinned_map == map.end()) return;
    used_list.erase(pinned_map->second);
    map.erase(pinned_map);
    pinned.insert(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(replacer_mutex);
    auto unpin = pinned.find(frame_id);
    if(unpin == pinned.end()) return;
    pinned.erase(unpin);
    used_list.emplace_back(frame_id);
    map.emplace(frame_id, used_list.rbegin());
}

size_t LRUReplacer::Size() {
    std::lock_guard<std::mutex> lock(replacer_mutex);
    return map.size() + pinned.size();
}
}  // namespace bustub
