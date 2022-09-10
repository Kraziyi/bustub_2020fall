//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  const std::unique_lock<std::mutex> guard(latch_);
  auto table_iteator = page_table_.find(page_id);
  frame_id_t frame_id;

  if(table_iteator != page_table_.end()) {
    frame_id = table_iteator->second;
    Page *cur_page = &pages_[frame_id];
    ++cur_page->pin_count_;
    replacer_->Pin(frame_id);
    return cur_page;
  }

  if(!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else {
    bool victim_flag = replacer_->Victim(&frame_id);
    if(!victim_flag) {return nullptr;}
  }

  Page *replaced_page = &pages_[frame_id];
  if(replaced_page->IsDirty()) {
    disk_manager_->WritePage(replaced_page->GetPageId(), replaced_page->GetData());
    replaced_page->is_dirty_ = false;
  }
  page_table_.erase(replaced_page->GetPageId());

  page_table_.emplace(page_id, frame_id);
  // P using the place of R in *pages_
  replaced_page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, replaced_page->GetData());
  replacer_->Pin(frame_id);
  ++replaced_page->pin_count_;
  return replaced_page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  const std::unique_lock<std::mutex> guard(latch_);
  auto table_iterator = page_table_.find(page_id);
  if(table_iterator == page_table_.end()) {return true;}
  frame_id_t frame_id = table_iterator->second;
  Page *unpin_page = &pages_[frame_id];

  if(is_dirty) {
    unpin_page->is_dirty_ = true;
  }

  if(unpin_page->pin_count_ <= 0) {return false;}
  --unpin_page->pin_count_;
  
  if(unpin_page->pin_count_ == 0) {replacer_->Unpin(frame_id);}
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  const std::unique_lock<std::mutex> guard(latch_);
  auto table_iterator = page_table_.find(page_id);
  if(table_iterator == page_table_.end()) {return false;}
  frame_id_t frame_id = table_iterator->second;
  Page *flused_page = &pages_[frame_id];
  
  disk_manager_->WritePage(page_id, flused_page->GetData());
  flused_page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  const std::unique_lock<std::mutex> guard(latch_);
  bool all_pinned = true;
  for(int i = 0; i < pool_size_; ++i) {
    if(pages_[i].GetPinCount() <= 0) {
      all_pinned = false;
      break;
    }
  }
  if(!all_pinned) return nullptr;
  
  frame_id_t frame_id;
  if(!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else {
    if(!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
  }

  Page *replaced_page = &pages_[frame_id];
  if(replaced_page->IsDirty()) {
    disk_manager_->WritePage(replaced_page->GetPageId(), replaced_page->GetData());
    replaced_page->is_dirty_ = false;
  }
  page_table_.erase(replaced_page->GetPageId());

  page_id_t new_page_id = disk_manager_->AllocatePage();
  replaced_page->page_id_ = new_page_id;
  replaced_page->pin_count_++;
  replaced_page->ResetMemory();
  replacer_->Pin(frame_id);
  page_table_.emplace(new_page_id, frame_id);

  *page_id = new_page_id;
  
  return replaced_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  const std::unique_lock<std::mutex> guard(latch_);
  auto table_iterator = page_table_.find(page_id);
  if(table_iterator == page_table_.end()) {return true;}
  frame_id_t frame_id = table_iterator->second;
  Page *delet_page = &pages_[frame_id];
  
  if(delet_page->GetPinCount() != 0) {return false;}
    if(delet_page->IsDirty()) {
    disk_manager_->WritePage(delet_page->GetPageId(), delet_page->GetData());
    delet_page->is_dirty_ = false;
  }
  page_table_.erase(table_iterator);
  disk_manager_->DeallocatePage(page_id);
  delet_page->ResetMemory();
  delet_page->page_id_ = INVALID_PAGE_ID;
  delet_page->is_dirty_ = false;
  delet_page->pin_count_ = 0;
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  const std::unique_lock<std::mutex> guard(latch_);
  for(int i = 0; i < pool_size_; ++i) {
    Page* curr_page = &pages_[i];
    if(curr_page->GetPageId() != INVALID_PAGE_ID && curr_page->IsDirty()) {
      disk_manager_->WritePage(curr_page->GetPageId(), curr_page->GetData());
      curr_page->is_dirty_ = false;
    }
  }
}

}  // namespace bustub
