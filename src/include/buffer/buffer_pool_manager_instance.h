//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */

class BufferPoolManagerInstance : public BufferPoolManager {
 public:


  /**
   * Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager = nullptr);
  
  
  
  
  /**
   * Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param num_instances total number of BPIs in parallel BPM
   * @param instance_index index of this BPI in the parallel BPM
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,DiskManager *disk_manager, LogManager *log_manager = nullptr);

  
  /**
   * Destroys an existing BufferPoolManagerInstance.
   */
  ~BufferPoolManagerInstance() override;



  /** @return size of the buffer pool */
  size_t GetPoolSize() override { return pool_size_; }




  /** @return pointer to all the pages in the buffer pool */
  Page *GetPages() { return pages_; }


 protected:

/*
  *
  * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
  * but all frames are currently in use and not evictable (in another word, pinned).
  *    从缓冲池获取请求的页面。如果需要从磁盘获取page_id，则返回nullptr但所有帧目前都在使用中，且不可收回(换句话说，固定)。
  * 
  * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
  * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
  * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
  * to disk and update the metadata of the new page
  * 
  * 首先在缓冲池中搜索page_id。如果没有找到，从空闲列表或替换器中选择一个替换帧(总是首先从空闲列表中查找)，通过调用
  * disk_manager_->ReadPage()从磁盘中读取页面，并替换帧中的旧页面。与NewPgImp()类似，如果旧页面是脏的，则需要将其写回磁盘，
  * 并更新新页面的元数据
  *
  * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
  *  此外，记得禁用驱逐并记录帧的访问历史，就像您对NewPgImp()所做的那样。
  * 
  * @param page_id id of page to be fetched
  * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
  */
  Page *FetchPgImp(page_id_t page_id) override;





  /*
   * TODO(P1): Add implementation
   *
   * @brief 从缓冲池解除目标页的固定。如果page_id不在缓冲池中，或者它的引脚计数已经为0，则返回false。
   * 
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   */
  bool UnpinPgImp(page_id_t page_id, bool is_dirty) override;



  /*
  *
  *
  * @brief Flush the target page to disk.    //将目标页刷入磁盘
  *
  * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag. 
  * 使用DiskManager::WritePage()方法将页面刷新到磁盘，而不考虑dirty标志。
  * 
  * Unset the dirty flag of the page after flushing.  刷盘后取消设置页面的脏标记。
  *
  * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
  * @return false if the page could not be found in the page table, true otherwise
  */
  bool FlushPgImp(page_id_t page_id) override;




  /*
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
    在缓冲池中创建一个新页面。将page_id设置为新页面的id，如果当前所有帧都在使用且不可收回(换句话说，固定)，则设置为nullptr。
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
  您应该从空闲列表或替换器中选择替换帧(总是从空闲列表中查找),然后调用AllocatePage()方法来获得一个新的页面id。如果替换帧有一个脏页面，
  你应该先把它写回磁盘。你还需要重置新页面的内存和元数据。

   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
  记得通过调用replacer来“Pin”帧。SetEvictable(frame_id, false)这样，在缓冲池管理器“Unpin”它之前，替换器不会驱逐帧。
  此外，记住在替换程序中记录帧的访问历史，以便lru-k算法工作。
  
   * @param[out] page_id id of created page   新创建页的page_id
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  Page *NewPgImp(page_id_t *page_id) override;






  /*
   * TODO(P1): Add implementation
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   page is pinned and cannot be deleted, return false immediately.  
   从缓冲池中删除一个页面。如果page_id不在缓冲池中，什么都不做并返回true。如果页面被钉住，无法删除，则立即返回false。

   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   imitate freeing the page on the disk.
   从页表中删除该页后，停止跟踪替换器中的帧，并将该帧添加回空闲列表。同时，重置页面的内存和元数据。最后，你应该调用DeallocatePage()来模拟释放磁盘上的页面。
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  bool DeletePgImp(page_id_t page_id) override;





  /**
   * Flushes all the pages in the buffer pool to disk.   挨个刷盘
   */
  void FlushAllPgsImp() override;



  /**
   * Allocate a page on disk.∂
   * @return the id of the allocated page
   */
  page_id_t AllocatePage();

  /**
   * Deallocate a page on disk.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }

  /**
   * Validate that the page_id being used is accessible to this BPI. This can be used in all of the functions to
   * validate input data and ensure that a parallel BPM is routing requests to the correct BPI
   * @param page_id
   */
  void ValidatePageId(page_id_t page_id) const;

  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** How many instances are in the parallel BPM (if present, otherwise just 1 BPI) */
  const uint32_t num_instances_ = 1;
  /** Index of this BPI in the parallel BPM (if present, otherwise just 0) */
  const uint32_t instance_index_ = 0;

  /** Each BPI maintains its own counter for page_ids to hand out, must ensure they mod back to its instance_index_ */
  std::atomic<page_id_t> next_page_id_ = instance_index_;

  /** Array of buffer pool pages. */
  Page *pages_;   //缓冲池中的实际容器页面槽位数组，用于存放从磁盘中读入的页面，并供DBMS访问

  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));   // 磁盘管理器，提供从磁盘读入页面及写入页面的接口；

  /** Pointer to the log manager. */
  LogManager *log_manager_ __attribute__((__unused__));

  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;    // 保存磁盘页面IDpage_id和槽位IDframe_id_t的映射； 

  /** Replacer to find unpinned pages for replacement. */
  Replacer *replacer_;       //用于选取所需驱逐的页面；

  /** List of free pages. */
  std::list<frame_id_t> free_list_;   // 保存缓冲池中的空闲槽位的frmae ID。
  
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;
};
}  // namespace bustub