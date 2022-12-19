//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {


/*
并行缓冲池的思想是分配多个独立的缓冲池，并将不同的页面ID映射至各自的缓冲池中，从而减少整体缓冲池的锁粒度，以增加并行性。
为了避免频繁争抢锁, 一个不错的idea是做多个BPM(Buffer Pool Manager), 根据page_id决定由哪个manager来管理, 这样抢锁的程度会低很多
*/

class ParallelBufferPoolManager : public BufferPoolManager {
 public:

  /**
   * Creates a new ParallelBufferPoolManager.
   * @param the number of individual BufferPoolManagerInstances to store
   * @param pool_size the pool size of each BufferPoolManagerInstance
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,LogManager *log_manager = nullptr);


  /**
   * Destroys an existing ParallelBufferPoolManager.
   */
  ~ParallelBufferPoolManager() override;



  /** @return size of the buffer pool */
  size_t GetPoolSize() override;

 protected:



  /**
   * @param page_id id of page
   * @return pointer to the BufferPoolManager responsible for handling given page id
   */
  BufferPoolManager *GetBufferPoolManager(page_id_t page_id);



  /**
   * Fetch the requested page from the buffer pool. 从缓冲池获取请求的页面。
   * @param page_id id of page to be fetched
   * @return the requested page
   */
  Page *FetchPgImp(page_id_t page_id) override;



  /**
   * Unpin the target page from the buffer pool.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page pin count is <= 0 before this call, true otherwise
   */
  bool UnpinPgImp(page_id_t page_id, bool is_dirty) override;



  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  bool FlushPgImp(page_id_t page_id) override;



  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  Page *NewPgImp(page_id_t *page_id) override;



  /**
   * Deletes a page from the buffer pool.
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  bool DeletePgImp(page_id_t page_id) override;




  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  void FlushAllPgsImp() override;


 private:
  std::vector<BufferPoolManager *> instances_;    //用于存储多个独立的缓冲池
  size_t pool_size_;    //记录各缓冲池的容量
  size_t num_instances_;    //独立缓冲池的个数
  size_t start_idx_ = 0;
};
}  // namespace bustub
