//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include<iostream>
namespace bustub {



ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  //各缓冲池在堆区进行分配
  for (size_t i = 0; i < num_instances; i++)  
  {
    BufferPoolManager *tmp = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager); //指针指向子类对象
    instances_.push_back(tmp);
  }
}




// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {

  for (size_t i = 0; i < num_instances_; i++) 
  {
    delete (instances_[i]);
  }
}




size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;     //返回全部缓冲池的容量，即独立缓冲池个数乘以缓冲池容量。返回的是总容量！！
  
}



//让BufferPoolManager负责处理给定的页面id。您可以在其他方法中使用此方法。
BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  return instances_[page_id % num_instances_];      //instacnes_数组里用于存储多个独立的缓冲池 , 通过对页面ID取余的方式将页面ID映射至对应的缓冲池。
}



//从负责的BufferPoolManagerInstance中获取page_id的页面
Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
    
  BufferPoolManager *instace = GetBufferPoolManager(page_id);
  return instace->FetchPage(page_id);
    
    
}



//从负责的BufferPoolManagerInstance取消pin page_id
bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *instace = GetBufferPoolManager(page_id);
  return instace->UnpinPage(page_id , is_dirty);
  
}




bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {

  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->FlushPage(page_id);
}



/*
当第一次实例化 ParallelBufferPoolManager 时，它的起始索引应该为0。每次创建一个新页面时，您将尝试每个 BufferPoolManagerInstance，
从起始索引开始，直到一个成功。然后将起始指数增加一。
*/
Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying BufferPoolManagerInstances
  //创建新页面。我们将以轮询方式从底层BufferPoolManagerInstances请求页面分配

  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called

  
  //1.从start_idx_开始遍历各独立缓冲池，如存在调用NewPage成功的页面，则返回该页面并将start_idx指向该页面的下一个页面；
  
  Page * res = nullptr;
  for(size_t i = 0 ; i< num_instances_ ; i++)
  {
    size_t idx = (start_idx_ + i) % num_instances_;
    Page *cur =  instances_[idx]->NewPage(page_id); 
    if(cur != nullptr)
    {
      start_idx_  = (*page_id + 1)% num_instances_;
      return cur;
    }
  }

  // 2.如全部缓冲池调用NewPage均失败，则返回空指针，并递增start_idx。
  start_idx_ ++;
  return nullptr;
}





bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *instance = GetBufferPoolManager(page_id);
  return instance->DeletePage(page_id);
  
}





void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for(size_t i = 0 ; i<num_instances_ ; i++)      //遍历每一个独立的缓冲池
  {
    instances_[i]->FlushAllPages();
  }
}

}  // namespace bustub


/*
上述函数仅需调用对应独立缓冲池的方法即可。值得注意的是，由于在缓冲池中存放的为缓冲池实现类的基类指针，
因此所调用函数的应为缓冲池实现类的基类对应的虚函数。并且，由于ParallelBufferPoolManager和BufferPoolManagerInstance为兄弟关系，
因此ParallelBufferPoolManager不能直接调用BufferPoolManagerInstance对应的Imp函数，因此直接在ParallelBufferPoolManager中存放
BufferPoolManagerInstance指针也是不可行的。
*/