//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <cstdio>
#include <iostream>
#include "common/macros.h"
namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,LogManager *log_manager): 
                                                    BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {
                                                      
                                                    }

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) 
  {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}



/*
将某一页刷盘, 无论是否dirty, 这是强制刷盘 ,函数用于显式地将缓冲池页面写回磁盘。首先，应当检查缓冲池中是否存在对应页面
ID的页面，如不存在则返回False；如存在对应页面，则将缓冲池内的该页面的is_dirty_置为false，并使用WritePage将该页面的实际数据data_写回磁盘。
*/
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {

  frame_id_t frame_id;
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) 
  {
    latch_.unlock();
    return false;
  }
  
  frame_id = it->second;
  pages_[frame_id].is_dirty_ = false;  //进行了刷盘就不是 dirty了。 
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);    // WritePage 刷盘函数，显式的进行刷盘

  latch_.unlock();
  return true;
}



/*
挨个刷盘
*/
void BufferPoolManagerInstance::FlushAllPgsImp() {
  latch_.lock();
  for(auto cur: page_table_)    // unordered_map<page_id_t, frame_id_t> page_table_
  {

    pages_[cur.second].is_dirty_ = false;
    disk_manager_->WritePage(cur.first , pages_[cur.second].data_);
  }
  latch_.unlock();
}


//在磁盘中分配新的物理页面，将其添加至缓冲池，并返回指向缓冲池页面Page的指针。
//不是通过磁盘刷进内存, 而是直接在缓存池中新建一页, 注意新页要第一时间写入到磁盘(尽管没什么内容), 以确保磁盘能感知到该页, 否则被LRU淘汰就完蛋了, 但如果内存全是被pin的页就没办法了, 返回空指针
Page* BufferPoolManagerInstance::NewPgImp(page_id_t *page_id){     
  //1 检查当前缓冲池中是否存在空闲槽位或存放页面可被驱逐的槽位（下文称其为目标槽位），在这里总是先通过检查free_list_以查询空闲槽位，
  //如无空闲槽位则尝试从replace_中驱逐页面并返回被驱逐页面的槽位。如目标槽位，则返回空指针；如存在目标槽位，则调用AllocatePage()为新的页面分配page_id页面ID。
  frame_id_t new_frame_id;
  latch_.lock();

  if( !free_list_.empty())
  {
    new_frame_id = free_list_.front();      //free_list 里存放的就是每一个槽(frame_id)对应的缓存池中的控制块位置
    free_list_.pop_front();
  }
  else      //如果不存在空闲页 需要用算法淘汰一页
  {
    bool res = replacer_->Victim(&new_frame_id);
    if(res == false)
    {
      latch_.unlock();
      return nullptr;
    }
  }
  * page_id = AllocatePage();

  //2 这里需要检查目标槽位中的页面是否为脏页面，如是则需将其写回磁盘，并将其脏位设为false；
  if(pages_[new_frame_id].is_dirty_)
  {
    page_id_t flush_page_id = pages_[new_frame_id].page_id_;
    pages_[new_frame_id].is_dirty_ = false;
    disk_manager_->WritePage(flush_page_id,  pages_[new_frame_id].data_);
  }
  
  page_table_.erase(pages_[new_frame_id].page_id_);
  page_table_[*page_id] = new_frame_id;
  pages_[new_frame_id].page_id_ = *page_id;
  pages_[new_frame_id].ResetMemory();
  pages_[new_frame_id].pin_count_ = 1;
  replacer_->Pin(new_frame_id);
  latch_.unlock();
  return &pages_[new_frame_id];

  //3 从page_table_中删除目标槽位中的原页面ID的映射，并将新的<页面ID - 槽位ID>映射插入，然后更新槽位中页面的元数据。
  // 需要注意的是，在这里由于我们返回了指向该页面的指针，我们需要将该页面的用户数pin_count_置为1，并调用replacer_的Pin。
}


//从内存中抓取一页,获取对应页面ID的页面，并返回指向该页面的指针
Page* BufferPoolManagerInstance::FetchPgImp(page_id_t page_id){ 
  
  frame_id_t frame_id;    
  latch_.lock();
  //1.通过检查page_table_以检查缓冲池中是否已经缓冲该传入参数page_id对应的页面，
  auto it = page_table_.find(page_id);

  //2. 如果Pages存在，固定它并立即返回它。  key是page , value是frame
  if( it!= page_table_.end())              
  {
    frame_id = it->second;      
    Page *res = &pages_[frame_id];    //  pages_ 是缓冲池中的实际容器页面槽位数组，用于存放从磁盘中读入的页面，数组里存放的就是一个一个的pages
    res->pin_count_++;
    replacer_->Pin(frame_id);     

    latch_.unlock();
    return res;
  }

  //3 页面总是首先从空闲列表中找到 如缓冲池中尚未缓冲该页面，则需寻找当前缓冲池中是否存在空闲槽位或存放页面可被驱逐的槽位（下文称其为目标槽位），
  if( ! free_list_.empty())
  {
    frame_id = free_list_.front();
    free_list_.pop_front();     //找到替换页了就弹出空闲链表的这一个记录控制块
    page_table_[page_id]  = frame_id;  // 更新一下此 page在hash表中的 frame_id 以至于在pool中容易找到这个frame_id对应到的控制块
    Page *res = &pages_[frame_id];  
    
    disk_manager_->ReadPage(page_id , res->data_);    // ReadPage() - 将指定页面的内容读入给定的内存区域 
    res->pin_count_ = 1;
    res->page_id_ = page_id;
    replacer_->Pin(page_id);    

    latch_.unlock();
    return res;
  }
  else      //如果不存在空余页,则调用replace管理类中的LUR 去除一页
  {
    bool ret = replacer_->Victim(&frame_id);
    if(ret == false)
    {
      latch_.unlock();
      return nullptr;
    }

    if(pages_[frame_id].IsDirty())    //4 如果替代页是脏的，则将其写回磁盘。  (脏页：数据改了，但是没有存。即在内存里的样子和磁盘里的样子是不一样的。) 
    {
      page_id_t flush_page_id = pages_[frame_id].page_id_;    // 先拿到缓存池中脏页的 page_id,因为缓存池中保存的只有每个page对应的frame_id,
      disk_manager_->WritePage(flush_page_id , pages_[frame_id].data_);  //  将指定页的内容写入磁盘文件，即完成缓存池与磁盘页的同步
    }
    page_table_.erase(pages_[frame_id].page_id_);   // 在page_table中删除该frame对应的页
  }


  page_table_[page_id] = frame_id;
  pages_[frame_id].is_dirty_ = false; 
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  latch_.unlock();
  return &pages_[frame_id];

}




/*
功能为从缓冲池中删除对应页面ID的页面，但是如果pin_count非0(非0代表现在有线程在引用他), 
就不能删除,，删除成功就将其插入空闲链表free_list_，
*/
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  
  DeallocatePage(page_id);
  latch_.lock();

  //1 首先，检查该页面是否存在于缓冲区，如未存在则返回True。然后，检查该页面的用户数pin_count_是否为0，如非0则返回False。
  //在这里，不难看出DeletePgImp的返回值代表的是该页面是否被用户使用，因此在该页面不在缓冲区时也返回True；
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) 
  {
    latch_.unlock();
    return true;
  }

  frame_id_t frame_id = it->second;   // 拿到哈希表中page_id对应的实值 frame_id
  if (pages_[frame_id].pin_count_ != 0)       //如果有用户正在引用则不能在缓存池中删除该页
  {
    latch_.unlock();
    return false;
  }

  //2 检查该页面是否为脏，如果是脏页则先写回磁盘,然后在进行删除
  if (pages_[frame_id].is_dirty_ == true) 
  {
    page_id_t flush_page_id = pages_[frame_id].page_id_;
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->WritePage(flush_page_id, pages_[frame_id].data_);
  }
  //没有上述情况,在page_table_中删除该页面的映射，并将该槽位中页面的page_id 置为INVALID_PAGE_ID。最后，将槽位ID插入空闲链表即可
  page_table_.erase(it);    
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);   //将此 frame_id映射的控制块插入free_list中
  latch_.unlock();
  return true;
}





//功能为提供用户向缓冲池通知页面使用完毕的接口，用户需声明使用完毕页面的页面ID以及使用过程中是否对该页面进行修改。
//一个线程不再引用一个页了, 将其pin_count减少1, 并通知manager是否该页已经脏了, 有任何异常情况(例如pin_count已经为0了)就返回false, 
//记得pin_count变为0时调用LRU的unpin （没有线程引用此页了就插入LRU链表中等待淘汰）
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty){ 
  latch_.lock();
  frame_id_t frame_id;
  auto it = page_table_.find(page_id);

  if(it == page_table_.end())
  {
    latch_.unlock();
    return false;
  }

  frame_id = page_table_[page_id];        //先拿到对应控制块的frame_id
  if(pages_[frame_id].pin_count_ <=0)
  {
    return false;
  }      

  if(is_dirty == true)
  {
    pages_[frame_id].is_dirty_ = true;
  }

  pages_[frame_id].pin_count_--;
  if(pages_[frame_id].pin_count_ == 0)
  {
    replacer_->Unpin(frame_id);   // 该页存入LRU等待淘汰
  }
  return true;
  
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub