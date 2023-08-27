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
��ĳһҳˢ��, �����Ƿ�dirty, ����ǿ��ˢ�� ,����������ʽ�ؽ������ҳ��д�ش��̡����ȣ�Ӧ����黺������Ƿ���ڶ�Ӧҳ��
ID��ҳ�棬�粻�����򷵻�False������ڶ�Ӧҳ�棬�򽫻�����ڵĸ�ҳ���is_dirty_��Ϊfalse����ʹ��WritePage����ҳ���ʵ������data_д�ش��̡�
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
  pages_[frame_id].is_dirty_ = false;  //������ˢ�̾Ͳ��� dirty�ˡ� 
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);    // WritePage ˢ�̺�������ʽ�Ľ���ˢ��

  latch_.unlock();
  return true;
}



/*
����ˢ��
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


//�ڴ����з����µ�����ҳ�棬�������������أ�������ָ�򻺳��ҳ��Page��ָ�롣
//����ͨ������ˢ���ڴ�, ����ֱ���ڻ�������½�һҳ, ע����ҳҪ��һʱ��д�뵽����(����ûʲô����), ��ȷ�������ܸ�֪����ҳ, ����LRU��̭���군��, ������ڴ�ȫ�Ǳ�pin��ҳ��û�취��, ���ؿ�ָ��
Page* BufferPoolManagerInstance::NewPgImp(page_id_t *page_id){     
  //1 ��鵱ǰ��������Ƿ���ڿ��в�λ����ҳ��ɱ�����Ĳ�λ�����ĳ���ΪĿ���λ����������������ͨ�����free_list_�Բ�ѯ���в�λ��
  //���޿��в�λ���Դ�replace_������ҳ�沢���ر�����ҳ��Ĳ�λ����Ŀ���λ���򷵻ؿ�ָ�룻�����Ŀ���λ�������AllocatePage()Ϊ�µ�ҳ�����page_idҳ��ID��
  frame_id_t new_frame_id;
  latch_.lock();

  if( !free_list_.empty())
  {
    new_frame_id = free_list_.front();      //free_list ���ŵľ���ÿһ����(frame_id)��Ӧ�Ļ�����еĿ��ƿ�λ��
    free_list_.pop_front();
  }
  else      //��������ڿ���ҳ ��Ҫ���㷨��̭һҳ
  {
    bool res = replacer_->Victim(&new_frame_id);
    if(res == false)
    {
      latch_.unlock();
      return nullptr;
    }
  }
  * page_id = AllocatePage();

  //2 ������Ҫ���Ŀ���λ�е�ҳ���Ƿ�Ϊ��ҳ�棬�������轫��д�ش��̣���������λ��Ϊfalse��
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

  //3 ��page_table_��ɾ��Ŀ���λ�е�ԭҳ��ID��ӳ�䣬�����µ�<ҳ��ID - ��λID>ӳ����룬Ȼ����²�λ��ҳ���Ԫ���ݡ�
  // ��Ҫע����ǣ��������������Ƿ�����ָ���ҳ���ָ�룬������Ҫ����ҳ����û���pin_count_��Ϊ1��������replacer_��Pin��
}


//���ڴ���ץȡһҳ,��ȡ��Ӧҳ��ID��ҳ�棬������ָ���ҳ���ָ��
Page* BufferPoolManagerInstance::FetchPgImp(page_id_t page_id){ 
  
  frame_id_t frame_id;    
  latch_.lock();
  //1.ͨ�����page_table_�Լ�黺������Ƿ��Ѿ�����ô������page_id��Ӧ��ҳ�棬
  auto it = page_table_.find(page_id);

  //2. ���Pages���ڣ��̶�����������������  key��page , value��frame
  if( it!= page_table_.end())              
  {
    frame_id = it->second;      
    Page *res = &pages_[frame_id];    //  pages_ �ǻ�����е�ʵ������ҳ���λ���飬���ڴ�ŴӴ����ж����ҳ�棬�������ŵľ���һ��һ����pages
    res->pin_count_++;
    replacer_->Pin(frame_id);     

    latch_.unlock();
    return res;
  }

  //3 ҳ���������ȴӿ����б����ҵ� �绺�������δ�����ҳ�棬����Ѱ�ҵ�ǰ��������Ƿ���ڿ��в�λ����ҳ��ɱ�����Ĳ�λ�����ĳ���ΪĿ���λ����
  if( ! free_list_.empty())
  {
    frame_id = free_list_.front();
    free_list_.pop_front();     //�ҵ��滻ҳ�˾͵��������������һ����¼���ƿ�
    page_table_[page_id]  = frame_id;  // ����һ�´� page��hash���е� frame_id ��������pool�������ҵ����frame_id��Ӧ���Ŀ��ƿ�
    Page *res = &pages_[frame_id];  
    
    disk_manager_->ReadPage(page_id , res->data_);    // ReadPage() - ��ָ��ҳ������ݶ���������ڴ����� 
    res->pin_count_ = 1;
    res->page_id_ = page_id;
    replacer_->Pin(page_id);    

    latch_.unlock();
    return res;
  }
  else      //��������ڿ���ҳ,�����replace�������е�LUR ȥ��һҳ
  {
    bool ret = replacer_->Victim(&frame_id);
    if(ret == false)
    {
      latch_.unlock();
      return nullptr;
    }

    if(pages_[frame_id].IsDirty())    //4 ������ҳ����ģ�����д�ش��̡�  (��ҳ�����ݸ��ˣ�����û�д档�����ڴ�������Ӻʹ�����������ǲ�һ���ġ�) 
    {
      page_id_t flush_page_id = pages_[frame_id].page_id_;    // ���õ����������ҳ�� page_id,��Ϊ������б����ֻ��ÿ��page��Ӧ��frame_id,
      disk_manager_->WritePage(flush_page_id , pages_[frame_id].data_);  //  ��ָ��ҳ������д������ļ�������ɻ���������ҳ��ͬ��
    }
    page_table_.erase(pages_[frame_id].page_id_);   // ��page_table��ɾ����frame��Ӧ��ҳ
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
����Ϊ�ӻ������ɾ����Ӧҳ��ID��ҳ�棬�������pin_count��0(��0�����������߳���������), 
�Ͳ���ɾ��,��ɾ���ɹ��ͽ�������������free_list_��
*/
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  
  DeallocatePage(page_id);
  latch_.lock();

  //1 ���ȣ�����ҳ���Ƿ�����ڻ���������δ�����򷵻�True��Ȼ�󣬼���ҳ����û���pin_count_�Ƿ�Ϊ0�����0�򷵻�False��
  //��������ѿ���DeletePgImp�ķ���ֵ������Ǹ�ҳ���Ƿ��û�ʹ�ã�����ڸ�ҳ�治�ڻ�����ʱҲ����True��
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) 
  {
    latch_.unlock();
    return true;
  }

  frame_id_t frame_id = it->second;   // �õ���ϣ����page_id��Ӧ��ʵֵ frame_id
  if (pages_[frame_id].pin_count_ != 0)       //������û��������������ڻ������ɾ����ҳ
  {
    latch_.unlock();
    return false;
  }

  //2 ����ҳ���Ƿ�Ϊ�࣬�������ҳ����д�ش���,Ȼ���ڽ���ɾ��
  if (pages_[frame_id].is_dirty_ == true) 
  {
    page_id_t flush_page_id = pages_[frame_id].page_id_;
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->WritePage(flush_page_id, pages_[frame_id].data_);
  }
  //û���������,��page_table_��ɾ����ҳ���ӳ�䣬�����ò�λ��ҳ���page_id ��ΪINVALID_PAGE_ID����󣬽���λID�������������
  page_table_.erase(it);    
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);   //���� frame_idӳ��Ŀ��ƿ����free_list��
  latch_.unlock();
  return true;
}





//����Ϊ�ṩ�û��򻺳��֪ͨҳ��ʹ����ϵĽӿڣ��û�������ʹ�����ҳ���ҳ��ID�Լ�ʹ�ù������Ƿ�Ը�ҳ������޸ġ�
//һ���̲߳�������һ��ҳ��, ����pin_count����1, ��֪ͨmanager�Ƿ��ҳ�Ѿ�����, ���κ��쳣���(����pin_count�Ѿ�Ϊ0��)�ͷ���false, 
//�ǵ�pin_count��Ϊ0ʱ����LRU��unpin ��û���߳����ô�ҳ�˾Ͳ���LRU�����еȴ���̭��
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty){ 
  latch_.lock();
  frame_id_t frame_id;
  auto it = page_table_.find(page_id);

  if(it == page_table_.end())
  {
    latch_.unlock();
    return false;
  }

  frame_id = page_table_[page_id];        //���õ���Ӧ���ƿ��frame_id
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
    replacer_->Unpin(frame_id);   // ��ҳ����LRU�ȴ���̭
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