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
  //��������ڶ������з���
  for (size_t i = 0; i < num_instances; i++)  
  {
    BufferPoolManager *tmp = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager); //ָ��ָ���������
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
  return num_instances_ * pool_size_;     //����ȫ������ص�����������������ظ������Ի�������������ص�������������
  
}



//��BufferPoolManager�����������ҳ��id��������������������ʹ�ô˷�����
BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  return instances_[page_id % num_instances_];      //instacnes_���������ڴ洢��������Ļ���� , ͨ����ҳ��IDȡ��ķ�ʽ��ҳ��IDӳ������Ӧ�Ļ���ء�
}



//�Ӹ����BufferPoolManagerInstance�л�ȡpage_id��ҳ��
Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
    
  BufferPoolManager *instace = GetBufferPoolManager(page_id);
  return instace->FetchPage(page_id);
    
    
}



//�Ӹ����BufferPoolManagerInstanceȡ��pin page_id
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
����һ��ʵ���� ParallelBufferPoolManager ʱ��������ʼ����Ӧ��Ϊ0��ÿ�δ���һ����ҳ��ʱ����������ÿ�� BufferPoolManagerInstance��
����ʼ������ʼ��ֱ��һ���ɹ���Ȼ����ʼָ������һ��
*/
Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying BufferPoolManagerInstances
  //������ҳ�档���ǽ�����ѯ��ʽ�ӵײ�BufferPoolManagerInstances����ҳ�����

  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called

  
  //1.��start_idx_��ʼ��������������أ�����ڵ���NewPage�ɹ���ҳ�棬�򷵻ظ�ҳ�沢��start_idxָ���ҳ�����һ��ҳ�棻
  
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

  // 2.��ȫ������ص���NewPage��ʧ�ܣ��򷵻ؿ�ָ�룬������start_idx��
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
  for(size_t i = 0 ; i<num_instances_ ; i++)      //����ÿһ�������Ļ����
  {
    instances_[i]->FlushAllPages();
  }
}

}  // namespace bustub


/*
��������������ö�Ӧ��������صķ������ɡ�ֵ��ע����ǣ������ڻ�����д�ŵ�Ϊ�����ʵ����Ļ���ָ�룬
��������ú�����ӦΪ�����ʵ����Ļ����Ӧ���麯�������ң�����ParallelBufferPoolManager��BufferPoolManagerInstanceΪ�ֵܹ�ϵ��
���ParallelBufferPoolManager����ֱ�ӵ���BufferPoolManagerInstance��Ӧ��Imp���������ֱ����ParallelBufferPoolManager�д��
BufferPoolManagerInstanceָ��Ҳ�ǲ����еġ�
*/