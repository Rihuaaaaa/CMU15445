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

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer(){
    ListNode * p = head;
    while(p)
    {
        ListNode *cur = p;
        p = p->next;
        delete cur;
    }
}

//ǿ����̭һ֡, ������̭��֡��id����*frame_id, ���LRUΪ���޷���̭����false, ��Ϊ����̭�ɹ��˾ͷ���true
bool LRUReplacer::Victim(frame_id_t *frame_id){   
    
    //�����ж������Ƿ�Ϊ�գ��粻Ϊ���򷵻������׽ڵ��ҳ��ID�����ڹ�ϣ���н��ָ���׽ڵ��ӳ�䡣
    m_mutex.lock();
    
    if(m_hash.empty() == true)
    {
        m_mutex.unlock();
        return false;
    }   

    //ͷ������ʾ������Ҫ�������page �� ��LRU������ÿһ��frame_id ��Ӧ�ľ����ڻ������page��һ��λ��) 
    *frame_id = head->val;       //�ȸ�ֵ����ĿҪ��ɾ��Ԫ�ؼ�¼��  frame_id ��
    m_hash.erase(head->val);    //�Ӵ���ϣ�����׽ڵ��ӳ�� 

    ListNode *cur  = head;
    DelelteNode(cur);

    m_mutex.unlock();
    return true;    

}



//��ĳ���ڴ�֡(frame)�̶����ڴ���, ������LRU��̭��(Ҳ����ֱ�Ӵ�LRU������ɾ����frame_id ,�����ڴ��е�frame_id ����Ӧ��page���ܰ�ȫ�Ĵ��ڻ�����У�)
void LRUReplacer::Pin(frame_id_t frame_id) {
    m_mutex.lock();

    auto it = m_hash.find(frame_id);       //
    if(it != m_hash.end())
    {   
        ListNode * cur = m_hash[frame_id];
        DelelteNode(cur);
        m_hash.erase(it); //ɾ��ӳ�伴��
    }

    m_mutex.unlock();
    return;
}   

//ĳ��֡��Ӧ��pageҳ��û���߳�������, ���Դ���LRU�ȴ�����̭��
void LRUReplacer::Unpin(frame_id_t frame_id){
    m_mutex.lock();

    auto it = m_hash.find(frame_id);
    if(it == m_hash.end())  //������Ҫ����
    {
        ListNode *newnode  = new ListNode(frame_id);
        if(m_hash.empty())      //���hash��Ϊ��
        {   
            head = newnode;
            tail = newnode;
        }
        else        //β�巨������
        {
            tail->next = newnode;
            newnode->prior = tail;
            tail = newnode;
        }

        m_hash[frame_id] = tail;
    }   

    m_mutex.unlock();
    return;
}

//�����LRU���֡����Ŀ
size_t LRUReplacer::Size()  { 
    m_mutex.lock();
    size_t res = m_hash.size();
    m_mutex.unlock();
    return res;
}

void LRUReplacer::DelelteNode(ListNode *p){
    if( p == head && p == tail)     //ֻ��һ�����
    {
        head = nullptr;
        tail = nullptr;
    }
    else if(p == head)          //ɾ������ͷ�ڵ�
    {
        head = head->next;
        p->next->prior = p->prior;      //ʵ���Ͼ�����ͷ����priorָ��null
    }
    else if(p == tail)      //ɾ������β���
    {
        tail = tail->prior;
        p->prior->next = p->next;    //ʵ���Ͼ�����β����nextָ��null

    }
    else        //ɾ�������������
    {
        p->next->prior = p->prior;  
        p->prior->next = p->next;
    }
}

}  // namespace bustub

/*
ע��Unpin��ʱ�������frame�Ѿ����ڴ�����, ����ֱ�ӷ��ض����ǽ���frame���µ�ͷ��, 
��һ���LeetCode��һ��(��LeetCode����), ��ϸ��һ�������Ǻ����, ��ΪLRU��U�ĺ�����ʹ��, 
ֻ��Unpin��û��ʹ��, ���Բ�Ӧ�ö�LRU�Ľṹ��Ӱ�졣
*/