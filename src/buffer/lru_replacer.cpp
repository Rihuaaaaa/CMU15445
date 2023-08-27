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

//强制淘汰一帧, 将被淘汰的帧的id存入*frame_id, 如果LRU为空无法淘汰返回false, 不为空淘汰成功了就返回true
bool LRUReplacer::Victim(frame_id_t *frame_id){   
    
    //首先判断链表是否为空，如不为空则返回链表首节点的页面ID，并在哈希表中解除指向首节点的映射。
    m_mutex.lock();
    
    if(m_hash.empty() == true)
    {
        m_mutex.unlock();
        return false;
    }   

    //头部结点表示的是需要被驱逐的page ， 在LRU链表中每一个frame_id 对应的就是在缓存池中page的一个位置) 
    *frame_id = head->val;       //先赋值，题目要求删除元素记录在  frame_id 中
    m_hash.erase(head->val);    //接触哈希表中首节点的映射 

    ListNode *cur  = head;
    DelelteNode(cur);

    m_mutex.unlock();
    return true;    

}



//将某个内存帧(frame)固定在内存中, 不允许LRU淘汰它(也就是直接从LRU链表中删除该frame_id ,这样内存中的frame_id 所对应的page就能安全的呆在缓冲池中！)
void LRUReplacer::Pin(frame_id_t frame_id) {
    m_mutex.lock();

    auto it = m_hash.find(frame_id);       //
    if(it != m_hash.end())
    {   
        ListNode * cur = m_hash[frame_id];
        DelelteNode(cur);
        m_hash.erase(it); //删除映射即可
    }

    m_mutex.unlock();
    return;
}   

//某个帧对应的page页面没有线程引用了, 可以存入LRU等待被淘汰了
void LRUReplacer::Unpin(frame_id_t frame_id){
    m_mutex.lock();

    auto it = m_hash.find(frame_id);
    if(it == m_hash.end())  //不存在要插入
    {
        ListNode *newnode  = new ListNode(frame_id);
        if(m_hash.empty())      //如果hash表为空
        {   
            head = newnode;
            tail = newnode;
        }
        else        //尾插法插入结点
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

//计算出LRU里的帧数数目
size_t LRUReplacer::Size()  { 
    m_mutex.lock();
    size_t res = m_hash.size();
    m_mutex.unlock();
    return res;
}

void LRUReplacer::DelelteNode(ListNode *p){
    if( p == head && p == tail)     //只有一个结点
    {
        head = nullptr;
        tail = nullptr;
    }
    else if(p == head)          //删除的是头节点
    {
        head = head->next;
        p->next->prior = p->prior;      //实际上就是让头结点的prior指向null
    }
    else if(p == tail)      //删除的是尾结点
    {
        tail = tail->prior;
        p->prior->next = p->next;    //实际上就是让尾结点的next指向null

    }
    else        //删除的是正常结点
    {
        p->next->prior = p->prior;  
        p->prior->next = p->next;
    }
}

}  // namespace bustub

/*
注意Unpin的时候如果该frame已经在内存中了, 函数直接返回而不是将该frame更新到头部, 
这一点和LeetCode不一样(比LeetCode更简单), 仔细想一下这样是合理的, 因为LRU中U的含义是使用, 
只是Unpin并没有使用, 所以不应该对LRU的结构有影响。
*/