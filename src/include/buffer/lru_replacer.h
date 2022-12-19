//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include<unordered_map>
#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

class ListNode{

public:
    frame_id_t val;
    ListNode *prior;
    ListNode *next;

    ListNode(int value): val(value), prior(nullptr) , next(nullptr){}
    ~ListNode();
};


class LRUReplacer : public Replacer {
public:

    
    explicit LRUReplacer(size_t num_pages);     // explicit关键字的作用就是防止类构造函数的隐式自动转换


    ~LRUReplacer() override;        //如果派生类在虚函数声明时使用了override描述符，那么该函数必须重写其基类中的同名函数。否则代码将无法通过编译。

    auto Victim(frame_id_t *frame_id) -> bool override;       //驱除函数，驱除最少使用的页面，并将驱除出去的页面内容存储在 *frame_id参数中，为空时返回False，否则返回True；

    void Pin(frame_id_t frame_id) override;   

    void Unpin(frame_id_t frame_id) override;

    auto Size() -> size_t override;        

    void DelelteNode(ListNode *p);      //添加一个删除结点的函数

private:
    //哈希表(unordered_map) + 双向链表实现(list) 

    std::unordered_map<frame_id_t ,ListNode*>m_hash;      //hash记录的是 <页面ID ， 链表结点>
    ListNode *head = new ListNode(0);
    ListNode *tail = new ListNode(0);
    std::mutex m_mutex;      //临界区上锁
};


} 