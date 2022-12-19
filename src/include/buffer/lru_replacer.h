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

    
    explicit LRUReplacer(size_t num_pages);     // explicit�ؼ��ֵ����þ��Ƿ�ֹ�๹�캯������ʽ�Զ�ת��


    ~LRUReplacer() override;        //������������麯������ʱʹ����override����������ô�ú���������д������е�ͬ��������������뽫�޷�ͨ�����롣

    auto Victim(frame_id_t *frame_id) -> bool override;       //������������������ʹ�õ�ҳ�棬����������ȥ��ҳ�����ݴ洢�� *frame_id�����У�Ϊ��ʱ����False�����򷵻�True��

    void Pin(frame_id_t frame_id) override;   

    void Unpin(frame_id_t frame_id) override;

    auto Size() -> size_t override;        

    void DelelteNode(ListNode *p);      //���һ��ɾ�����ĺ���

private:
    //��ϣ��(unordered_map) + ˫������ʵ��(list) 

    std::unordered_map<frame_id_t ,ListNode*>m_hash;      //hash��¼���� <ҳ��ID �� ������>
    ListNode *head = new ListNode(0);
    ListNode *tail = new ListNode(0);
    std::mutex m_mutex;      //�ٽ�������
};


} 