//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// abstract_executor.h
//
// Identification: src/include/execution/executors/abstract_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/executor_context.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * The AbstractExecutor implements the Volcano tuple-at-a-time iterator model.
This is the base class from which all executors in the BustTub execution
engine inherit, and defines the minimal interface that all executors support.

AbstractExecutorʵ����Volcanoÿ��Ԫ�������ģ�͡�  ����bustubִ������������ִ�г���̳еĻ��࣬������������ִ�г���֧�ֵ���С�ӿڡ�
 */


class AbstractExecutor {      


 public:
  /**
   * Construct a new AbstractExecutor instance.
   * @param exec_ctx the executor context that the executor runs with   ִ�г�������ʱʹ�õ�ִ�г���������
   */
  explicit AbstractExecutor(ExecutorContext *exec_ctx) : exec_ctx_{exec_ctx} {}

  /** Virtual destructor. */
  virtual ~AbstractExecutor() = default;



  /**
   * Initialize the executor.
   * @warning This function must be called before Next() is called!
   * // ÿ��ִ������Ҫ��ʼ���Ķ�������һ����һ���ǳ�ʼ��ָ���ͷ�ĵ�����ָ�룬�����Լ������һЩ����ѭ����ֵ
    // ��seq_scan��˵������Ҫ�����ｫ������ָ��ָ���ͷ��
   * 
   */
  virtual void Init() = 0;



  /**
   * Yield the next tuple from this executor. �����ִ����������һ��Ԫ��
   * 
   * // ��Ҫ��������һ��next�������ֻ���һ��tuple(һ��)�Ĺ��ܣ������ͨ����ֵ*tuple��������return true
    // ���û����������ˣ�return false

   * @param[out] tuple The next tuple produced by this executor
   * @param[out] rid The next tuple RID produced by this executor
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  virtual bool Next(Tuple *tuple, RID *rid) = 0;




  /** @return The schema of the tuples that this executor produces  // Schema�൱�ڴ洢�˱�ı�ͷ���֣�OutputSchema����ָ���˽ڵ���Ҫ�����Щ�У���Щ��ͷ��Ҫ�������ڣ�*/
  virtual const Schema *GetOutputSchema() = 0;




  /** @return The executor context in which this executor runs  ִ�г������������е�ִ�г��������� */
  ExecutorContext *GetExecutorContext() { return exec_ctx_; }



 protected:
  /** The executor context in which the executor runs  ִ�г������������е�ִ�г���������   */
  ExecutorContext *exec_ctx_;


};


}  // namespace bustub
