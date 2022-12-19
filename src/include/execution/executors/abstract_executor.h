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

AbstractExecutor实现了Volcano每次元组迭代器模型。  这是bustub执行引擎中所有执行程序继承的基类，并定义了所有执行程序支持的最小接口。
 */


class AbstractExecutor {      


 public:
  /**
   * Construct a new AbstractExecutor instance.
   * @param exec_ctx the executor context that the executor runs with   执行程序运行时使用的执行程序上下文
   */
  explicit AbstractExecutor(ExecutorContext *exec_ctx) : exec_ctx_{exec_ctx} {}

  /** Virtual destructor. */
  virtual ~AbstractExecutor() = default;



  /**
   * Initialize the executor.
   * @warning This function must be called before Next() is called!
   * // 每个执行器需要初始化的东西都不一样，一般是初始化指向表头的迭代器指针，或者自己定义的一些辅助循环的值
    // 拿seq_scan来说，就需要在这里将迭代器指针指向表头。
   * 
   */
  virtual void Init() = 0;



  /**
   * Yield the next tuple from this executor. 从这个执行器生成下一个元组
   * 
   * // 需要做到调用一次next就输出且只输出一组tuple(一行)的功能，输出是通过赋值*tuple参数，并return true
    // 如果没有能输出的了，return false

   * @param[out] tuple The next tuple produced by this executor
   * @param[out] rid The next tuple RID produced by this executor
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  virtual bool Next(Tuple *tuple, RID *rid) = 0;




  /** @return The schema of the tuples that this executor produces  // Schema相当于存储了表的表头名字，OutputSchema用来指出此节点需要输出哪些列（哪些表头需要考虑在内）*/
  virtual const Schema *GetOutputSchema() = 0;




  /** @return The executor context in which this executor runs  执行程序在其中运行的执行程序上下文 */
  ExecutorContext *GetExecutorContext() { return exec_ctx_; }



 protected:
  /** The executor context in which the executor runs  执行程序在其中运行的执行程序上下文   */
  ExecutorContext *exec_ctx_;


};


}  // namespace bustub
