//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// abstract_expression.h
//
// Identification: src/include/expression/abstract_expression.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * AbstractExpression is the base class of all the expressions in the system.   AbstractExpression是系统中所有表达式的基类。 
 * 
 * 表达式类，一颗表达式树中的节点，比如比较，聚合，或者常量，column元素。其中column表达式也作为了column类的成员之一。
 * 不同的表达式实现的功能差距交大，这个是非常重要的一个类，每一个executor的代码都会用到。
 * 
 * Expressions are modeled as trees, i.e. every expression may have a variable number of children.
 * 表达式被建模为树，即每个表达式可能有数量可变的子表达式。
 */


class AbstractExpression {
 public:
  /**
   * Create a new AbstractExpression with the given children and return type.
   * @param children the children of this abstract expression
   * @param ret_type the return type of this abstract expression when it is evaluated
   */
  AbstractExpression(std::vector<const AbstractExpression *> &&children, TypeId ret_type)
      : children_{std::move(children)}, ret_type_{ret_type} {}





  /** Virtual destructor. */
  virtual ~AbstractExpression() = default;




  /** @return The value obtained by evaluating the tuple with the given schema */
  virtual Value Evaluate(const Tuple *tuple, const Schema *schema) const = 0;





  /**
   * Returns the value obtained by evaluating a JOIN.
   * @param left_tuple The left tuple
   * @param left_schema The left tuple's schema
   * @param right_tuple The right tuple
   * @param right_schema The right tuple's schema
   * @return The value obtained by evaluating a JOIN on the left and right
   */
   virtual auto EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema, const Tuple *right_tuple,
                            const Schema *right_schema) const -> Value = 0;



  /**
   * Returns the value obtained by evaluating the aggregates.
   * @param group_bys The group by values
   * @param aggregates The aggregate values
   * @return The value obtained by checking the aggregates and group-bys
   */
  virtual Value EvaluateAggregate(const std::vector<Value> &group_bys, const std::vector<Value> &aggregates) const = 0;



  /** @return the child_idx'th child of this expression */
  const AbstractExpression *GetChildAt(uint32_t child_idx) const { return children_[child_idx]; }



  /** @return the children of this expression, ordering may matter */
  const std::vector<const AbstractExpression *> &GetChildren() const { return children_; }




  /** @return the type of this expression if it were to be evaluated */
  virtual TypeId GetReturnType() const { return ret_type_; }





 private:
  /** The children of this expression. Note that the order of appearance of children may matter.  这个表达的后代注意，孩子出现的顺序可能很重要 */
  std::vector<const AbstractExpression *> children_;
  /** The return type of this expression. */
  TypeId ret_type_;
};
}  // namespace bustub
