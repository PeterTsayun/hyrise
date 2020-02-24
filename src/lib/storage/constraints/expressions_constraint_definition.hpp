#pragma once

#include <functional>
#include <vector>
#include "expression/abstract_expression.hpp"
#include "storage/constraints/table_constraint_definition.hpp"
#include "types.hpp"

namespace opossum {

// Defines a unique constraint on a set of abstract expressions.

struct ExpressionsConstraintDefinition final {
  ExpressionsConstraintDefinition(ExpressionUnorderedSet init_column_expressions)
      : column_expressions(std::move(init_column_expressions)) {}

  bool operator==(const ExpressionsConstraintDefinition& rhs) const {
    return column_expressions == rhs.column_expressions;
  }
  bool operator!=(const ExpressionsConstraintDefinition& rhs) const { return !(rhs == *this); }

  ExpressionUnorderedSet column_expressions;
};

using ExpressionsConstraintDefinitions = std::unordered_set<ExpressionsConstraintDefinition>;

}  // namespace opossum

namespace std {

template <>
struct hash<opossum::ExpressionsConstraintDefinition> {
  size_t operator()(const opossum::ExpressionsConstraintDefinition& constraint) const {
    auto hash = boost::hash_value(constraint.column_expressions.size());
    for (const auto& expression : constraint.column_expressions) {
      boost::hash_combine(hash, expression->hash());
    }
    return hash;
  }
};

} // namespace std
