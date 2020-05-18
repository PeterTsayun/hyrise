#pragma once

#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This node type represents sorting operations as defined in ORDER BY clauses.
 */
class SortNode : public EnableMakeForLQPNode<SortNode>, public AbstractLQPNode {
 public:
  explicit SortNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                    const std::vector<SortMode>& init_sort_modes);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::shared_ptr<ExpressionsConstraintDefinitions> constraints() const override;

  const std::vector<SortMode> sort_modes;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
