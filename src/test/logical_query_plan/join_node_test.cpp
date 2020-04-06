#include <memory>
#include <utility>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/constraint_test_utils.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class JoinNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _mock_node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Float, "y"}}, "t_b");

    _t_a_a = {_mock_node_a, ColumnID{0}};
    _t_a_b = {_mock_node_a, ColumnID{1}};
    _t_a_c = {_mock_node_a, ColumnID{2}};
    _t_b_x = {_mock_node_b, ColumnID{0}};
    _t_b_y = {_mock_node_b, ColumnID{1}};

    _cross_join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
    _cross_join_node->set_left_input(_mock_node_a);
    _cross_join_node->set_right_input(_mock_node_b);

    _inner_join_node = JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
    _semi_join_node = JoinNode::make(JoinMode::Semi, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
    _anti_join_node = JoinNode::make(JoinMode::AntiNullAsTrue, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);

    // Prepare constraint definitions
    _unique_constraint_a = TableConstraintDefinition{{_t_a_a.original_column_id()}};
    _unique_constraint_b_c = TableConstraintDefinition{{_t_a_b.original_column_id(), _t_a_c.original_column_id()}};
    _unique_constraint_x = TableConstraintDefinition{{_t_b_x.original_column_id()}};
    _unique_constraint_y = TableConstraintDefinition{{_t_b_y.original_column_id()}};
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<JoinNode> _inner_join_node;
  std::shared_ptr<JoinNode> _semi_join_node;
  std::shared_ptr<JoinNode> _anti_join_node;
  std::shared_ptr<JoinNode> _cross_join_node;
  LQPColumnReference _t_a_a;
  LQPColumnReference _t_a_b;
  LQPColumnReference _t_a_c;
  LQPColumnReference _t_b_x;
  LQPColumnReference _t_b_y;
  TableConstraintDefinition _unique_constraint_a;
  TableConstraintDefinition _unique_constraint_b_c;
  TableConstraintDefinition _unique_constraint_x;
  TableConstraintDefinition _unique_constraint_y;
};

TEST_F(JoinNodeTest, Description) { EXPECT_EQ(_cross_join_node->description(), "[Join] Mode: Cross"); }

TEST_F(JoinNodeTest, DescriptionInnerJoin) { EXPECT_EQ(_inner_join_node->description(), "[Join] Mode: Inner [a = y]"); }

TEST_F(JoinNodeTest, DescriptionSemiJoin) { EXPECT_EQ(_semi_join_node->description(), "[Join] Mode: Semi [a = y]"); }

TEST_F(JoinNodeTest, DescriptionAntiJoin) {
  EXPECT_EQ(_anti_join_node->description(), "[Join] Mode: AntiNullAsTrue [a = y]");
}

TEST_F(JoinNodeTest, OutputColumnExpressions) {
  ASSERT_EQ(_cross_join_node->column_expressions().size(), 5u);
  EXPECT_EQ(*_cross_join_node->column_expressions().at(0), *lqp_column_(_t_a_a));
  EXPECT_EQ(*_cross_join_node->column_expressions().at(1), *lqp_column_(_t_a_b));
  EXPECT_EQ(*_cross_join_node->column_expressions().at(2), *lqp_column_(_t_a_c));
  EXPECT_EQ(*_cross_join_node->column_expressions().at(3), *lqp_column_(_t_b_x));
  EXPECT_EQ(*_cross_join_node->column_expressions().at(4), *lqp_column_(_t_b_y));
}

TEST_F(JoinNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_cross_join_node, *_cross_join_node);
  EXPECT_EQ(*_inner_join_node, *_inner_join_node);
  EXPECT_EQ(*_semi_join_node, *_semi_join_node);
  EXPECT_EQ(*_anti_join_node, *_anti_join_node);

  const auto other_join_node_a = JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_x), _mock_node_a, _mock_node_b);
  const auto other_join_node_b = JoinNode::make(JoinMode::Inner, not_like_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
  const auto other_join_node_c = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
  const auto other_join_node_d = JoinNode::make(JoinMode::Inner, equals_(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);

  EXPECT_NE(*other_join_node_a, *_inner_join_node);
  EXPECT_NE(*other_join_node_b, *_inner_join_node);
  EXPECT_NE(*other_join_node_c, *_inner_join_node);
  EXPECT_EQ(*other_join_node_d, *_inner_join_node);

  EXPECT_NE(other_join_node_a->hash(), _inner_join_node->hash());
  EXPECT_NE(other_join_node_b->hash(), _inner_join_node->hash());
  EXPECT_NE(other_join_node_c->hash(), _inner_join_node->hash());
  EXPECT_EQ(other_join_node_d->hash(), _inner_join_node->hash());
}

TEST_F(JoinNodeTest, Copy) {
  EXPECT_EQ(*_cross_join_node, *_cross_join_node->deep_copy());
  EXPECT_EQ(*_inner_join_node, *_inner_join_node->deep_copy());
  EXPECT_EQ(*_semi_join_node, *_semi_join_node->deep_copy());
  EXPECT_EQ(*_anti_join_node, *_anti_join_node->deep_copy());
}

TEST_F(JoinNodeTest, OutputColumnReferencesSemiJoin) {
  ASSERT_EQ(_semi_join_node->column_expressions().size(), 3u);
  EXPECT_EQ(*_semi_join_node->column_expressions().at(0), *lqp_column_(_t_a_a));
  EXPECT_EQ(*_semi_join_node->column_expressions().at(1), *lqp_column_(_t_a_b));
  EXPECT_EQ(*_semi_join_node->column_expressions().at(2), *lqp_column_(_t_a_c));
}

TEST_F(JoinNodeTest, OutputColumnReferencesAntiJoin) {
  ASSERT_EQ(_anti_join_node->column_expressions().size(), 3u);
  EXPECT_EQ(*_anti_join_node->column_expressions().at(0), *lqp_column_(_t_a_a));
  EXPECT_EQ(*_anti_join_node->column_expressions().at(1), *lqp_column_(_t_a_b));
  EXPECT_EQ(*_anti_join_node->column_expressions().at(2), *lqp_column_(_t_a_c));
}

TEST_F(JoinNodeTest, NodeExpressions) {
  ASSERT_EQ(_inner_join_node->node_expressions.size(), 1u);
  EXPECT_EQ(*_inner_join_node->node_expressions.at(0u), *equals_(_t_a_a, _t_b_y));
  ASSERT_EQ(_cross_join_node->node_expressions.size(), 0u);
}

TEST_F(JoinNodeTest, IsColumnNullableWithoutOuterJoin) {
  // Test that for LQPs without (Left,Right)Outer Joins, lqp_column_is_nullable() is equivalent to
  // expression.is_nullable()

  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(add_(_t_a_a, null_()), _t_b_x),
    ProjectionNode::make(expression_vector(_t_a_a, _t_a_b, add_(_t_a_a, null_())),
     _mock_node_a),
    _mock_node_b);
  // clang-format on

  EXPECT_FALSE(lqp->is_column_nullable(ColumnID{0}));
  EXPECT_FALSE(lqp->is_column_nullable(ColumnID{1}));
  EXPECT_TRUE(lqp->is_column_nullable(ColumnID{2}));
  EXPECT_FALSE(lqp->is_column_nullable(ColumnID{3}));
  EXPECT_FALSE(lqp->is_column_nullable(ColumnID{4}));
}

TEST_F(JoinNodeTest, IsColumnNullableWithOuterJoin) {
  // Test that columns on the "null-supplying" side of an outer join are always nullable.
  // Test that is_null_(<nullable>) is never nullable

  // clang-format off
  const auto lqp_left_join_basic =
  JoinNode::make(JoinMode::Left, equals_(_t_a_a, _t_b_x),
    _mock_node_a,
    _mock_node_b);
  // clang-format on

  EXPECT_FALSE(lqp_left_join_basic->is_column_nullable(ColumnID{0}));
  EXPECT_FALSE(lqp_left_join_basic->is_column_nullable(ColumnID{1}));
  EXPECT_FALSE(lqp_left_join_basic->is_column_nullable(ColumnID{2}));
  EXPECT_TRUE(lqp_left_join_basic->is_column_nullable(ColumnID{3}));
  EXPECT_TRUE(lqp_left_join_basic->is_column_nullable(ColumnID{4}));

  // clang-format off
  const auto lqp_left_join =
  ProjectionNode::make(expression_vector(_t_a_a, _t_b_x, add_(_t_a_a, _t_b_x), add_(_t_a_a, 3), is_null_(add_(_t_a_a, _t_b_x))),  // NOLINT
    JoinNode::make(JoinMode::Left, equals_(_t_a_a, _t_b_x),
      _mock_node_a,
      _mock_node_b));
  // clang-format on

  EXPECT_FALSE(lqp_left_join->is_column_nullable(ColumnID{0}));
  EXPECT_TRUE(lqp_left_join->is_column_nullable(ColumnID{1}));
  EXPECT_TRUE(lqp_left_join->is_column_nullable(ColumnID{2}));
  EXPECT_FALSE(lqp_left_join->is_column_nullable(ColumnID{3}));
  EXPECT_FALSE(lqp_left_join->is_column_nullable(ColumnID{4}));

  // clang-format off
  const auto lqp_right_join =
  ProjectionNode::make(expression_vector(_t_a_a, _t_b_x, add_(_t_a_a, _t_b_x), add_(_t_a_a, 3), is_null_(add_(_t_a_a, _t_b_x))),  // NOLINT
    JoinNode::make(JoinMode::Right, equals_(_t_a_a, _t_b_x),
      _mock_node_a,
      _mock_node_b));
  // clang-format on

  EXPECT_TRUE(lqp_right_join->is_column_nullable(ColumnID{0}));
  EXPECT_FALSE(lqp_right_join->is_column_nullable(ColumnID{1}));
  EXPECT_TRUE(lqp_right_join->is_column_nullable(ColumnID{2}));
  EXPECT_TRUE(lqp_right_join->is_column_nullable(ColumnID{3}));
  EXPECT_FALSE(lqp_right_join->is_column_nullable(ColumnID{4}));

  // clang-format off
  const auto lqp_full_join =
  ProjectionNode::make(expression_vector(_t_a_a, _t_b_x, add_(_t_a_a, _t_b_x), add_(_t_a_a, 3), is_null_(add_(_t_a_a, _t_b_x))),  // NOLINT
    JoinNode::make(JoinMode::FullOuter, equals_(_t_a_a, _t_b_x),
      _mock_node_a,
        _mock_node_b));
  // clang-format on

  EXPECT_TRUE(lqp_full_join->is_column_nullable(ColumnID{0}));
  EXPECT_TRUE(lqp_full_join->is_column_nullable(ColumnID{1}));
  EXPECT_TRUE(lqp_full_join->is_column_nullable(ColumnID{2}));
  EXPECT_TRUE(lqp_full_join->is_column_nullable(ColumnID{3}));
  EXPECT_FALSE(lqp_full_join->is_column_nullable(ColumnID{4}));
}

TEST_F(JoinNodeTest, FunctionalDependenciesDuplicates) {
  // In case of e.g. self-joins there might be FD duplicates, which have to be sorted out correctly.

  // Create a tables of 3 columns (a, b, c) where the last column of which is nullable (c)
  TableColumnDefinitions column_definitions{{"a", DataType::Int, false},
      {"b", DataType::Int, false},
      {"c", DataType::Int, false}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  const auto table_name = "table";
  Hyrise::get().storage_manager.add_table(table_name, table);

  // Add unique constraints
  table->add_soft_unique_constraint({{ColumnID{0}}});
  table->add_soft_unique_constraint({{ColumnID{0}, ColumnID{1}}});

  const auto stored_table_node_a = StoredTableNode::make(table_name);
  const auto stored_table_node_b = StoredTableNode::make(table_name);
  EXPECT_EQ(stored_table_node_a->functional_dependencies().size(), 2); // a => (b,c) and (a,b) => c
  EXPECT_EQ(stored_table_node_b->functional_dependencies().size(), 2); // a => (b,c) and (a,b) => c
  const auto join_column_a = lqp_column_(LQPColumnReference(stored_table_node_a, ColumnID{0}));
  const auto join_column_b = lqp_column_(LQPColumnReference(stored_table_node_b, ColumnID{1}));

  // Create JoinNode
  // clang-format off
  const auto join_node = JoinNode::make(JoinMode::Inner, equals_(join_column_a, join_column_b),
                                          stored_table_node_a,
                                            stored_table_node_b);
  // clang-format on

  EXPECT_EQ(join_node->functional_dependencies().size(), 2);
}

TEST_F(JoinNodeTest, FunctionalDependenciesNullabilityFilter) {


}



TEST_F(JoinNodeTest, ConstraintsSemiAndAntiJoins) {
  _mock_node_a->set_table_constraints({_unique_constraint_a, _unique_constraint_b_c});
  _mock_node_b->set_table_constraints({_unique_constraint_x});

  for(const auto join_mode : {JoinMode::Semi, JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse}) {
    // clang-format off
    const auto join_node = JoinNode::make(join_mode, equals_(_t_a_a, _t_b_y),
                             _mock_node_a,
                               _mock_node_b);
    // clang-format on

    EXPECT_TRUE(*join_node->constraints() == *_mock_node_a->constraints());
  }
}

TEST_F(JoinNodeTest, ConstraintsInnerAndOuterJoins) {
  // TODO(Julian) Test that...

  // Prepare join nodes
  for(const auto join_mode : {JoinMode::Inner, JoinMode::Left, JoinMode::Right, JoinMode::FullOuter}) {
    // clang-format off
    const auto join_node = JoinNode::make(join_mode, equals_(_t_a_a, _t_b_y),
                          _mock_node_a,
                            _mock_node_b);
    // clang-format on

    // Case 1 - Join columns of both, LEFT and RIGHT tables are not unique
    _mock_node_a->set_table_constraints({});
    _mock_node_b->set_table_constraints({});
    EXPECT_TRUE(join_node->constraints()->empty());

    // Case 2 - Join column of LEFT table (a) is unique whereas join column of RIGHT table (y) is not
    _mock_node_a->set_table_constraints({_unique_constraint_a, _unique_constraint_b_c});
    _mock_node_b->set_table_constraints({_unique_constraint_x});

    // Expect unique constraints of RIGHT table (x) to be forwarded
    auto join_constraints = join_node->constraints();
    EXPECT_EQ(join_constraints->size(), 1);
    EXPECT_TRUE(*join_constraints == *_mock_node_b->constraints());

    // Case 3 - Join column of LEFT table (a) is not(!) unique whereas join column of RIGHT table (y) is
    _mock_node_a->set_table_constraints({_unique_constraint_b_c});
    _mock_node_b->set_table_constraints({_unique_constraint_x, _unique_constraint_y});

    // Expect unique constraints of LEFT table (b_c) to be forwarded
    join_constraints = join_node->constraints();
    EXPECT_EQ(join_constraints->size(), 1);
    EXPECT_TRUE(*join_constraints == *_mock_node_a->constraints());

    // Case 4 - Join column of both, LEFT (a) and RIGHT (y) table are unique
    _mock_node_a->set_table_constraints({_unique_constraint_a, _unique_constraint_b_c});
    _mock_node_b->set_table_constraints({_unique_constraint_x, _unique_constraint_y});

    // Expect unique constraints of both, LEFT (a, b_c) and RIGHT (x, y) table to be forwarded
    join_constraints = join_node->constraints();
    EXPECT_EQ(join_constraints->size(), 4);
    check_table_constraint_representation({_unique_constraint_a, _unique_constraint_b_c, _unique_constraint_x,
                                              _unique_constraint_y}, join_constraints);
  }
}

TEST_F(JoinNodeTest, ConstraintsNonEquiJoin) {
  // Currently, we do not support constraint forwarding for Non-Equi- or Theta-Joins
  _mock_node_a->set_table_constraints({_unique_constraint_a, _unique_constraint_b_c});
  _mock_node_b->set_table_constraints({_unique_constraint_x, _unique_constraint_y});
  // clang-format off
  const auto theta_join_node = JoinNode::make(JoinMode::Inner, greater_than_(_t_a_a, _t_b_x),
                                                _mock_node_a,
                                                 _mock_node_b);
  // clang-format on

  EXPECT_TRUE(theta_join_node->constraints()->empty());
}

TEST_F(JoinNodeTest, ConstraintsNonSemiNonAntiMultiPredicateJoin) {
  // Except for Semi- and Anti-Joins, we currently do not support constraint forwarding for multi predicate joins.
  _mock_node_a->set_table_constraints({_unique_constraint_a, _unique_constraint_b_c});
  _mock_node_b->set_table_constraints({_unique_constraint_x, _unique_constraint_y});
  // clang-format off
  const auto join_node = JoinNode::make(JoinMode::Inner,
                                        expression_vector(less_than_(_t_a_a, _t_b_x), greater_than_(_t_a_a, _t_b_y)),
                                          _mock_node_a,
                                            _mock_node_b);
  // clang-format on

  EXPECT_TRUE(join_node->constraints()->empty());
}

TEST_F(JoinNodeTest, ConstraintsCrossJoin) {
  _mock_node_a->set_table_constraints({_unique_constraint_a, _unique_constraint_b_c});
  _mock_node_b->set_table_constraints({_unique_constraint_x, _unique_constraint_y});

  EXPECT_TRUE(_cross_join_node->constraints()->empty());
}

}  // namespace opossum
