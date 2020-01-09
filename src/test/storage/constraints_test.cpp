#include <memory>

#include "base_test.hpp"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/table.hpp"

namespace opossum {

class ConstraintsTest : public BaseTest {
 protected:
  void SetUp() override {
    {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, false);
      column_definitions.emplace_back("column2", DataType::Int, false);
      column_definitions.emplace_back("column3", DataType::Int, false);
      auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

      auto& sm = Hyrise::get().storage_manager;
      sm.add_table("table", table);

      table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::No);
    }

    {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("column0", DataType::Int, false);
      column_definitions.emplace_back("column1", DataType::Int, true);
      auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

      auto& sm = Hyrise::get().storage_manager;
      sm.add_table("table_nullable", table);

      table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::No);
    }
  }
};

TEST_F(ConstraintsTest, InvalidConstraintAdd) {
  auto& sm = Hyrise::get().storage_manager;
  auto table = sm.get_table("table");
  auto table_nullable = sm.get_table("table_nullable");

  // TODO Update test: Invalid because the constraint contains duplicated columns.
//  auto count_before = table->get_soft_unique_constraints().size();
//  table->add_soft_unique_constraint({ColumnID{1}, ColumnID{1}}, IsPrimaryKey::No);
//  EXPECT_EQ(table->get_soft_unique_constraints().size(), count_before + 1);

  // Invalid because the column id is out of range
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{5}}, IsPrimaryKey::No), std::logic_error);

  // Invalid because the column must be non nullable for a primary key.
  EXPECT_THROW(table_nullable->add_soft_unique_constraint({ColumnID{1}}, IsPrimaryKey::Yes), std::logic_error);

  // Invalid because there is still a nullable column.
  EXPECT_THROW(table_nullable->add_soft_unique_constraint({ColumnID{0}, ColumnID{1}}, IsPrimaryKey::Yes),
               std::logic_error);

  table->add_soft_unique_constraint({ColumnID{2}}, IsPrimaryKey::Yes);

  // Invalid because another primary key already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{2}}, IsPrimaryKey::Yes), std::logic_error);

  // Invalid because a constraint on the same column already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::No), std::logic_error);

  table->add_soft_unique_constraint({ColumnID{0}, ColumnID{2}}, IsPrimaryKey::No);

  // Invalid because a concatenated constraint on the same columns already exists.
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{0}, ColumnID{2}}, IsPrimaryKey::No), std::logic_error);
  EXPECT_THROW(table->add_soft_unique_constraint({ColumnID{0}, ColumnID{2}}, IsPrimaryKey::Yes), std::logic_error);
}

TEST_F(ConstraintsTest, Equals) {
  const auto constraint_a = TableConstraintDefinition{{ColumnID{0}, ColumnID{2}}, IsPrimaryKey::No};
  const auto constraint_a_pk = TableConstraintDefinition{{ColumnID{0}, ColumnID{2}}, IsPrimaryKey::Yes};
  const auto constraint_b = TableConstraintDefinition{{ColumnID{2}, ColumnID{3}}, IsPrimaryKey::No};
  const auto constraint_c = TableConstraintDefinition{{ColumnID{0}}, IsPrimaryKey::No};

  EXPECT_TRUE(constraint_a.equals(constraint_a));

  EXPECT_FALSE(constraint_a.equals(constraint_a_pk));
  EXPECT_FALSE(constraint_a_pk.equals(constraint_a));

  EXPECT_FALSE(constraint_a.equals(constraint_b));
  EXPECT_FALSE(constraint_b.equals(constraint_a));

  EXPECT_FALSE(constraint_a.equals(constraint_c));
  EXPECT_FALSE(constraint_c.equals(constraint_a));
}

}  // namespace opossum
