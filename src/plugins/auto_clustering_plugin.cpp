#include "auto_clustering_plugin.hpp"
#include "storage/table.hpp"


#include "sql/sql_pipeline_builder.hpp"


namespace opossum {

const std::string AutoClusteringPlugin::description() const { return "AutoClustering"; }

void AutoClusteringPlugin::start() {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("col_1", DataType::Int, false);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  storage_manager.add_table("DummyTable", table);

  auto pipeline = SQLPipelineBuilder{std::string{"SELECT * FROM meta_tables;"}}.create_pipeline();
  const auto [status, meta_table] = pipeline.get_result_table();

  auto s = *meta_table->get_value<pmr_string>(ColumnID{0}, 0);
  std:: string s2 = std::string{s};
  std::cout << s2 << "\n";

  std::cout << meta_table->column_count() << "\n";
  std::cout << meta_table->row_count() << "\n";
  std::cout << meta_table->column_id_by_name("table_name") << "\n";

  TableColumnDefinitions c_definitions = meta_table->column_definitions();
  for (TableColumnDefinition c_definition : c_definitions){
    std::cout << c_definition << "\n";
  }

  if (status == SQLPipelineStatus::Success) {
    std::cout << "Success!\n";
  } else {
    std::cout << "Problem\n";
  }

}

void AutoClusteringPlugin::stop() {
   Hyrise::get().storage_manager.drop_table("DummyTable");
  std::cout << "You killed me!\n";
}

EXPORT_PLUGIN(AutoClusteringPlugin)

}  // namespace opossum
