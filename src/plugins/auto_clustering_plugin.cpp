#include "auto_clustering_plugin.hpp"
#include "storage/table.hpp"


#include "sql/sql_pipeline_builder.hpp"


namespace opossum {

const std::string AutoClusteringPlugin::description() const { return "AutoClustering"; }

std::map<std::string, std::map<std::string, int64_t>> get_access_data(){
  std::string sql = "SELECT table_name, column_name, "
  "SUM(point_accesses) + SUM(sequential_accesses) + SUM(monotonic_accesses) + SUM(random_accesses) "
  "AS n_accesses "
  "FROM meta_segments "
  "GROUP BY table_name, column_name;";

  auto pipeline = SQLPipelineBuilder{sql}.create_pipeline();
  const auto [status, meta_table] = pipeline.get_result_table();

  auto table_name_column = meta_table->column_id_by_name("table_name");
  auto column_name_column = meta_table->column_id_by_name("column_name");
  auto n_accesses_column = meta_table->column_id_by_name("n_accesses");
  auto n_rows = meta_table->row_count();

  std::map<std::string, std::map<std::string, int64_t>> access_data;

  for (long unsigned int row = 0; row < n_rows; ++row){
    auto table_name = std::string{*meta_table->get_value<pmr_string>(ColumnID{table_name_column}, row)};
    auto column_name = std::string{*meta_table->get_value<pmr_string>(ColumnID{column_name_column}, row)};
    auto n_accesses = *meta_table->get_value<int64_t>(ColumnID{n_accesses_column}, row);

    auto it = access_data.find(table_name);
    if (it == access_data.end()){
      access_data.insert({table_name, std::map<std::string, int64_t>()});
    }
    access_data[table_name].insert({column_name, n_accesses});
  }

  return access_data;
}

std::map<std::string, std::string> get_most_accessed_columns(std::map<std::string, std::map<std::string, int64_t>> access_data){
  std::map<std::string, std::string> most_accessed{};

  for (auto it=access_data.begin(); it!=access_data.end(); ++it){
    auto table_name = it->first;
    std::string max_column_name = "";
    int64_t max_access_count = 0;

    bool first = true;
    for (auto it2=it->second.begin(); it2!=it->second.end(); ++it2){
      if (first){
        first = false;
        max_column_name = it2->first;
        max_access_count = it2->second;
        continue;
      }
      auto column_name = it2->first;
      auto n_accesses = it2->second;
      if (n_accesses > max_access_count){
        max_access_count = n_accesses;
        max_column_name = column_name;
      }
    }
    most_accessed.insert({table_name, max_column_name});
  }

  return most_accessed;
}

void print_access_data(std::map<std::string, std::map<std::string, int64_t>> access_data){
  for (auto it=access_data.begin(); it!=access_data.end(); ++it){
    std::cout << "Table: " << it->first << '\n';
    for (auto it2=it->second.begin(); it2!=it->second.end(); ++it2){
        std::cout << "   " << it2->first << " : " << it2->second << '\n';
    }
  }
}

void print_most_accessed_columns(std::map<std::string, std::string> most_accessed_columns){
  for (auto it=most_accessed_columns.begin(); it!=most_accessed_columns.end(); ++it){
    std::cout << it->first << " : " << it->second << '\n';
  }
}

void AutoClusteringPlugin::start() {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("col_1", DataType::Int, false);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  storage_manager.add_table("DummyTable", table);

  auto access_data = get_access_data();
  print_access_data(access_data);
  std::cout << '\n';
  auto most_accessed_columns = get_most_accessed_columns(access_data);
  print_most_accessed_columns(most_accessed_columns);
}

void AutoClusteringPlugin::stop() {
   Hyrise::get().storage_manager.drop_table("DummyTable");
  std::cout << "You killed me!\n";
}

EXPORT_PLUGIN(AutoClusteringPlugin)

}  // namespace opossum
