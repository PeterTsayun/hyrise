
#include "access_clustering_plugin.hpp"
#include "sql/sql_pipeline_builder.hpp"

#include "operators/get_table.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/base_segment.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"

namespace opossum {

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

const std::string AccessClusteringPlugin::description() const { return "AutoClusteringPlugin"; }

void AccessClusteringPlugin::start() {
  Hyrise::get().log_manager.add_message(description(), "Initialized!", LogLevel::Info);
  _loop_thread = std::make_unique<PausableLoopThread>(THREAD_INTERVAL, [&](size_t) { _optimize_clustering(); });
}

void AccessClusteringPlugin::_optimize_clustering() {
  if (_optimized) return;

  _optimized = true;

  auto access_data = get_access_data();
  // std::map<std::string, std::string> sort_orders = {{"orders_tpch_0_1", "o_orderdate"}, {"orders_tpch_1", "o_orderdate"}, {"lineitem_tpch_0_1", "l_shipdate"}, {"lineitem_tpch_1", "l_shipdate"}};
  std::map<std::string, std::string> sort_orders = get_most_accessed_columns(access_data);

  for (auto& [table_name, column_name] : sort_orders) {
    std::cout << "Working with table: " << table_name << " column: " << column_name << '\n';
    if (!Hyrise::get().storage_manager.has_table(table_name)) {
      Hyrise::get().log_manager.add_message(description(), "No optimization possible with given parameters for " + table_name + " table!", LogLevel::Debug);
      continue;
    }
    auto table = Hyrise::get().storage_manager.get_table(table_name);

    const auto sort_column_id = table->column_id_by_name(column_name);

    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();
    auto sort = Sort{table_wrapper, {SortColumnDefinition{sort_column_id, OrderByMode::Ascending}}, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes};
    sort.execute();
    const auto immutable_sorted_table = sort.get_output();

    Assert(immutable_sorted_table->chunk_count() == table->chunk_count(), "Mismatching chunk_count");

    table = std::make_shared<Table>(immutable_sorted_table->column_definitions(), TableType::Data,
                                    table->target_chunk_size(), UseMvcc::Yes);
    const auto column_count = immutable_sorted_table->column_count();
    const auto chunk_count = immutable_sorted_table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = immutable_sorted_table->get_chunk(chunk_id);
      auto mvcc_data = std::make_shared<MvccData>(chunk->size(), CommitID{0});
      Segments segments{};
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        const auto base_segment = chunk->get_segment(column_id);
        std::shared_ptr<BaseSegment> new_segment;
        const auto data_type = table->column_data_type(column_id);
        new_segment = ChunkEncoder::encode_segment(base_segment, data_type, SegmentEncodingSpec{EncodingType::Dictionary});
        segments.emplace_back(new_segment);
      }
      table->append_chunk(segments, mvcc_data);
      table->get_chunk(chunk_id)->set_ordered_by({sort_column_id,  OrderByMode::Ascending});
      table->get_chunk(chunk_id)->finalize();
    }

    // for (ChunkID chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    //   auto chunk = table->get_chunk(chunk_id);
    //   for (ColumnID column_)
    // const auto base_segment = chunk->get_segment(column_id);
    // memory_usage_old += base_segment->memory_usage(MemoryUsageCalculationMode::Sampled);

    // std::shared_ptr<BaseSegment> new_segment;
    // new_segment = encode_and_compress_segment(base_segment, data_type, SegmentEncodingSpec{EncodingType::LZ4});
    // memory_usage_new += new_segment->memory_usage(MemoryUsageCalculationMode::Sampled);

    // chunk->replace_segment(column_id, new_segment);
    // }

    table->set_table_statistics(TableStatistics::from_table(*table));
    generate_chunk_pruning_statistics(table);

    Hyrise::get().storage_manager.replace_table(table_name, table);
    if (Hyrise::get().default_lqp_cache)
      Hyrise::get().default_lqp_cache->clear();
    if (Hyrise::get().default_pqp_cache)
      Hyrise::get().default_pqp_cache->clear();

    Hyrise::get().log_manager.add_message(description(), "Applied new clustering configuration (" + column_name + ") to " + table_name + " table.", LogLevel::Warning);
  }
}

void AccessClusteringPlugin::stop() {}


EXPORT_PLUGIN(AccessClusteringPlugin)

}  // namespace opossum
