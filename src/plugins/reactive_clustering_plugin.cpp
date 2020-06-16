
#include "reactive_clustering_plugin.hpp"
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

std::map<std::string, std::map<std::string, int64_t>> calculate_access_data_difference(
  std::map<std::string, std::map<std::string, int64_t>> base,
  std::map<std::string, std::map<std::string, int64_t>> subtractor
)
  {
    std::map<std::string, std::map<std::string, int64_t>> res;
    for (auto table_iterator=base.begin(); table_iterator!=base.end(); ++table_iterator){
      auto table_name = table_iterator->first;
      auto it_1 = subtractor.find(table_name);

      if (it_1 != subtractor.end()){
        res.insert({table_name, std::map<std::string, int64_t>()});
        for (auto column_iterator=table_iterator->second.begin(); column_iterator!=table_iterator->second.end(); ++column_iterator){
          auto column_name = column_iterator->first;

          auto it_2 = subtractor[table_name].find(column_name);
          if (it_2 != subtractor[table_name].end() && base[table_name][column_name] >= subtractor[table_name][column_name]){
            res[table_name][column_name] = base[table_name][column_name] - subtractor[table_name][column_name];
          }else{
            res[table_name][column_name] = base[table_name][column_name];
          }
        }
      }else{
          res[table_name] = base[table_name];
      }
    }
    return res;
}

std::map<std::string, std::map<std::string, int64_t>> get_current_access_data(){
  static std::map<std::string, std::map<std::string, int64_t>> last_access_data;

  if (last_access_data.empty()){
    last_access_data = get_access_data();
    return std::map<std::string, std::map<std::string, int64_t>>();
  }

  auto new_access_data = get_access_data();
  auto current_access_data = calculate_access_data_difference(new_access_data, last_access_data);
  last_access_data = new_access_data;

  return current_access_data;
}

std::map<std::string, std::string> get_most_accessed_columns(std::map<std::string, std::map<std::string, int64_t>> access_data){
  static std::map<std::string, std::string> last_most_accessed;
  std::map<std::string, std::string> most_accessed;

  for (auto it=access_data.begin(); it!=access_data.end(); ++it){
    auto table_name = it->first;
    std::string max_column_name = "";
    int64_t max_access_count = 0;
    int64_t first_accesses = 0;


    bool first = true;
    for (auto it2=it->second.begin(); it2!=it->second.end(); ++it2){
      auto column_name = it2->first;
      auto n_accesses = it2->second;

      if (first){
        first = false;
        max_column_name = column_name;
        max_access_count = n_accesses;
        first_accesses = n_accesses;
        continue;
      }

      if (n_accesses > max_access_count){
        max_access_count = n_accesses;
        max_column_name = column_name;
      }
    }
    if (max_access_count > 0 && max_access_count != first_accesses) {
      most_accessed.insert({table_name, max_column_name});
    } else {
      std::cout<<"[WARNING] table: " << table_name << " max_count: " << max_access_count << " first_accesses: " << first_accesses << '\n';
    }
  }

  if (last_most_accessed.empty()){
    last_most_accessed = most_accessed;
    return std::map<std::string, std::string>();
  }

  std::map<std::string, std::string> res;

  for(auto it=most_accessed.begin(); it!=most_accessed.end(); ++it){
    auto table_name = it->first;
    auto column_name = it->second;

    auto it_2 = last_most_accessed.find(table_name);
    if (it_2 != last_most_accessed.end()){
        if (last_most_accessed[table_name] == most_accessed[table_name]){
          res[table_name] = column_name;
        } else {
          std::cout<<"[WARNING] unstable table: " << table_name << "(" << last_most_accessed[table_name] << " -> " << most_accessed[table_name] << ")\n";
        }
    } else {
      std::cout<<"[WARNING] table was not present: " << table_name << "\n";
    }
  }
  last_most_accessed = most_accessed;

  return res;
}

void print_access_data(std::map<std::string, std::map<std::string, int64_t>> access_data){
  for (auto it=access_data.begin(); it!=access_data.end(); ++it){
    std::cout << it->first << " :\n";
    for (auto it2=it->second.begin(); it2!=it->second.end(); ++it2){
        std::cout << "    " << it2->first << " : " << it2->second << "\n";
    }
  }
}

void print_sort_orders(std::map<std::string, std::string> access_data){
  for (auto it=access_data.begin(); it!=access_data.end(); ++it){
    std::cout << it->first << " : " << it->second << "\n";
  }
}

bool check_if_already_sorted(std::string table_name, std::string column_name){
  auto table = Hyrise::get().storage_manager.get_table(table_name);
  auto column_id = table->column_id_by_name(column_name);
  const auto chunk_count = table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    auto order_information = chunk->ordered_by();
    auto chunk_column_id = order_information->first;
    if (chunk_column_id != column_id){
      return false;
    }
  }
  return true;
}

const std::string ReactiveClusteringPlugin::description() const { return "ReactiveClusteringPlugin"; }

void ReactiveClusteringPlugin::start() {
  std::cout<<"ReactiveClustering: Init!\n";
  Hyrise::get().log_manager.add_message(description(), "Initialized!", LogLevel::Info);
  get_current_access_data();
  _loop_thread = std::make_unique<PausableLoopThread>(THREAD_INTERVAL, [&](size_t) { _optimize_clustering(); });
}

void ReactiveClusteringPlugin::_optimize_clustering() {
  // if (_optimized) return;
  //
  // _optimized = true;

  //___________________________________



  auto access_data = get_current_access_data();
  // print_access_data(access_data);
  std::cout << "_______________________\n";
  if (access_data.empty()){
    std::cout<< "Empty access data\n";
    return;
  }

  // std::map<std::string, std::string> sort_orders = {{"orders_tpch_0_1", "o_orderdate"}, {"orders_tpch_1", "o_orderdate"}, {"lineitem_tpch_0_1", "l_shipdate"}, {"lineitem_tpch_1", "l_shipdate"}};
  std::map<std::string, std::string> sort_orders = get_most_accessed_columns(access_data);
  // print_sort_orders(sort_orders);
  if (sort_orders.empty()){
    std::cout<< "Empty sort orders\n";
    return;
  }

  //___________________________________


  for (auto& [table_name, column_name] : sort_orders) {
    std::cout << "Working with: " << table_name << " : " << column_name << '\n';
    if (!Hyrise::get().storage_manager.has_table(table_name)) {
      Hyrise::get().log_manager.add_message(description(), "No optimization possible with given parameters for " + table_name + " table!", LogLevel::Debug);
      continue;
    }

    if(check_if_already_sorted(table_name, column_name)){
      std::cout << "    OK " << table_name << " already sorted by " << column_name << '\n';
      continue;
    }
    std::cout << "    UPS Sorting " << table_name << " by " << column_name << "...\n";

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

void ReactiveClusteringPlugin::stop() {}


EXPORT_PLUGIN(ReactiveClusteringPlugin)

}  // namespace opossum
