#pragma once

#include <memory>
#include <unordered_set>

#include "table_statistics.hpp"

namespace opossum {

class Table;
class TableStatistics2;

/**
 * Generate statistics about a Table by analysing its entire data. This may be slow, use with caution.
 */
TableStatistics generate_table_statistics(const Table& table);
void generate_table_statistics2(Table& table);

void generate_compact_table_statistics(TableStatistics2& table_statistics);

}  // namespace opossum
