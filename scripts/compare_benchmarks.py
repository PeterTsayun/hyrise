#!/usr/bin/env python3

import json
import math
import numpy as np
import re
import sys
from terminaltables import AsciiTable
from termcolor import colored
from scipy.stats import ttest_ind
from array import array

p_value_significance_threshold = 0.001
min_iterations = 10
min_runtime_ns = 59 * 1000 * 1000 * 1000

def format_diff(diff):
    diff -= 1  # adapt to show change in percent
    if diff < 0.0:
        return f"{diff:.0%}"
    else:
        return f"+{diff:.0%}"

def color_diff(diff, inverse_colors = False):
    select_color = lambda value, color: color if abs(value - 1) > 0.05 else 'white'

    diff_str = format_diff(diff)
    color = 'green' if (diff_str[0] == '+') != (inverse_colors) else 'red'

    return colored(format_diff(diff), select_color(diff, color))

def geometric_mean(values):
    product = 1
    for value in values:
        product *= value

    return product**(1 / float(len(values)))

def calculate_and_format_p_value(old_durations, new_durations):
    p_value = ttest_ind(old_durations, new_durations)[1]
    is_significant = p_value < p_value_significance_threshold

    notes = ""
    old_runtime = sum(runtime for runtime in old_durations)
    new_runtime = sum(runtime for runtime in new_durations)
    if (old_runtime < min_runtime_ns or new_runtime < min_runtime_ns):
        is_significant = False
        return "(run time too short)"
    elif (len(old_durations) < min_iterations or len(new_durations) < min_iterations):
        is_significant = False

        # in case we cannot decide whether the change is significant due to an insufficient number of measurements, the
        # add_note_for_insufficient_pvalue_runs flag it set, for which a note is later added to the table output
        global add_note_for_insufficient_pvalue_runs
        add_note_for_insufficient_pvalue_runs = True
        return colored('˅', 'yellow', attrs=['bold'])
    else:
        if is_significant:
            return colored(f"{p_value:.4f}", 'white')
        else:
            return colored(f"{p_value:.4f}", 'yellow', attrs=['bold'])

def print_context_overview(old_config, new_config):
    ignore_difference_for = ['GIT-HASH', 'date']

    old_context_keys = set(old_config['context'].keys())
    new_context_keys = set(new_config['context'].keys())
    common_context_keys = old_context_keys & new_context_keys

    table_lines = [["Parameter", sys.argv[1], sys.argv[2]]]
    for key in sorted(common_context_keys):
        old_value = old_config['context'][key]
        new_value = new_config['context'][key]
        color = 'white'
        marker = ' '
        if old_value != new_value and key not in ignore_difference_for:
            color = 'red'
            marker = '≠'
        table_lines.append([colored(marker + key, color), old_value, new_value])

    # print keys that are not present in both contexts
    for key in sorted(old_context_keys - common_context_keys):
        value = old_config['context'][key]
        table_lines.append([colored('≠' + key, 'red'), value, 'undefined'])

    for key in sorted(new_context_keys - common_context_keys):
        value = new_config['context'][key]
        table_lines.append([colored('≠' + key, 'red'), 'undefined', value])

    table = AsciiTable(table_lines)
    table.title = 'Configuration Overview'
    print(table.table)


# Doubles the separators (can be | normal rows and + for horizontal separators within the table)
def double_vertical_separators(lines, vertical_separators_to_duplicate):
   for line_id, line in enumerate(lines):
        vertical_separator = line[0]
        # positions might change due to color coding
        pos_separators = [m.start() for m in re.finditer(re.escape(vertical_separator), line)]
        # 0 required for splicing
        pos_splits = [0] + [pos_separators[index] for index in vertical_separators_to_duplicate]
        new_line = [line[i:j] for i,j in zip(pos_splits, pos_splits[1:] + [None])]
        lines[line_id] = vertical_separator.join(new_line)
   return lines



if not len(sys.argv) in [3, 4]:
    exit("Usage: " + sys.argv[0] + " benchmark1.json benchmark2.json [--github]")

# Format the output as a diff (prepending - and +) so that Github shows colors
github_format = bool(len(sys.argv) == 4 and sys.argv[3] == '--github')

with open(sys.argv[1]) as old_file:
    old_data = json.load(old_file)

with open(sys.argv[2]) as new_file:
    new_data = json.load(new_file)

if old_data['context']['benchmark_mode'] != new_data['context']['benchmark_mode']:
    exit("Benchmark runs with different modes (ordered/shuffled) are not comparable")

diffs_throughput = []
total_runtime_old = 0
total_runtime_new = 0

add_note_for_capped_runs = False
add_note_for_insufficient_pvalue_runs = False

print_context_overview(old_data, new_data)

table_data = []
# $latency and $thrghpt (abbreviated to keep the column at a max width of 8 chars) will later be replaced with a title
# spanning two columns
table_data.append(["Item", "$latency", "", "Change", "$thrghpt", "", "Change", "p-value"])
table_data.append(["", "old", "new", "", "old", "new", "", ""])

for old, new in zip(old_data['benchmarks'], new_data['benchmarks']):
    name = old['name']
    if old['name'] != new['name']:
        name += ' -> ' + new['name']

    old_successful_durations = np.array([run["duration"] for run in old["successful_runs"]], dtype=np.float64)
    new_successful_durations = np.array([run["duration"] for run in new["successful_runs"]], dtype=np.float64)
    old_unsuccessful_durations = np.array([run["duration"] for run in old["unsuccessful_runs"]], dtype=np.float64)
    new_unsuccessful_durations = np.array([run["duration"] for run in new["unsuccessful_runs"]], dtype=np.float64)
    old_iteration_count = len(old_successful_durations) + len(old_unsuccessful_durations)
    new_iteration_count = len(new_successful_durations) + len(new_unsuccessful_durations)
    old_avg_successful_duration = np.mean(old_successful_durations)  # defaults to np.float64 for int input
    new_avg_successful_duration = np.mean(new_successful_durations)

    total_runtime_old += old_avg_successful_duration if not math.isnan(old_avg_successful_duration) else 0.0
    total_runtime_new += new_avg_successful_duration if not math.isnan(new_avg_successful_duration) else 0.0

    if float(old_avg_successful_duration) > 0.0:
        diff_duration = float(new_avg_successful_duration / old_avg_successful_duration)
    else:
        diff_duration = float('nan')

    if float(old['items_per_second']) > 0.0:
        diff_throughput = float(new['items_per_second']) / float(old['items_per_second'])
        diffs_throughput.append(diff_throughput)
    else:
        diff_throughput = float('nan')

    diff_throughput_formatted = color_diff(diff_throughput)
    diff_duration_formatted = color_diff(diff_duration, True)
    p_value_formatted = calculate_and_format_p_value(old_successful_durations, new_successful_durations)

    if (old_data['context']['max_runs'] > 0 or new_data['context']['max_runs'] > 0) and \
       (old_iteration_count >= old_data['context']['max_runs'] or new_iteration_count >= new_data['context']['max_runs']):
        note = colored('˄', 'yellow', attrs=['bold'])
        add_note_for_capped_runs = True
    else:
        note = ' '

    # Note, we use column widths of 7 (latency) and 8 (throughput) for printing to ensure that we can later savely
    # replace the latency/throughput marker and everything still fits nicely.
    table_data.append([name,
                       f'{(old_avg_successful_duration / 1e6):>7.1f}' if old_avg_successful_duration else 'nan',
                       f'{(new_avg_successful_duration / 1e6):>7.1f}' if new_avg_successful_duration else 'nan',
                       diff_duration_formatted + note if not math.isnan(diff_duration) else '',
                       f'{old["items_per_second"]:>8.2f}', f'{new["items_per_second"]:>8.2f}',
                       diff_throughput_formatted + note, p_value_formatted])

    if (len(old['unsuccessful_runs']) > 0 or len(new['unsuccessful_runs']) > 0):
        if old_data['context']['benchmark_mode'] == 'Ordered':
            old_unsuccessful_per_second = float(len(old_unsuccessful_durations)) / (float(old['duration']) / 1e9)
            new_unsuccessful_per_second = float(len(new_unsuccessful_durations)) / (float(new['duration']) / 1e9)
        else:
            old_unsuccessful_per_second = float(len(old_unsuccessful_durations)) / (float(old_data['summary']['total_duration']) / 1e9)
            new_unsuccessful_per_second = float(len(new_unsuccessful_durations)) / (float(new_data['summary']['total_duration']) / 1e9)

        old_avg_unsuccessful_iteration = np.mean(old_unsuccessful_durations)
        new_avg_unsuccessful_iteration = np.mean(new_unsuccessful_durations)
        if len(old_unsuccessful_durations) > 0 and len(new_unsuccessful_durations) > 0:
            diff_throughput_unsuccessful = float(new_unsuccessful_per_second / old_unsuccessful_per_second)
            diff_duration_unsuccessful = new_avg_unsuccessful_iteration / old_avg_unsuccessful_iteration
        else:
            diff_throughput_unsuccessful = float('nan')
            diff_duration_unsuccessful = float('nan')

        unsuccessful_info = [
            '   unsucc.:',
            f'{(old_avg_unsuccessful_iteration / 1e6):>7.1f}' if not math.isnan(old_avg_unsuccessful_iteration) else 'nan',
            f'{(new_avg_unsuccessful_iteration / 1e6):>7.1f}' if not math.isnan(new_avg_unsuccessful_iteration) else 'nan',
            format_diff(diff_duration_unsuccessful) + ' ' if not math.isnan(diff_duration_unsuccessful) else ' ',
            f'{old_unsuccessful_per_second:>.2f}',
            f'{new_unsuccessful_per_second:>.2f}',
            format_diff(diff_throughput_unsuccessful) + ' ' if not math.isnan(diff_throughput_unsuccessful) else ' '
        ]

        unsuccessful_info_colored = [colored(text, attrs=['dark']) for text in unsuccessful_info]
        table_data.append(unsuccessful_info_colored)

table_data.append(['Sum', f'{(total_runtime_old / 1e6):>7.1f}', f'{(total_runtime_new / 1e6):>7.1f}',
                   color_diff(total_runtime_new / total_runtime_old, True) + ' '])
table_data.append(['Geomean', '', '', '', '', '', color_diff(geometric_mean(diffs_throughput)) + ' '])

table = AsciiTable(table_data)
for column_index in range(1, len(table_data[0])): # all columns justified to right, except for item name
    table.justify_columns[column_index] = 'right'

result = str(table.table)

new_result = ''
lines = result.splitlines()


# Double the vertical line between the item names and the two major measurements.
lines = double_vertical_separators(lines, [1, 4])

# As the used terminaltables module does not support cells that span multiple columns, we do that manually for latency
# and throughput in the header. We used two space holders that are narrow enough to not grow the column any wider than
# necessary for the actual values. As the new header spans two columns, we use the full descriptions.
for (placeholder, final) in [('$thrghpt', 'Throughput (iter/s)'), ('$latency', 'Latency (ms/iter)')]:
    header_strings = lines[1].split('|')
    for column_id, text in enumerate(header_strings):
        if placeholder in text:
            title_column = header_strings[column_id]
            unit_column = header_strings[column_id + 1]
            previous_length = len(title_column) + len(unit_column) + 1
            new_title = f' {final} '.ljust(previous_length,' ')
            lines[1] = '|'.join(header_strings[:column_id] + [new_title] + header_strings[column_id+2:])


# Swap second line of header with automatically added separator. Terminaltables does not support multi-line header. So
# we have the second header line as part of the results after a separator line. We need to swap these.
lines[2], lines[3] = lines[3], lines[2]
for (line_number, line) in enumerate(lines):
    if line_number == len(table_data):
        # Add another separation between benchmark items and aggregates
        new_result += lines[-1] + "\n"

    new_result += line + "\n"


# In case the runs for the executed benchmark have been cut or the number of runs was insufficient for the p-value
# calcution, we add notes to the end of the table.
if add_note_for_capped_runs or add_note_for_insufficient_pvalue_runs:
    first_column_width = len(lines[1].split('|')[1])
    width_for_note = len(lines[0]) - first_column_width - 5 # 5 for seperators and spaces
    if add_note_for_capped_runs:
        note = '˄' + f' Execution stopped at {new_data["context"]["max_runs"]} runs'
        new_result += '|' + (' Notes '.rjust(first_column_width, ' ')) +  '|| ' + note.ljust(width_for_note, ' ') + '|\n'
    if add_note_for_insufficient_pvalue_runs:
        note = '˅' + ' Insufficient number of runs for p-value calculation'
        new_result += '|' + (' ' * first_column_width) + '|| ' + note.ljust(width_for_note, ' ') + '|\n'
    new_result += lines[-1] + "\n"

result = new_result


# If github_format is set, format the output in the style of a diff file where added lines (starting with +) are
# colored green, removed lines (starting with -) are red, and others (starting with an empty space) are black.
# Because terminaltables (unsurprisingly) does not support this hack, we need to post-process the result string,
# searching for the control codes that define text to be formatted as green or red.

if github_format:
    new_result = '```diff\n'
    green_control_sequence = colored('', 'green')[0:5]
    red_control_sequence = colored('', 'red')[0:5]

    for line in result.splitlines():
        if green_control_sequence + '+' in line:
            new_result += '+'
        elif red_control_sequence + '-' in line:
            new_result += '-'
        else:
            new_result += ' '

        new_result += line + '\n'
    new_result += '```'
    result = new_result

print("")
print(result)
print("")
