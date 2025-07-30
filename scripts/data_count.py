import os
import glob

# Path setup
raw_data_path_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
raw_data_path = os.path.join(raw_data_path_dir, 'raw_data_files')

# Get all .txt files
csv_files = glob.glob(os.path.join(raw_data_path, '*.txt'))

file_line_counts = []
file_name_to_count = {}
zero_line_files = []

# Count lines in each file
for file_path in csv_files:
    with open(file_path, 'r', encoding='utf-8') as f:
        count = sum(1 for line in f if line.strip())  # non-empty lines only
        file_line_counts.append(count)
        file_name = os.path.basename(file_path)
        file_name_to_count[file_name] = count
        if count == 0:
            zero_line_files.append(file_name)

# Filter out zero-line files for min calculation
non_zero_counts = [count for count in file_line_counts if count > 0]

# Summary statistics
total_files = len(csv_files)
total_lines = sum(file_line_counts)
max_lines = max(file_line_counts) if file_line_counts else 0
min_lines = min(non_zero_counts) if non_zero_counts else 0
avg_lines = total_lines / total_files if total_files > 0 else 0

# Find files with min non-zero line count
min_line_files = [fname for fname, cnt in file_name_to_count.items() if cnt == min_lines]

# Display summary
print("📊 Taxi Data File Summary")
print(f"Total files              : {total_files}")
print(f"Total data points        : {total_lines}")
print(f"Maximum lines in a file  : {max_lines}")
print(f"Minimum lines in a file  : {min_lines} (excluding empty files)")
print(f"Files with min lines     : {len(min_line_files)}")
print(f"Average lines per file   : {avg_lines:.2f}")

# Display files with minimum lines (excluding 0)
if min_line_files:
    print(f"\n📁 Files with exactly {min_lines} records:")
    for filename in min_line_files:
        print(f"  - {filename}")

# Display files with zero records
if zero_line_files:
    print(f"\n🚫 Empty files ({len(zero_line_files)}):")
    for filename in zero_line_files:
        print(f"  - {filename}")
else:
    print("\n✅ No files with 0 records.")
