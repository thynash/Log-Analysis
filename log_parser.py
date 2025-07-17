import re
import csv
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

# Extract block ID from message
def extract_block_id(message):
    match = re.search(r"(blk_-?\d+)", message)
    return match.group(1) if match else None

# Precompiled full regex for speed
LOG_PATTERN = re.compile(
    r'(?P<date>\d{6})\s+'
    r'(?P<time>\d{6})\s+'
    r'(?P<pid>\d+)\s+'
    r'(?P<level>[A-Z]+)\s+'
    r'(?P<component>[a-zA-Z0-9$._]+):\s+'
    r'(?P<message>.+)'
)

# Parses a single line into dict
def parse_line(line, line_id):
    match = LOG_PATTERN.match(line)
    if match:
        d = match.groupdict()
        timestamp = f"{d['date']} {d['time']}"
        block_id = extract_block_id(d['message'])
        return {
            "line_id": line_id,
            "timestamp": timestamp,
            "log_level": d['level'],
            "message": d['message'],
            "block_id": block_id
        }
    return None

# Batch processor (input: list of (line_id, line))
def parse_batch(lines_with_ids):
    parsed = []
    for line_id, line in lines_with_ids:
        parsed_line = parse_line(line, line_id)
        if parsed_line:
            parsed.append(parsed_line)
    return parsed

# Main pipeline
def process_log_file(input_path, output_path, batch_size=100_000, workers=4):
    futures = []
    batch = []
    batch_count = 0
    line_id = 0

    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', newline='', encoding='utf-8') as csvfile, \
         ProcessPoolExecutor(max_workers=workers) as executor:

        writer = csv.DictWriter(csvfile, fieldnames=["line_id", "timestamp", "log_level", "message", "block_id"])
        writer.writeheader()

        for line in tqdm(infile, desc="Reading lines"):
            batch.append((line_id, line.strip()))
            line_id += 1

            if len(batch) >= batch_size:
                futures.append(executor.submit(parse_batch, batch[:]))
                print(f"✅ Dispatched batch {batch_count+1} (lines {line_id - batch_size} to {line_id - 1})")
                batch.clear()
                batch_count += 1

        if batch:
            futures.append(executor.submit(parse_batch, batch))
            print(f"✅ Dispatched final batch {batch_count+1} (lines {line_id - len(batch)} to {line_id - 1})")

        for future in tqdm(futures, desc="Writing parsed batches"):
            parsed_lines = future.result()
            for row in parsed_lines:
                writer.writerow(row)

    print(f"✅ Completed! Output written to: {output_path}")

if __name__ == "__main__":
    process_log_file("data/HDFS.log", "parsed_logs.csv")

