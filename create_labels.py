import pandas as pd

# Load full parsed logs and ground truth
print("📥 Starting to load parsed logs... 🕒")
parsed_logs = pd.read_csv("parsed_logs.csv", usecols=["line_id", "timestamp", "log_level", "message", "block_id"])
print("📥 Parsed logs loaded successfully! 🎉")

print("📥 Loading ground truth labels... 🕒")
true_labels = pd.read_csv("data/preprocessed/anomaly_label.csv", usecols=["block_id", "label"])
print("📥 Ground truth labels loaded successfully! 🎉")

# ⚠️ Fix: Map string labels to int
print("🔧 Mapping string labels to integers... 🔧")
label_map = {
    "Normal": 0,
    "Anomaly": 1,
    "normal": 0,
    "anomaly": 1,
    0: 0,
    1: 1
}
true_labels["label"] = true_labels["label"].map(label_map)
print("🔧 Label mapping completed! ✅")

# Fill any unmapped (NaN) with 0 — assume normal
print("🧹 Filling unmapped labels with 0 (normal)... 🧹")
true_labels["label"] = true_labels["label"].fillna(0).astype(int)
print("🧹 Unmapped labels filled! ✅")

# Replace missing block_ids with dummy value
print("🧹 Replacing missing block_ids in parsed logs... 🧹")
parsed_logs["block_id"] = parsed_logs["block_id"].fillna("NO_BLOCK")
print("🧹 Missing block_ids in parsed logs replaced! ✅")

print("🧹 Replacing missing block_ids in ground truth... 🧹")
true_labels["block_id"] = true_labels["block_id"].fillna("NO_BLOCK")
print("🧹 Missing block_ids in ground truth replaced! ✅")

# Merge labels into parsed logs
print("🔗 Merging logs with ground truth labels... 🔗")
labeled_logs = pd.merge(parsed_logs, true_labels, on="block_id", how="left")
print("🔗 Merge completed successfully! 🎉")

# Missing labels = 0 (normal)
print("🔢 Assigning default label 0 to missing entries... 🔢")
labeled_logs["label"] = labeled_logs["label"].fillna(0).astype(int)
print("🔢 Default labeling done! ✅")

# Save final output
print("💾 Saving labeled logs to file... 💾")
labeled_logs.to_csv("parsed_logs_labeled_full.csv", index=False)
print(f"✅ Labeled full logs. Shape: {labeled_logs.shape}")
