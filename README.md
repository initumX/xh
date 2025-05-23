# 📁 xh — File Scanner & Duplicate Detector

A high-performance CLI utility to scan directories and detect duplicate files using multi-stage hashing.

Efficiently identifies duplicates by progressively narrowing down candidates with partial content hashes, minimizing full-file reads for speed and performance.

---

## 🔍 Features

- ✅ Recursive directory scanning
- 🔍 Duplicate detection using cascading hash strategies:
  - **Fast mode**: size → front → end → middle
  - **Normal mode**: size → front → end → middle → first_quarter → third_quarter *(default)*
  - **Full mode**: size → front → end → middle → full_hash
- 📏 Supports filtering by:
  - Minimum / maximum file size (e.g., `64K`, `1.5M`)
  - File extensions (including multi-part like `.tar.gz`)
- 💾 Efficient I/O: avoids reading entire files unless necessary
- 🧹 Safe handling of unreadable/broken files and permission errors
- 📊 Optional performance statistics

---

## 🚀 Quick Start

```bash
# Find all .jpg and .webp files ≥ 100KB in ~/Pictures and detect duplicates
python3 xh.py ~/Pictures -s 100K -e jpg,webp -w 
```

```bash
# Find all files from 100KB to 15MB  in ~/Downloads and detect duplicates
python3 xh.py ~/Downloads -s 100K -S 15M -w
```

```bash
# Scan all readable files ≥ 300M  in Downloads folder
python3 xh.py ~/Downloads -s 300M
```

---

## 🛠️ Command-Line Options

| Option | Description |
|--------|-------------|
| `DIRECTORY` | Root directory to scan |
| `-s SIZE` | Minimum file size (e.g., `64K`, `1.5M`) |
| `-S SIZE` | Maximum file size |
| `-e EXT` | Comma-separated list of extensions (e.g., `txt,pdf,tar.gz`) |
| `--fast` | Use fast cascade (can give false positive results) |
| `--normal` | Default mode (balanced accuracy/performance) |
| `--full` | Confirm matches using full file hash |
| `-w` | Enable duplicate detection after filtering |

---

## 📊 Sample Output

```text
🔍 Scanning directory: ~/Documents
🔧 Applied filters: Min.size: 64.00 KB, Extensions: .txt
🛠️ Mode: Find duplicates
🚀 Cascade: Normal (size → front → end → middle → first_quarter → third_quarter)
📦 Chunk size: 64 KB
✅ Found 3 duplicate groups:

📁 Group 1 — Size: 84.23 KB
/path/to/file1.txt
/path/to/copy1.txt

📁 Group 2 — Size: 192.00 KB
/path/to/report_v2.txt
/path/to/report_final.txt
```

---
