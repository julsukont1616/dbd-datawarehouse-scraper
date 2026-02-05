# DBD DataWarehouse Revenue Scraper

A Python-based web scraper that extracts revenue data (รายได้รวม) for Thai companies from the [DBD DataWarehouse](https://datawarehouse.dbd.go.th/).

## Features

- **Multi-strategy search**: Uses 8+ search strategies to find companies even with name variations
- **Pagination support**: Searches through all result pages to find exact matches
- **Parallel processing**: Supports multiple workers for faster scraping
- **Flexible input**: Accepts CSV, Excel (.xlsx/.xls), or text files
- **Registration number caching**: Saves reg numbers for faster re-runs
- **Match tracking**: Records how each company was matched (exact vs similarity)
- **Auto-retry**: Retries failed revenue extractions with configurable attempts
- **Backup safety**: Creates backups before overwriting existing output files
- **Config file support**: YAML configuration file for easy settings management

---

## Installation

### Prerequisites

- Python 3.8+
- Google Chrome browser
- ChromeDriver (matching your Chrome version)

### Install Dependencies

```bash
pip install selenium pandas openpyxl pyyaml
```

### ChromeDriver Setup

Download ChromeDriver from https://chromedriver.chromium.org/ and ensure it's in your PATH, or place it in the same directory as the script.

---

## Quick Start

```bash
# Basic usage - reads from config.yaml settings
python3 scraper_v2.py

# With custom input file
python3 scraper_v2.py -i companies.csv -c company_name

# Test with first 5 companies (visible browser)
python3 scraper_v2.py -i companies.csv --test 5 --visible

# Generate default config file
python3 scraper_v2.py --generate-config
```

---

## Configuration File

The scraper supports a YAML configuration file for easy settings management. This is the recommended way to configure the scraper for repeated use.

### Generate Default Config

```bash
python3 scraper_v2.py --generate-config
```

This creates `config.yaml` with all available settings and comments.

### Config File Structure

```yaml
# Input settings
input:
  file: "companies.csv"           # Input file path
  company_column: "company_name"  # Column name for company names
  reg_column: null                # Column for registration numbers (optional)
  sheet: null                     # Excel sheet name (optional)
  filter_thai: true               # Only include Thai companies

# Output settings
output:
  revenue_file: "dbd_revenue_v2.csv"
  not_found_file: "dbd_not_found_v2.csv"
  batch_dir: "batches_v2"
  force_overwrite: false

# Search settings
search:
  max_pages: 20                   # Max pages to search per term
  similarity_threshold: 0.95      # Minimum similarity score (0-1)

# Processing settings
processing:
  workers: 1                      # Number of parallel workers
  batch_size: 20                  # Companies per batch
  delay_between_requests: 3       # Seconds between requests

# Retry settings
retry:
  max_retries: 3                  # Max retries for "No revenue data"
  extra_wait_per_retry: 2         # Additional seconds per retry

# Browser settings
browser:
  headless: true                  # Run without visible window
  page_load_wait: 10              # Seconds to wait for page load

# Debug settings
debug:
  enabled: false                  # Save debug screenshots
  test_count: null                # Limit to N companies (null = all)

# Extraction settings
extraction:
  mode: "all"                     # "all" or "revenue_only"
  income_statement_fields:        # Fields from งบกำไรขาดทุน
    - รายได้หลัก
    - รายได้รวม
    - ต้นทุนขาย
    - กำไร(ขาดทุน) ขั้นต้น
    - ค่าใช้จ่ายในการขายและบริหาร
    - รายจ่ายรวม
    - ดอกเบี้ยจ่าย
    - กำไร(ขาดทุน) ก่อนภาษี
    - ภาษีเงินได้
    - กำไร(ขาดทุน) สุทธิ
  include_balance_sheet: true     # Set to false to skip Balance Sheet
  balance_sheet_fields:           # Fields from งบแสดงฐานะการเงิน
    - ลูกหนี้การค้าสุทธิ
    - สินค้าคงเหลือ
    - สินทรัพย์หมุนเวียน
    - ที่ดิน อาคารและอุปกรณ์
    - สินทรัพย์ไม่หมุนเวียน
    - สินทรัพย์รวม
    - หนี้สินหมุนเวียน
    - หนี้สินไม่หมุนเวียน
    - หนี้สินรวม
    - ส่วนของผู้ถือหุ้น
    - หนี้สินรวมและส่วนของผู้ถือหุ้น
```

### Using Config File

```bash
# Use default config.yaml
python3 scraper_v2.py

# Use custom config file
python3 scraper_v2.py --config my_settings.yaml

# CLI arguments override config file
python3 scraper_v2.py --config config.yaml --workers 4
```

### Priority Order

Settings are applied in this order (later overrides earlier):
1. **Hardcoded defaults** in script
2. **Config file** (config.yaml)
3. **Command line arguments**

---

## Input Formats

### CSV File
```csv
company_name,registration_number
บริษัท ABC จำกัด,
บริษัท XYZ จำกัด (มหาชน),0107537001650
```

### Excel File (.xlsx)
```bash
python3 scraper_v2.py -i data.xlsx -s Sheet1 -c "Company Name" -r "Reg Number"
```

### Text File (one company per line)
```
บริษัท ABC จำกัด
บริษัท XYZ จำกัด
```

---

## Output Format

### Financial Data CSV (`dbd_revenue_v2.csv`)

| Column | Description |
|--------|-------------|
| `company_name` | Original company name from input |
| `registration_number` | 13-digit DBD registration number |
| `match_type` | How the company was matched (see below) |
| `search_strategy` | Which search strategy found the match |
| `table_type` | Source table (งบกำไรขาดทุน or งบแสดงฐานะการเงิน) |
| `field_name` | Financial field name (see below) |
| `value` | Financial value in THB |
| `year` | Fiscal year (พ.ศ.) |

#### Income Statement Fields (งบกำไรขาดทุน) - 10 fields

| Thai Field Name | English Translation |
|-----------------|---------------------|
| รายได้หลัก | Main Revenue |
| รายได้รวม | Total Revenue |
| ต้นทุนขาย | Cost of Sales |
| กำไร(ขาดทุน) ขั้นต้น | Gross Profit/Loss |
| ค่าใช้จ่ายในการขายและบริหาร | Selling & Admin Expenses |
| รายจ่ายรวม | Total Expenses |
| ดอกเบี้ยจ่าย | Interest Expense |
| กำไร(ขาดทุน) ก่อนภาษี | Profit/Loss Before Tax |
| ภาษีเงินได้ | Income Tax |
| กำไร(ขาดทุน) สุทธิ | Net Profit/Loss |

#### Balance Sheet Fields (งบแสดงฐานะการเงิน) - 11 fields (optional)

| Thai Field Name | English Translation |
|-----------------|---------------------|
| ลูกหนี้การค้าสุทธิ | Trade Receivables (Net) |
| สินค้าคงเหลือ | Inventory |
| สินทรัพย์หมุนเวียน | Current Assets |
| ที่ดิน อาคารและอุปกรณ์ | Property, Plant & Equipment |
| สินทรัพย์ไม่หมุนเวียน | Non-Current Assets |
| สินทรัพย์รวม | Total Assets |
| หนี้สินหมุนเวียน | Current Liabilities |
| หนี้สินไม่หมุนเวียน | Non-Current Liabilities |
| หนี้สินรวม | Total Liabilities |
| ส่วนของผู้ถือหุ้น | Shareholders' Equity |
| หนี้สินรวมและส่วนของผู้ถือหุ้น | Total Liabilities & Equity |

**Example output:**
```csv
company_name,registration_number,match_type,search_strategy,table_type,field_name,value,year
บริษัท ABC จำกัด,0105550130954,exact,5,งบกำไรขาดทุน,รายได้รวม,6790765.26,2563
บริษัท ABC จำกัด,0105550130954,exact,5,งบกำไรขาดทุน,กำไร(ขาดทุน) สุทธิ,1500000.00,2563
บริษัท ABC จำกัด,0105550130954,exact,5,งบแสดงฐานะการเงิน,สินทรัพย์รวม,50000000.00,2563
บริษัท ABC จำกัด,0105550130954,exact,5,งบแสดงฐานะการเงิน,หนี้สินรวม,30000000.00,2563
```

### Not Found CSV (`dbd_not_found_v2.csv`)

| Column | Description |
|--------|-------------|
| `company_name` | Original company name from input |
| `registration_number` | Reg number if found but no revenue data |
| `match_type` | Match type if applicable |
| `search_strategy` | Search strategy if applicable |
| `reason` | Why the company wasn't processed |

---

## Match Types

| Value | Description |
|-------|-------------|
| `exact` | Exact match found in DBD search results |
| `similarity_XX%` | Match found via similarity scoring (e.g., `similarity_95%`) |
| `existing` | Registration number was provided in input file |

---

## Search Strategies

The scraper tries multiple search strategies in order until a match is found:

| # | Strategy | Example |
|---|----------|---------|
| `direct` | DBD redirected to company page | Single/exact match found |
| 1 | Full name (without บริษัท) | `ABC จำกัด (มหาชน)` |
| 2 | No-space variant | `ABC จำกัด(มหาชน)` |
| 3 | Without มหาชน | `ABC จำกัด` |
| 4 | Core name only | `ABC` |
| 5 | Without filler words | Removes `(ประเทศไทย)`, `Thailand`, etc. |
| 6 | Without parentheses | Removes all `(...)` content |
| 7 | Without trailing numbers | Removes years like `2020` |
| 8+ | Progressive word trimming | `ABC DEF` → `ABC` |
| `fallback` | Similarity scoring | Best match with ≥95% similarity |

**Note:** `direct` means DBD automatically redirected to the company detail page because there was only one match. This is the fastest path.

### Partnership Handling (ห้างหุ้นส่วน)

For partnerships, additional strategies are used:
- Full name: `ห้างหุ้นส่วนจำกัด ABC`
- Without prefix: `ABC`
- Short prefix: `ห้างหุ้นส่วน ABC`

---

## Command Line Options

### Input Options

| Option | Description | Default |
|--------|-------------|---------|
| `-i, --input` | Input file path (CSV, Excel, or text) | `companies.csv` |
| `-c, --column` | Column name for company names | `company_name` |
| `-r, --reg-column` | Column name for registration numbers | None |
| `-s, --sheet` | Sheet name for Excel files | First sheet |
| `--no-filter` | Include non-Thai companies | False |

### Output Options

| Option | Description | Default |
|--------|-------------|---------|
| `-o, --output` | Output CSV file path | `dbd_revenue_v2.csv` |
| `--not-found-output` | Not found CSV file path | `dbd_not_found_v2.csv` |
| `-f, --force` | Overwrite without backup | False |

### Processing Options

| Option | Description | Default |
|--------|-------------|---------|
| `-w, --workers` | Number of parallel workers | 1 |
| `--test N` | Process only first N companies | All |
| `--start N` | Start from Nth company (0-indexed) | 0 |
| `--max-search-pages` | Max pages to search per term | 20 |

### Retry Options

| Option | Description | Default |
|--------|-------------|---------|
| `--max-retries` | Max retries for "No revenue data" | 3 |
| `--no-retry` | Disable retry mechanism | False |

### Debug Options

| Option | Description | Default |
|--------|-------------|---------|
| `--visible` | Show browser window | False (headless) |
| `--debug` | Save debug screenshots | False |

---

## Usage Examples

### Basic Usage

```bash
# Process all companies from default input
python3 scraper_v2.py

# Process from custom CSV file
python3 scraper_v2.py -i my_companies.csv -c "Company Name"
```

### With Registration Numbers (Skip Search)

```bash
# If you have reg numbers from a previous run, use them to skip search
python3 scraper_v2.py -i companies.csv -c company_name -r registration_number
```

### Excel Input

```bash
# Read from Excel file, specific sheet
python3 scraper_v2.py -i data.xlsx -s "Sheet1" -c "บริษัท" -r "เลขทะเบียน"
```

### Parallel Processing

```bash
# Use 4 workers for faster processing
python3 scraper_v2.py -i companies.csv -w 4
```

### Testing & Debugging

```bash
# Test with first 10 companies, visible browser
python3 scraper_v2.py --test 10 --visible

# Debug mode with screenshots
python3 scraper_v2.py --test 5 --visible --debug
```

### Resume from Specific Index

```bash
# Start from company #100 (0-indexed)
python3 scraper_v2.py --start 100
```

### Adjust Search Depth

```bash
# Search up to 50 pages per search term (slower but more thorough)
python3 scraper_v2.py --max-search-pages 50

# Quick search (only 5 pages per term)
python3 scraper_v2.py --max-search-pages 5
```

---

## How It Works

### 1. Search Phase

For each company, the scraper:
1. Generates multiple search term variants
2. Searches DBD DataWarehouse for each term
3. Paginates through results looking for exact matches
4. Falls back to similarity scoring if no exact match found

### 2. Extraction Phase

Once a company is found:
1. Navigates to company detail page
2. Clicks on "ข้อมูลงบการเงิน" (Financial Data) tab
3. Extracts revenue data from the financial table
4. Retries if data not immediately available

### 3. Output Phase

Results are saved in batches to prevent data loss:
- Worker batch files: `worker_X_batch_Y_revenue.csv`
- Final combined files: `dbd_revenue_v2.csv`, `dbd_not_found_v2.csv`

---

## Similarity Scoring

When no exact match is found, the scraper uses token-based similarity:

```
similarity = |common_tokens| / |all_unique_tokens|
```

- Threshold: **95%** (configurable in code)
- Below threshold: Match is **rejected** (returns as not found)

---

## Backup Safety

Before combining batch files into final output:
- Checks if output files already exist
- Creates timestamped backups (e.g., `dbd_revenue_v2_backup_20240115_143022.csv`)
- Use `-f` or `--force` to skip backup

---

## Troubleshooting

### ChromeDriver Version Mismatch

```
Error: session not created: This version of ChromeDriver only supports Chrome version XX
```

**Solution**: Download matching ChromeDriver version from https://chromedriver.chromium.org/

### No Revenue Data

Some companies may not have financial data in DBD. The scraper will:
1. Retry up to 3 times (configurable)
2. Record in `not_found` CSV with reason "No revenue data"

### Company Not Found

If a company can't be matched:
1. Check company name spelling
2. Try providing the registration number directly
3. Increase `--max-search-pages` for deeper search

### Rate Limiting

If you encounter errors, try:
- Reducing number of workers (`-w 1`)
- The scraper has built-in delays between requests

---

## File Structure

```
dbd-datawarehouse-scraper/
├── scraper_v2.py           # Main scraper script
├── config.yaml             # Configuration file
├── README.md               # This file
├── companies.csv           # Default input file
├── dbd_revenue.csv         # Output: successful extractions
├── dbd_not_found.csv       # Output: failed extractions
└── batches/                # Temporary batch files
```

---

## License

MIT License
