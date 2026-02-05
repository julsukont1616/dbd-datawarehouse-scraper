#!/usr/bin/env python3
"""
DBD DataWarehouse Revenue Scraper v2 (FULLY FIXED)
Extracts revenue data (รายได้รวม) for Thai companies from DBD DataWarehouse.

FEATURES:
1. Correct headless mode settings (--headless not --headless=new)
2. Anti-automation detection bypass
3. Proper year extraction from table headers
4. Correct cell-to-year mapping (value at index i*2 for year i)
5. Proper wait times for page/table loading
6. Auto-retry for "No revenue data" cases (configurable)
7. Pagination support - searches through multiple pages to find exact company match
8. Multiple search strategies: full name → จำกัด(มหาชน) → จำกัด → core name
9. Flexible input: supports CSV, Excel (.xlsx/.xls), and text files
10. Registration number support: can read existing reg numbers to skip search
11. Exports registration numbers in results for future re-runs

OUTPUT FORMAT:
    Financial Data CSV: company_name, registration_number, match_type, search_strategy, table_type, field_name, value, year
    Not Found CSV: company_name, registration_number, match_type, search_strategy, reason

    table_type values:
    - งบกำไรขาดทุน (Income Statement)
    - งบแสดงฐานะการเงิน (Balance Sheet) - optional, enabled via config

    Income Statement fields (configurable via extraction.income_statement_fields):
    - รายได้หลัก (Main Revenue)
    - รายได้รวม (Total Revenue)
    - ต้นทุนขาย (Cost of Sales)
    - กำไร(ขาดทุน) ขั้นต้น (Gross Profit/Loss)
    - ค่าใช้จ่ายในการขายและบริหาร (Selling & Admin Expenses)
    - รายจ่ายรวม (Total Expenses)
    - ดอกเบี้ยจ่าย (Interest Expense)
    - กำไร(ขาดทุน) ก่อนภาษี (Profit/Loss Before Tax)
    - ภาษีเงินได้ (Income Tax)
    - กำไร(ขาดทุน) สุทธิ (Net Profit/Loss)

    Balance Sheet fields (configurable via extraction.balance_sheet_fields):
    - ลูกหนี้การค้าสุทธิ (Trade Receivables)
    - สินค้าคงเหลือ (Inventory)
    - สินทรัพย์หมุนเวียน (Current Assets)
    - ที่ดิน อาคารและอุปกรณ์ (Property, Plant & Equipment)
    - สินทรัพย์ไม่หมุนเวียน (Non-Current Assets)
    - สินทรัพย์รวม (Total Assets)
    - หนี้สินหมุนเวียน (Current Liabilities)
    - หนี้สินไม่หมุนเวียน (Non-Current Liabilities)
    - หนี้สินรวม (Total Liabilities)
    - ส่วนของผู้ถือหุ้น (Shareholders' Equity)
    - หนี้สินรวมและส่วนของผู้ถือหุ้น (Total Liabilities & Equity)

    match_type values:
    - 'exact': Exact match found in DBD search
    - 'similarity_XX%': Match found via similarity scoring (e.g., similarity_95%)
    - 'existing': Registration number was provided in input file

    search_strategy values (1-based index of which search term found the match):
    - 'direct': DBD redirected to company detail page (single/exact match)
    - 1: Full name with จำกัด (มหาชน)
    - 2: Full name with จำกัด(มหาชน) - no space variant
    - 3: Full name with จำกัด only / partnership without prefix
    - 4: Core name (before จำกัด)
    - 5: Core name without filler words
    - 6: Core name without parentheses
    - 7: Core name without trailing numbers
    - 8+: Progressive word trimming (N-1, N-2, ... 1 word)
    - 'fallback': Similarity scoring fallback

Usage:
    # Basic usage with default input file
    python3 scraper_v2.py              # Run all companies

    # Custom input file (CSV, Excel, or text)
    python3 scraper_v2.py -i companies.csv -c company_name
    python3 scraper_v2.py -i data.xlsx -s Sheet1 -c "Company Name"
    python3 scraper_v2.py -i company_list.txt

    # With existing registration numbers (skips search for those companies)
    python3 scraper_v2.py -i companies.csv -c company_name -r registration_number

    # Custom output files
    python3 scraper_v2.py -o results.csv --not-found-output missing.csv

    # Test and debug options
    python3 scraper_v2.py --test 5     # Test with first 5 companies
    python3 scraper_v2.py --visible    # Show browser window
    python3 scraper_v2.py --resume     # Resume from last processed company

    # Performance options
    python3 scraper_v2.py --workers 4  # Run with 4 parallel workers
    python3 scraper_v2.py --max-retries 5  # Set max retries for "No revenue data"
    python3 scraper_v2.py --no-retry   # Disable retry (faster but may miss data)
    python3 scraper_v2.py --max-search-pages 30  # Search up to 30 pages for exact match
"""

import csv
import time
import argparse
import re
import os
from multiprocessing import Pool
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Try to import yaml for config file support
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

# Configuration
BASE_URL = "https://datawarehouse.dbd.go.th"
INPUT_CSV = "companies.csv"
OUTPUT_CSV = "dbd_revenue.csv"
NOT_FOUND_CSV = "dbd_not_found.csv"
PROGRESS_FILE = "progress.txt"
BATCH_DIR = "batches"
TARGET_YEARS = None  # None means export ALL available years
DELAY_BETWEEN_REQUESTS = 3
BATCH_SIZE = 20

# Wait times (in seconds)
PAGE_LOAD_WAIT = 10      # Initial page load
TAB_CLICK_WAIT = 4       # After clicking financial tab
TABLE_LOAD_WAIT = 6      # After clicking income statement button
EXTRA_WAIT = 3           # Additional wait for data

# Retry configuration for "No revenue data" cases
MAX_RETRY_NO_REVENUE = 3      # Max retries when company found but no revenue data
RETRY_EXTRA_WAIT = 2          # Additional wait per retry attempt

# Similarity threshold for fallback matching
SIMILARITY_THRESHOLD = 0.95   # 95% minimum similarity

# Default financial fields to extract from Income Statement (งบกำไรขาดทุน)
# Can be overridden via config.yaml extraction.fields
DEFAULT_FINANCIAL_FIELDS = [
    'รายได้หลัก',                         # Main Revenue
    'รายได้รวม',                         # Total Revenue
    'ต้นทุนขาย',                         # Cost of Sales
    'กำไร(ขาดทุน) ขั้นต้น',               # Gross Profit/Loss
    'ค่าใช้จ่ายในการขายและบริหาร',         # Selling & Admin Expenses
    'รายจ่ายรวม',                        # Total Expenses
    'ดอกเบี้ยจ่าย',                       # Interest Expense
    'กำไร(ขาดทุน) ก่อนภาษี',              # Profit/Loss Before Tax
    'ภาษีเงินได้',                        # Income Tax
    'กำไร(ขาดทุน) สุทธิ',                 # Net Profit/Loss
]

# Extraction mode: 'all' for all fields, 'revenue_only' for backward compatibility
EXTRACTION_MODE = 'all'
FINANCIAL_FIELDS = DEFAULT_FINANCIAL_FIELDS.copy()

# Default balance sheet fields to extract from งบแสดงฐานะการเงิน (Balance Sheet)
# Can be overridden via config.yaml extraction.balance_sheet_fields
DEFAULT_BALANCE_SHEET_FIELDS = [
    'ลูกหนี้การค้าสุทธิ',                  # Trade Receivables (Net)
    'สินค้าคงเหลือ',                      # Inventory
    'สินทรัพย์หมุนเวียน',                  # Current Assets
    'ที่ดิน อาคารและอุปกรณ์',              # Property, Plant & Equipment
    'สินทรัพย์ไม่หมุนเวียน',               # Non-Current Assets
    'สินทรัพย์รวม',                       # Total Assets
    'หนี้สินหมุนเวียน',                    # Current Liabilities
    'หนี้สินไม่หมุนเวียน',                 # Non-Current Liabilities
    'หนี้สินรวม',                         # Total Liabilities
    'ส่วนของผู้ถือหุ้น',                   # Shareholders' Equity
    'หนี้สินรวมและส่วนของผู้ถือหุ้น',       # Total Liabilities & Equity
]

# Whether to include balance sheet extraction
INCLUDE_BALANCE_SHEET = True
BALANCE_SHEET_FIELDS = DEFAULT_BALANCE_SHEET_FIELDS.copy()


def load_config(config_path='config.yaml'):
    """Load configuration from YAML file.

    Args:
        config_path: Path to config file

    Returns:
        dict: Configuration dictionary, or empty dict if file not found
    """
    if not YAML_AVAILABLE:
        return {}

    if not os.path.exists(config_path):
        return {}

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            return config if config else {}
    except Exception as e:
        print(f"Warning: Could not load config file: {e}")
        return {}


def generate_default_config(output_path='config.yaml'):
    """Generate a default config.yaml file with all settings.

    Args:
        output_path: Path to write config file
    """
    default_config = '''# =============================================================================
# DBD DataWarehouse Revenue Scraper - Configuration File
# =============================================================================
# This file contains all configurable settings for the scraper.
# Command line arguments will override these settings.
#
# Usage:
#   python3 scraper_v2.py                      # Uses this config file
#   python3 scraper_v2.py --config other.yaml  # Uses different config
#   python3 scraper_v2.py --workers 4          # CLI overrides config
# =============================================================================

# -----------------------------------------------------------------------------
# Input Settings
# -----------------------------------------------------------------------------
input:
  # Path to input file (CSV, Excel .xlsx/.xls, or text file)
  file: "companies.csv"

  # Column name containing company names
  # Set to null to auto-detect or use first column
  company_column: "company_name"

  # Column name containing registration numbers (optional)
  # If provided, companies with existing reg numbers will skip search
  reg_column: null

  # Excel sheet name (only for .xlsx/.xls files)
  # Set to null to use the first sheet
  sheet: null

  # Filter to only include Thai companies (containing บริษัท or ห้างหุ้นส่วน)
  filter_thai: true

# -----------------------------------------------------------------------------
# Output Settings
# -----------------------------------------------------------------------------
output:
  # Output CSV file for successful revenue extractions
  revenue_file: "dbd_revenue_v2.csv"

  # Output CSV file for companies not found or without revenue data
  not_found_file: "dbd_not_found_v2.csv"

  # Directory for temporary batch files
  batch_dir: "batches_v2"

  # Skip backup confirmation when output files exist
  force_overwrite: false

# -----------------------------------------------------------------------------
# Search Settings
# -----------------------------------------------------------------------------
search:
  # Maximum pages to search per search term (DBD shows 10 results per page)
  # Higher = more thorough but slower
  max_pages: 20

  # Minimum similarity score (0.0 to 1.0) for fallback matching
  # Only matches >= this threshold will be accepted
  # Set to 1.0 to only accept exact matches
  similarity_threshold: 0.95

# -----------------------------------------------------------------------------
# Processing Settings
# -----------------------------------------------------------------------------
processing:
  # Number of parallel browser workers
  # More workers = faster but uses more memory and may hit rate limits
  workers: 1

  # Number of companies per batch (for progress saving)
  batch_size: 20

  # Seconds to wait between requests (to avoid rate limiting)
  delay_between_requests: 3

  # Start from Nth company (0-indexed, for resuming)
  start_index: 0

# -----------------------------------------------------------------------------
# Retry Settings
# -----------------------------------------------------------------------------
retry:
  # Maximum retries when company found but no revenue data displayed
  max_retries: 3

  # Additional seconds to wait per retry attempt
  extra_wait_per_retry: 2

# -----------------------------------------------------------------------------
# Browser Settings
# -----------------------------------------------------------------------------
browser:
  # Run browser in headless mode (no visible window)
  headless: true

  # Seconds to wait for initial page load
  page_load_wait: 10

  # Seconds to wait after clicking financial tab
  tab_click_wait: 4

  # Seconds to wait for financial table to load
  table_load_wait: 6

  # Additional wait time for data to appear
  extra_wait: 3

# -----------------------------------------------------------------------------
# Debug Settings
# -----------------------------------------------------------------------------
debug:
  # Enable debug mode (saves screenshots on errors)
  enabled: false

  # Limit processing to N companies (for testing)
  # Set to null to process all companies
  test_count: null
'''

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(default_config)

    print(f"Generated default config file: {output_path}")


def apply_config(args, config):
    """Apply config file settings to args, with CLI taking priority.

    Args:
        args: argparse.Namespace from command line
        config: dict from config file

    Returns:
        argparse.Namespace: Updated args
    """
    if not config:
        return args

    # Input settings
    input_cfg = config.get('input', {})
    if args.input == INPUT_CSV and input_cfg.get('file'):
        args.input = input_cfg['file']
    if args.column is None and input_cfg.get('company_column'):
        args.column = input_cfg['company_column']
    if args.reg_column is None and input_cfg.get('reg_column'):
        args.reg_column = input_cfg['reg_column']
    if args.sheet is None and input_cfg.get('sheet'):
        args.sheet = input_cfg['sheet']
    if not args.no_filter and input_cfg.get('filter_thai') is False:
        args.no_filter = True

    # Output settings
    output_cfg = config.get('output', {})
    if args.output == OUTPUT_CSV and output_cfg.get('revenue_file'):
        args.output = output_cfg['revenue_file']
    if args.not_found_output == NOT_FOUND_CSV and output_cfg.get('not_found_file'):
        args.not_found_output = output_cfg['not_found_file']
    if not args.force and output_cfg.get('force_overwrite'):
        args.force = True

    # Search settings
    search_cfg = config.get('search', {})
    if args.max_search_pages == 20 and search_cfg.get('max_pages'):
        args.max_search_pages = search_cfg['max_pages']
    if search_cfg.get('similarity_threshold'):
        args.similarity_threshold = search_cfg['similarity_threshold']

    # Processing settings
    proc_cfg = config.get('processing', {})
    if args.workers == 1 and proc_cfg.get('workers'):
        args.workers = proc_cfg['workers']
    if args.batch_size == BATCH_SIZE and proc_cfg.get('batch_size'):
        args.batch_size = proc_cfg['batch_size']
    if proc_cfg.get('start_index'):
        args.start = proc_cfg['start_index']

    # Retry settings
    retry_cfg = config.get('retry', {})
    if args.max_retries == 3 and retry_cfg.get('max_retries') is not None:
        args.max_retries = retry_cfg['max_retries']

    # Browser settings
    browser_cfg = config.get('browser', {})
    if not args.visible and browser_cfg.get('headless') is False:
        args.visible = True

    # Debug settings
    debug_cfg = config.get('debug', {})
    if not args.debug and debug_cfg.get('enabled'):
        args.debug = True
    if args.test is None and debug_cfg.get('test_count'):
        args.test = debug_cfg['test_count']

    return args


def setup_driver(headless=True):
    """Initialize Chrome WebDriver with anti-detection settings."""
    chrome_options = Options()

    if headless:
        # Use old-style headless (more compatible with this site)
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')

    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')

    # Anti-detection options
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except TypeError:
        driver = webdriver.Chrome(
            executable_path=ChromeDriverManager().install(),
            options=chrome_options
        )

    # Set realistic user agent
    try:
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    except:
        pass

    driver.set_page_load_timeout(60)
    return driver


def get_companies_from_file(file_path, column=None, reg_column=None, sheet=None, filter_thai=True):
    """Extract company names and optional registration numbers from various file formats.

    Supports:
    - CSV files (.csv)
    - Excel files (.xlsx, .xls)
    - Text files (.txt) - one company per line

    Args:
        file_path: Path to the input file
        column: Column name containing company names (for CSV/Excel)
                If None, tries 'company_name' or first column
        reg_column: Column name containing registration numbers (optional)
                    If provided, will skip search for companies with existing reg numbers
        sheet: Sheet name for Excel files (default: first sheet)
        filter_thai: If True, only include companies with 'จำกัด' or 'มหาชน'

    Returns:
        list: Sorted list of tuples (company_name, reg_number or None)
    """
    file_path = Path(file_path)
    companies = {}  # Use dict to store name -> reg_number mapping

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    file_ext = file_path.suffix.lower()

    if file_ext == '.csv':
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames

            # Determine name column to use
            if column and column in fieldnames:
                col_name = column
            elif 'company_name' in fieldnames:
                col_name = 'company_name'
            elif fieldnames:
                col_name = fieldnames[0]
                print(f"    Using first column for names: '{col_name}'")
            else:
                raise ValueError("CSV file has no columns")

            # Check if reg_column exists
            has_reg_col = reg_column and reg_column in fieldnames
            if reg_column and not has_reg_col:
                print(f"    Warning: Registration column '{reg_column}' not found")

            for row in reader:
                company_name = row.get(col_name, '').strip()
                reg_number = None
                if has_reg_col:
                    reg_number = row.get(reg_column, '').strip()
                    # Validate reg number format (13 digits starting with 0)
                    if reg_number and not re.match(r'^0\d{12}$', reg_number):
                        reg_number = None

                if company_name:
                    if not filter_thai or ('จำกัด' in company_name or 'มหาชน' in company_name):
                        # Keep the reg_number if we have one
                        if company_name not in companies or reg_number:
                            companies[company_name] = reg_number

    elif file_ext in ['.xlsx', '.xls']:
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas and openpyxl required for Excel files. Install with: pip install pandas openpyxl")

        df = pd.read_excel(file_path, sheet_name=sheet if sheet else 0)

        # Determine name column to use
        if column and column in df.columns:
            col_name = column
        elif 'company_name' in df.columns:
            col_name = 'company_name'
        else:
            col_name = df.columns[0]
            print(f"    Using first column for names: '{col_name}'")

        # Check if reg_column exists
        has_reg_col = reg_column and reg_column in df.columns
        if reg_column and not has_reg_col:
            print(f"    Warning: Registration column '{reg_column}' not found")

        for idx, row in df.iterrows():
            company_name = str(row[col_name]).strip() if pd.notna(row[col_name]) else ''
            reg_number = None
            if has_reg_col and pd.notna(row[reg_column]):
                reg_number = str(row[reg_column]).strip()
                # Validate reg number format (13 digits starting with 0)
                if not re.match(r'^0\d{12}$', reg_number):
                    reg_number = None

            if company_name:
                if not filter_thai or ('จำกัด' in company_name or 'มหาชน' in company_name):
                    if company_name not in companies or reg_number:
                        companies[company_name] = reg_number

    elif file_ext == '.txt':
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                company_name = line.strip()
                if company_name:
                    if not filter_thai or ('จำกัด' in company_name or 'มหาชน' in company_name):
                        companies[company_name] = None

    else:
        raise ValueError(f"Unsupported file format: {file_ext}. Use .csv, .xlsx, .xls, or .txt")

    # Convert to sorted list of tuples
    result = sorted([(name, reg) for name, reg in companies.items()])

    with_reg = sum(1 for _, reg in result if reg)
    print(f"    Loaded {len(result)} unique companies from {file_path.name}")
    if with_reg > 0:
        print(f"    {with_reg} companies already have registration numbers (will skip search)")

    return result


def get_thai_companies(input_csv):
    """Extract unique Thai company names from input CSV (legacy function)."""
    return get_companies_from_file(input_csv, column='company_name', filter_thai=True)


def accept_cookies(driver):
    """Accept cookie consent if present."""
    try:
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for btn in buttons:
            try:
                btn_text = btn.text.strip()
                if 'ยอมรับทั้งหมด' in btn_text or 'ยอมรับ' in btn_text:
                    btn.click()
                    time.sleep(1)
                    return True
            except:
                continue
        for btn in buttons:
            try:
                btn_text = btn.text.strip()
                if 'ปิด' in btn_text:
                    btn.click()
                    time.sleep(1)
                    return True
            except:
                continue
    except:
        pass
    return False


def normalize_company_name(name):
    """Normalize company name for comparison."""
    # Remove common prefixes and clean up
    normalized = name.strip()
    normalized = normalized.replace('บริษัท', '').strip()
    normalized = normalized.replace('ห้างหุ้นส่วนจำกัด', '').strip()
    normalized = normalized.replace('ห้างหุ้นส่วนสามัญ', '').strip()
    # Remove extra whitespace
    normalized = ' '.join(normalized.split())
    return normalized


def extract_company_core_name(name):
    """Extract core company name (before จำกัด/มหาชน).

    Handles both clean company names and search result lines that contain
    row numbers, registration numbers, etc.

    Special handling for ห้างหุ้นส่วนจำกัด (limited partnership) - removes
    the full prefix before extracting core name.
    """
    core = name.strip()

    # Remove row number and registration number prefix from search results
    # Pattern: "123 0123456789012 Company Name จำกัด ..."
    reg_match = re.search(r'\d+\s+(0\d{12})\s+(.+)', core)
    if reg_match:
        core = reg_match.group(2)  # Get just the company name part

    # Handle ห้างหุ้นส่วนจำกัด (limited partnership) - remove full prefix FIRST
    # Must do this before splitting on จำกัด, otherwise "ห้างหุ้นส่วนจำกัด XYZ" becomes "ห้างหุ้นส่วน"
    partnership_prefixes = [
        'ห้างหุ้นส่วนจำกัด',  # Limited partnership
        'ห้างหุ้นส่วนสามัญนิติบุคคล',  # Registered ordinary partnership
        'ห้างหุ้นส่วนสามัญ',  # Ordinary partnership
    ]
    for prefix in partnership_prefixes:
        if prefix in core:
            core = core.replace(prefix, '').strip()
            break  # Only remove one prefix

    core = core.replace('บริษัท', '').strip()

    # Extract just the core name (before จำกัด)
    # This handles "XYZ จำกัด" -> "XYZ"
    if 'จำกัด' in core:
        core = core.split('จำกัด')[0].strip()

    # Remove extra whitespace
    core = ' '.join(core.split())
    return core


# Common filler patterns to remove from company names
FILLER_PATTERNS = [
    r'\(ประเทศไทย\)', r'ประเทศไทย',
    r'\(ไทยแลนด์\)', r'ไทยแลนด์',
    r'\(Thailand\)', r'Thailand',
    r'\(เอเชีย\)', r'เอเชีย',
    r'\(Asia\)', r'Asia',
    r'อินเตอร์เนชั่นแนล', r'อินเตอร์เนชันแนล',
    r'กรุ๊ปส์', r'กรุ๊ป',
    r'โฮลดิ้งส์', r'โฮลดิ้ง',
    r'เอ็นเตอร์ไพรส์', r'เอ็นเตอร์ไพรซ์',
    r'คอร์ปอเรชั่น', r'คอร์ปอเรชัน',
]


def clean_filler_words(name):
    """Remove common filler words/patterns from company name.

    Removes Thailand-related suffixes and common business terms that
    often differ between data sources.

    Args:
        name: Company name string

    Returns:
        str: Cleaned company name
    """
    cleaned = name
    for pattern in FILLER_PATTERNS:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    # Normalize whitespace
    cleaned = ' '.join(cleaned.split())
    return cleaned


def remove_parentheses(name):
    """Remove all content inside parentheses from company name.

    Args:
        name: Company name string

    Returns:
        str: Company name with parenthetical content removed
    """
    # Remove content in parentheses: (xxx) or （xxx）
    cleaned = re.sub(r'\([^)]*\)', '', name)
    cleaned = re.sub(r'（[^）]*）', '', cleaned)  # Full-width parentheses
    # Normalize whitespace
    cleaned = ' '.join(cleaned.split())
    return cleaned


def remove_trailing_numbers(name):
    """Remove trailing numbers/years from company name.

    Handles patterns like "ABC 2020", "DEF 123", "GHI (1999)"

    Args:
        name: Company name string

    Returns:
        str: Company name with trailing numbers removed
    """
    # Remove numbers in parentheses first
    cleaned = re.sub(r'\(\d+\)', '', name)
    # Remove trailing numbers (year or other numeric suffix)
    cleaned = re.sub(r'\s+\d+\s*$', '', cleaned)
    # Normalize whitespace
    cleaned = ' '.join(cleaned.split())
    return cleaned


def calculate_similarity(name1, name2):
    """Calculate similarity score between two company names using token overlap.

    Uses token (word) overlap ratio, which works well for Thai text.

    Args:
        name1: First company name
        name2: Second company name

    Returns:
        float: Similarity score between 0.0 and 1.0
    """
    # Extract core names for comparison
    core1 = extract_company_core_name(name1)
    core2 = extract_company_core_name(name2)

    # Tokenize by splitting on whitespace
    tokens1 = set(core1.split())
    tokens2 = set(core2.split())

    if not tokens1 or not tokens2:
        return 0.0

    # Calculate Jaccard-like similarity
    common = tokens1 & tokens2
    total = tokens1 | tokens2

    if not total:
        return 0.0

    return len(common) / len(total)


def generate_search_terms(company_name):
    """Generate search term variants in priority order.

    Order:
    1. Full name (without บริษัท/ห้างหุ้นส่วน prefix)
    2. Name with จำกัด(มหาชน) (no space) if original has มหาชน
    3. Name with จำกัด only (for บริษัท) OR name without ห้างหุ้นส่วนจำกัด prefix
    4. Core name (before จำกัด)
    5. Core name without filler words (ประเทศไทย, Thailand, etc.)
    6. Core name without parentheses content
    7. Core name without trailing numbers/years
    8. Progressive word trimming: remove last word one at a time

    Returns:
        list: List of search terms to try in order
    """
    search_terms = []

    # Check if this is a partnership (ห้างหุ้นส่วน)
    is_partnership = 'ห้างหุ้นส่วน' in company_name

    # Remove บริษัท prefix
    base_name = company_name.replace('บริษัท', '').strip()
    base_name = ' '.join(base_name.split())  # Normalize whitespace

    # 1. Try full name first (without บริษัท)
    if base_name:
        search_terms.append(base_name)

    # Handle partnerships (ห้างหุ้นส่วนจำกัด, ห้างหุ้นส่วนสามัญ, etc.)
    if is_partnership:
        # 2. Try without the partnership prefix
        partnership_prefixes = [
            'ห้างหุ้นส่วนจำกัด',
            'ห้างหุ้นส่วนสามัญนิติบุคคล',
            'ห้างหุ้นส่วนสามัญ',
        ]
        name_without_prefix = base_name
        for prefix in partnership_prefixes:
            if prefix in name_without_prefix:
                name_without_prefix = name_without_prefix.replace(prefix, '').strip()
                break

        if name_without_prefix and name_without_prefix not in search_terms:
            search_terms.append(name_without_prefix)

        # 3. Try with just "ห้างหุ้นส่วน" + core name (some DBD entries use this)
        short_prefix_variant = f"ห้างหุ้นส่วน {name_without_prefix}"
        if short_prefix_variant not in search_terms:
            search_terms.append(short_prefix_variant)

    # 2-3. Handle บริษัท with มหาชน variants
    elif '(มหาชน)' in base_name or 'มหาชน' in base_name:
        # Create variant: "เสริมสุข จำกัด (มหาชน)" -> "เสริมสุข จำกัด(มหาชน)"
        no_space_variant = base_name.replace('จำกัด (มหาชน)', 'จำกัด(มหาชน)')
        no_space_variant = no_space_variant.replace('จำกัด  (มหาชน)', 'จำกัด(มหาชน)')
        if no_space_variant not in search_terms:
            search_terms.append(no_space_variant)

        # 3. Try with จำกัด only (remove มหาชน part)
        just_limited = base_name.split('(มหาชน)')[0].strip()
        just_limited = just_limited.split('มหาชน')[0].strip()
        if just_limited and just_limited not in search_terms:
            search_terms.append(just_limited)

    # 4. Core name (before จำกัด, handles both บริษัท and ห้างหุ้นส่วน)
    core_name = extract_company_core_name(company_name)
    if core_name and core_name not in search_terms:
        search_terms.append(core_name)

    # 5. Core name without filler words (ประเทศไทย, Thailand, etc.)
    cleaned_filler = clean_filler_words(core_name)
    if cleaned_filler and cleaned_filler != core_name and cleaned_filler not in search_terms:
        search_terms.append(cleaned_filler)

    # 6. Core name without parentheses content
    no_parens = remove_parentheses(core_name)
    if no_parens and no_parens != core_name and no_parens not in search_terms:
        search_terms.append(no_parens)

    # 7. Core name without trailing numbers/years
    no_numbers = remove_trailing_numbers(core_name)
    if no_numbers and no_numbers != core_name and no_numbers not in search_terms:
        search_terms.append(no_numbers)

    # 8. Progressive word trimming: remove last word one at a time
    # Use the cleanest version of core name for trimming
    # Try from: no_parens (if different) or core_name
    trimming_base = no_parens if no_parens and no_parens != core_name else core_name
    words = trimming_base.split()
    if len(words) > 1:
        for i in range(len(words) - 1, 0, -1):  # From n-1 words down to 1 word
            trimmed = ' '.join(words[:i])
            if trimmed and trimmed not in search_terms:
                search_terms.append(trimmed)

    return search_terms


def search_single_term(driver, search_term, target_name, strategy_num, max_pages=20):
    """Search with a single term and paginate through results.

    Args:
        driver: Selenium WebDriver
        search_term: The term to search for
        target_name: Original company name to match against
        strategy_num: The 1-based search strategy number being used
        max_pages: Maximum pages to search

    Returns:
        tuple: (registration_number, found_name, match_type, search_strategy) or (None, None, None, None)
               match_type is 'exact' for exact matches
               search_strategy is the 1-based index of which search term found the match
    """
    target_core = extract_company_core_name(target_name)
    target_normalized = normalize_company_name(target_name)

    search_url = f"{BASE_URL}/juristic/searchInfo?keyword={search_term}"
    driver.get(search_url)
    time.sleep(PAGE_LOAD_WAIT)

    accept_cookies(driver)

    # Check if DBD directly navigated to company detail page (single/exact match)
    current_url = driver.current_url
    if '/company/profile/' in current_url:
        print(f"      Direct navigation to company detail page")
        page_text = driver.find_element(By.TAG_NAME, "body").text

        # Extract registration number from detail page
        # Format: "เลขทะเบียนนิติบุคคล : 0107537001650"
        reg_match = re.search(r'เลขทะเบียนนิติบุคคล\s*[:\s]\s*(0\d{12})', page_text)
        if reg_match:
            reg_number = reg_match.group(1)

            # Extract company name from detail page
            # Format: "ชื่อนิติบุคคล : บริษัท ABC จำกัด"
            name_match = re.search(r'ชื่อนิติบุคคล\s*[:\s]\s*(.+?)(?:\n|$)', page_text)
            found_name = name_match.group(1).strip() if name_match else ''

            # Verify it matches target
            found_core = extract_company_core_name(found_name)
            if target_core == found_core:
                print(f"      ✓ Direct match: {found_name[:50]}")
                return reg_number, found_name, 'exact', 'direct'
            else:
                print(f"      Direct page but name mismatch: '{found_core}' != '{target_core}'")
                # Still return as it's the only result, but let caller decide
                return reg_number, found_name, 'exact', 'direct'

        print(f"      Could not extract reg number from detail page")
        return None, None, None, None

    page_text = driver.find_element(By.TAG_NAME, "body").text
    if 'ไม่พบข้อมูล' in page_text:
        print(f"      No results for this search term")
        return None, None, None, None

    # Get total pages from pagination
    total_pages = 1
    for line in page_text.split('\n'):
        match = re.search(r'หน้า\s*\d+\s*/?\s*(\d+)', line)
        if match:
            total_pages = int(match.group(1))
            break
        match = re.search(r'/\s*(\d+)', line)
        if match and int(match.group(1)) > 1:
            total_pages = int(match.group(1))
            break

    pages_to_search = min(total_pages, max_pages)
    print(f"      Found {total_pages} pages, searching up to {pages_to_search}")

    # Search through pages
    for page_num in range(1, pages_to_search + 1):
        if page_num > 1:
            try:
                page_inputs = driver.find_elements(By.CSS_SELECTOR, 'input[type="number"]')
                if page_inputs:
                    page_input = page_inputs[0]
                    page_input.clear()
                    page_input.send_keys(str(page_num))
                    page_input.send_keys('\n')
                    time.sleep(3)
                else:
                    break
            except Exception as e:
                print(f"      Error navigating to page {page_num}: {e}")
                break

        page_text = driver.find_element(By.TAG_NAME, "body").text
        lines = page_text.split('\n')

        for line in lines:
            reg_match = re.search(r'(0\d{12})', line)
            if reg_match and 'จำกัด' in line:
                reg_number = reg_match.group(1)
                found_core = extract_company_core_name(line)

                # Exact match check (highest priority)
                if target_core == found_core:
                    print(f"      ✓ Exact match on page {page_num}")
                    return reg_number, line, 'exact', strategy_num

        # Check if redirect happened during scanning (JavaScript redirect)
        current_url = driver.current_url
        if '/company/profile/' in current_url:
            print(f"      Redirect detected during page scan")
            page_text = driver.find_element(By.TAG_NAME, "body").text

            reg_match = re.search(r'เลขทะเบียนนิติบุคคล\s*[:\s]\s*(0\d{12})', page_text)
            if reg_match:
                reg_number = reg_match.group(1)
                name_match = re.search(r'ชื่อนิติบุคคล\s*[:\s]\s*(.+?)(?:\n|$)', page_text)
                found_name = name_match.group(1).strip() if name_match else ''

                found_core = extract_company_core_name(found_name)
                if target_core == found_core:
                    print(f"      ✓ Direct match (delayed redirect): {found_name[:50]}")
                    return reg_number, found_name, 'exact', 'direct'
                else:
                    print(f"      Direct page (delayed) but accepting: '{found_name[:40]}'")
                    return reg_number, found_name, 'exact', 'direct'

    return None, None, None, None


def search_and_get_reg_number(driver, company_name, max_pages=20):
    """Search for a company using multiple search strategies.

    Tries search terms in order:
    1. Full name (without บริษัท prefix)
    2. Name with จำกัด(มหาชน) (no space) if applicable
    3. Name with จำกัด only
    4. Core name (before จำกัด)

    Args:
        driver: Selenium WebDriver
        company_name: Full company name to search for
        max_pages: Maximum pages to search per term (default 20)

    Returns:
        tuple: (registration_number, found_name, match_type, search_strategy) or (None, None, None, None)
               match_type is 'exact' or 'similarity_XX%'
               search_strategy is the 1-based index of which search term found the match
    """
    search_terms = generate_search_terms(company_name)
    print(f"    Search strategies: {len(search_terms)} terms to try")

    for i, term in enumerate(search_terms):
        strategy_num = i + 1  # 1-based index
        print(f"    [{strategy_num}/{len(search_terms)}] Trying: '{term[:50]}{'...' if len(term) > 50 else ''}'")

        result = search_single_term(driver, term, company_name, strategy_num, max_pages=max_pages)
        if result[0]:  # Found a match
            print(f"    ✓ Found match with strategy #{strategy_num}")
            return result

        print(f"      No match found with this term")

    # If no match found with any term, use similarity scoring fallback
    print(f"    No exact match found, using similarity scoring fallback")

    # Try with the core name (broadest search for collecting candidates)
    core_name = extract_company_core_name(company_name)
    fallback_term = core_name.split()[0] if core_name.split() else search_terms[-1]

    search_url = f"{BASE_URL}/juristic/searchInfo?keyword={fallback_term}"
    driver.get(search_url)
    time.sleep(PAGE_LOAD_WAIT)

    page_text = driver.find_element(By.TAG_NAME, "body").text
    if 'ไม่พบข้อมูล' in page_text:
        return None, None, None, None

    # Collect all candidates from first page and score them
    candidates = []
    for line in page_text.split('\n'):
        reg_match = re.search(r'(0\d{12})', line)
        if reg_match and 'จำกัด' in line:
            reg_number = reg_match.group(1)
            similarity = calculate_similarity(company_name, line)
            candidates.append((reg_number, line, similarity))

    if not candidates:
        return None, None, None, None

    # Sort by similarity score (highest first)
    candidates.sort(key=lambda x: x[2], reverse=True)
    best_match = candidates[0]

    # Use global threshold (configurable via config file or CLI)
    if best_match[2] >= SIMILARITY_THRESHOLD:
        print(f"      ✓ Best similarity match: {best_match[2]:.1%}")
        return best_match[0], best_match[1], f'similarity_{best_match[2]:.0%}', 'fallback'
    else:
        print(f"      ✗ Best match below threshold ({best_match[2]:.1%} < {SIMILARITY_THRESHOLD:.0%}), rejecting")
        return None, None, None, None


def click_financial_tab(driver):
    """Click on the financial data tab."""
    try:
        # Find the exact text match for ข้อมูลงบการเงิน
        elements = driver.find_elements(By.XPATH, "//*[text()='ข้อมูลงบการเงิน']")
        if not elements:
            elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'ข้อมูลงบการเงิน')]")

        for elem in elements:
            try:
                text = elem.text.strip()
                if text == 'ข้อมูลงบการเงิน':
                    elem.click()
                    time.sleep(TAB_CLICK_WAIT)
                    return True
            except:
                continue
    except:
        pass
    return False


def click_income_statement(driver):
    """Click on the income statement button (งบกำไรขาดทุน)."""
    try:
        elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'งบกำไรขาดทุน')]")
        for elem in elements:
            try:
                if elem.is_displayed():
                    elem.click()
                    time.sleep(TABLE_LOAD_WAIT)
                    return True
            except:
                continue
    except:
        pass
    return False


def click_balance_sheet(driver):
    """Click on the balance sheet button (งบแสดงฐานะการเงิน)."""
    try:
        elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'งบแสดงฐานะการเงิน')]")
        for elem in elements:
            try:
                if elem.is_displayed():
                    elem.click()
                    time.sleep(TABLE_LOAD_WAIT)
                    return True
            except:
                continue
    except:
        pass
    return False


def extract_revenue(driver, registration_number):
    """Extract revenue data from company profile (legacy wrapper).

    This function is kept for backward compatibility.
    When EXTRACTION_MODE is 'revenue_only', it returns the old format.
    When EXTRACTION_MODE is 'all', it extracts all configured fields.
    """
    return extract_financial_data(driver, registration_number)


def extract_table_data(driver, fields_to_extract, table_name="table"):
    """Extract data from a financial table that's currently displayed.

    Args:
        driver: Selenium WebDriver instance
        fields_to_extract: List of field names to extract
        table_name: Name of table for logging (e.g., "Income Statement", "Balance Sheet")

    Returns:
        dict: {field_name: {year: value, ...}, ...} or empty dict if no data found
    """
    table_data = {}

    # Find the financial table with years
    tables = driver.find_elements(By.TAG_NAME, 'table')
    target_table = None

    for table in tables:
        try:
            header = table.find_element(By.TAG_NAME, 'tr')
            if re.search(r'256[0-9]', header.text):
                target_table = table
                break
        except:
            continue

    if not target_table:
        print(f"    Warning: Could not find {table_name} table with years")
        return table_data

    # Extract years from header
    header_row = target_table.find_element(By.TAG_NAME, 'tr')
    header_cells = header_row.find_elements(By.TAG_NAME, 'th')

    years = []
    for cell in header_cells:
        text = cell.text.strip()
        year_match = re.match(r'^(25[6-7][0-9])$', text)
        if year_match:
            years.append(int(year_match.group(1)))

    if not years:
        print(f"    Warning: Could not extract years from {table_name} header")
        return table_data

    print(f"    {table_name} years: {years}")

    # Get all rows from the table
    rows = target_table.find_elements(By.TAG_NAME, 'tr')

    # Find matching rows for each field
    field_rows = {}
    for row in rows:
        row_text = row.text
        for field in fields_to_extract:
            # Use 'in' for partial match since row may contain extra text
            if field in row_text:
                # Avoid duplicate matches - take first occurrence
                if field not in field_rows:
                    field_rows[field] = row
                break  # Each row matches at most one field

    if not field_rows:
        print(f"    Warning: Could not find any {table_name} field rows")
        return table_data

    print(f"    Found {len(field_rows)} {table_name} fields")

    # Extract values for each field
    for field_name, row in field_rows.items():
        cells = row.find_elements(By.TAG_NAME, 'td')

        # Mapping: For year at index i, value is at cell index (i * 2)
        # Because each year has 2 columns: value and %change
        for year_idx, year in enumerate(years):
            value_cell_idx = year_idx * 2

            if value_cell_idx < len(cells):
                cell_text = cells[value_cell_idx].text.strip()

                # Skip empty or placeholder values
                if cell_text in ['-', '', '0.00']:
                    continue

                try:
                    value = float(cell_text.replace(',', ''))
                    # Export all years (TARGET_YEARS=None) or filter if specified
                    if TARGET_YEARS is None or year in TARGET_YEARS:
                        if field_name not in table_data:
                            table_data[field_name] = {}
                        table_data[field_name][year] = value
                except ValueError:
                    continue

    return table_data


def extract_financial_data(driver, registration_number):
    """Extract financial data from company profile.

    Extracts all configured financial fields from:
    1. Income Statement (งบกำไรขาดทุน) - always extracted
    2. Balance Sheet (งบแสดงฐานะการเงิน) - optional, based on INCLUDE_BALANCE_SHEET

    All fields are extracted in a single page navigation - no additional
    HTTP requests are needed beyond the initial page load.

    Args:
        driver: Selenium WebDriver instance
        registration_number: 13-digit DBD registration number

    Returns:
        dict: When EXTRACTION_MODE is 'revenue_only':
              {year: revenue_value, ...}

              When EXTRACTION_MODE is 'all':
              {'income_statement': {field_name: {year: value}},
               'balance_sheet': {field_name: {year: value}}}  # if INCLUDE_BALANCE_SHEET

              Empty dict if no data found.
    """
    financial_data = {}

    for prefix in ['5', '7', '6', '3', '']:  # '3' is for partnerships (ห้างหุ้นส่วน)
        try:
            url = f"{BASE_URL}/company/profile/{prefix}{registration_number}"
            driver.get(url)
            time.sleep(PAGE_LOAD_WAIT)

            accept_cookies(driver)

            page_text = driver.find_element(By.TAG_NAME, "body").text
            if 'ชื่อนิติบุคคล' not in page_text and 'ข้อมูลนิติบุคคล' not in page_text:
                continue

            # Click financial tab
            if not click_financial_tab(driver):
                print("    Warning: Could not click financial tab")
                continue

            # Check if table loaded
            tables = driver.find_elements(By.TAG_NAME, 'table')
            if not tables:
                print("    Warning: No financial table found")
                continue

            # ========== EXTRACT INCOME STATEMENT ==========
            # Click income statement button
            if not click_income_statement(driver):
                print("    Warning: Could not click income statement button")
                # Continue anyway - might still have data

            # Wait for data
            time.sleep(EXTRA_WAIT)

            # Determine which fields to extract based on mode
            if EXTRACTION_MODE == 'revenue_only':
                income_fields = ['รายได้รวม']
            else:
                income_fields = FINANCIAL_FIELDS

            income_data = extract_table_data(driver, income_fields, "Income Statement")

            # Handle revenue_only mode (legacy format)
            if EXTRACTION_MODE == 'revenue_only':
                if 'รายได้รวม' in income_data:
                    financial_data = income_data['รายได้รวม']  # {year: value}
                if financial_data:
                    return financial_data
                break

            # ========== EXTRACT BALANCE SHEET (if enabled) ==========
            balance_data = {}
            if INCLUDE_BALANCE_SHEET and BALANCE_SHEET_FIELDS:
                # Click balance sheet button
                if click_balance_sheet(driver):
                    time.sleep(EXTRA_WAIT)
                    balance_data = extract_table_data(driver, BALANCE_SHEET_FIELDS, "Balance Sheet")
                else:
                    print("    Warning: Could not click balance sheet button")

            # Combine results
            if income_data or balance_data:
                if income_data:
                    financial_data['income_statement'] = income_data
                if balance_data:
                    financial_data['balance_sheet'] = balance_data
                return financial_data

            break  # Found valid profile page

        except Exception as e:
            print(f"    Error with prefix {prefix}: {e}")
            continue

    return financial_data


def save_worker_batch(worker_id, batch_num, revenue_data, not_found_data):
    """Save batch files for a specific worker."""
    if revenue_data:
        batch_file = os.path.join(BATCH_DIR, f"revenue_w{worker_id}_batch_{batch_num:03d}.csv")
        with open(batch_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # New header includes table_type and field_name columns for multi-table extraction
            writer.writerow(['company_name', 'registration_number', 'match_type', 'search_strategy', 'table_type', 'field_name', 'value', 'year'])
            for record in revenue_data:
                writer.writerow(record)
        print(f"[Worker {worker_id}] >> Saved batch {batch_num} with {len(revenue_data)} financial records")

    if not_found_data:
        batch_file = os.path.join(BATCH_DIR, f"not_found_w{worker_id}_batch_{batch_num:03d}.csv")
        with open(batch_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['company_name', 'registration_number', 'match_type', 'search_strategy', 'reason'])
            for record in not_found_data:
                writer.writerow(record)
        print(f"[Worker {worker_id}] >> Saved batch {batch_num} with {len(not_found_data)} not found records")


def combine_batches(output_csv=None, not_found_csv=None, force_overwrite=False):
    """Combine all batch files into final output files.

    Args:
        output_csv: Path for revenue output file
        not_found_csv: Path for not-found output file
        force_overwrite: If False, will backup existing files before overwriting
    """
    import glob
    from datetime import datetime

    output_csv = output_csv or OUTPUT_CSV
    not_found_csv = not_found_csv or NOT_FOUND_CSV

    # Safety check: backup existing files before overwriting
    for filepath in [output_csv, not_found_csv]:
        if os.path.exists(filepath) and not force_overwrite:
            # Get file size to check if it has content
            file_size = os.path.getsize(filepath)
            if file_size > 0:
                # Create backup with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = filepath.replace('.csv', f'_backup_{timestamp}.csv')
                print(f"\n⚠️  Existing file found: {filepath}")
                print(f"   Size: {file_size:,} bytes")
                print(f"   Creating backup: {backup_path}")
                import shutil
                shutil.copy2(filepath, backup_path)

    revenue_batches = sorted(glob.glob(os.path.join(BATCH_DIR, "revenue_*.csv")))
    if revenue_batches:
        with open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            # New header includes table_type and field_name columns for multi-table extraction
            writer.writerow(['company_name', 'registration_number', 'match_type', 'search_strategy', 'table_type', 'field_name', 'value', 'year'])

            total_records = 0
            for batch_file in revenue_batches:
                with open(batch_file, 'r', encoding='utf-8') as infile:
                    reader = csv.reader(infile)
                    next(reader)  # Skip header
                    for row in reader:
                        writer.writerow(row)
                        total_records += 1

        print(f"\nCombined {len(revenue_batches)} financial data batches into {output_csv}")
        print(f"Total financial records: {total_records}")

    not_found_batches = sorted(glob.glob(os.path.join(BATCH_DIR, "not_found_*.csv")))
    if not_found_batches:
        with open(not_found_csv, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['company_name', 'registration_number', 'match_type', 'search_strategy', 'reason'])

            total_not_found = 0
            for batch_file in not_found_batches:
                with open(batch_file, 'r', encoding='utf-8') as infile:
                    reader = csv.reader(infile)
                    next(reader)
                    for row in reader:
                        writer.writerow(row)
                        total_not_found += 1

        print(f"Combined {len(not_found_batches)} not found batches into {not_found_csv}")
        print(f"Total not found: {total_not_found}")


def save_progress(index):
    """Save progress to file."""
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(index))


def load_progress():
    """Load progress from file."""
    try:
        with open(PROGRESS_FILE, 'r') as f:
            return int(f.read().strip())
    except:
        return 0


def process_company_chunk(args_tuple):
    """Process a chunk of companies in a separate process.

    Each company in companies_chunk is a tuple: (company_name, existing_reg_number or None)
    """
    worker_id, companies_chunk, headless, debug, batch_size, max_retries, retry_extra_wait, max_search_pages = args_tuple

    # Set retry settings for this worker process
    global MAX_RETRY_NO_REVENUE, RETRY_EXTRA_WAIT
    MAX_RETRY_NO_REVENUE = max_retries
    RETRY_EXTRA_WAIT = retry_extra_wait
    search_pages = max_search_pages

    print(f"[Worker {worker_id}] Starting with {len(companies_chunk)} companies")

    driver = setup_driver(headless=headless)

    current_batch_revenue = []
    current_batch_not_found = []
    batch_num = 1
    total_revenue = 0
    total_not_found = 0

    for i, company_data in enumerate(companies_chunk):
        # Handle both tuple (name, reg) and string (legacy) formats
        if isinstance(company_data, tuple):
            company, existing_reg = company_data
        else:
            company = company_data
            existing_reg = None

        print(f"[Worker {worker_id}] [{i+1}/{len(companies_chunk)}] {company}")

        try:
            # If we already have a registration number, skip search
            if existing_reg:
                reg_number = existing_reg
                match_type = 'existing'
                search_strategy = ''
                print(f"[Worker {worker_id}]   Using existing Reg#: {reg_number}")
            else:
                reg_number, found_name, match_type, search_strategy = search_and_get_reg_number(driver, company, max_pages=search_pages)

            if not reg_number:
                print(f"[Worker {worker_id}]   Not found in search")
                current_batch_not_found.append([company, '', '', '', "No search results"])
                time.sleep(DELAY_BETWEEN_REQUESTS)
            else:
                if not existing_reg:
                    print(f"[Worker {worker_id}]   Reg#: {reg_number}")

                # Try to extract revenue with retry logic
                revenue_data = None
                retry_count = 0

                while retry_count <= MAX_RETRY_NO_REVENUE:
                    if retry_count > 0:
                        print(f"[Worker {worker_id}]   Retry {retry_count}/{MAX_RETRY_NO_REVENUE} (waiting {RETRY_EXTRA_WAIT * retry_count}s extra)...")
                        time.sleep(RETRY_EXTRA_WAIT * retry_count)  # Progressive wait

                    revenue_data = extract_revenue(driver, reg_number)

                    if revenue_data:
                        break  # Success! Exit retry loop

                    retry_count += 1

                    if retry_count <= MAX_RETRY_NO_REVENUE:
                        print(f"[Worker {worker_id}]   No revenue data, will retry...")

                if debug:
                    driver.save_screenshot(f"/tmp/debug_v2_w{worker_id}_{i}.png")

                if not revenue_data:
                    print(f"[Worker {worker_id}]   No revenue data found (after {MAX_RETRY_NO_REVENUE} retries)")
                    current_batch_not_found.append([company, reg_number, match_type or '', search_strategy or '', "No revenue data"])
                else:
                    if retry_count > 0:
                        print(f"[Worker {worker_id}]   ✓ Success on retry {retry_count}!")

                    # Handle both extraction modes
                    if EXTRACTION_MODE == 'revenue_only':
                        # Legacy format: revenue_data = {year: value}
                        for year, revenue in sorted(revenue_data.items()):
                            current_batch_revenue.append([company, reg_number, match_type or '', search_strategy or '', 'งบกำไรขาดทุน', 'รายได้รวม', revenue, year])
                            print(f"[Worker {worker_id}]     ✓ {year}: {revenue:,.2f}")
                    else:
                        # New format: revenue_data = {'income_statement': {...}, 'balance_sheet': {...}}
                        for table_type, table_data in revenue_data.items():
                            table_display = 'งบกำไรขาดทุน' if table_type == 'income_statement' else 'งบแสดงฐานะการเงิน'
                            for field_name, year_values in table_data.items():
                                for year, value in sorted(year_values.items()):
                                    current_batch_revenue.append([company, reg_number, match_type or '', search_strategy or '', table_display, field_name, value, year])
                                    print(f"[Worker {worker_id}]     ✓ [{table_display[:10]}] {field_name} {year}: {value:,.2f}")

        except WebDriverException as e:
            print(f"[Worker {worker_id}]   Browser error, reinitializing...")
            try:
                driver.quit()
            except:
                pass
            driver = setup_driver(headless=headless)
            current_batch_not_found.append([company, existing_reg or '', '', '', "Browser error"])

        except Exception as e:
            print(f"[Worker {worker_id}]   Error: {e}")
            current_batch_not_found.append([company, existing_reg or '', '', '', str(e)[:100]])

        # Save batch periodically
        companies_processed = i + 1
        if companies_processed % batch_size == 0 or companies_processed == len(companies_chunk):
            save_worker_batch(worker_id, batch_num, current_batch_revenue, current_batch_not_found)
            total_revenue += len(current_batch_revenue)
            total_not_found += len(current_batch_not_found)
            current_batch_revenue = []
            current_batch_not_found = []
            batch_num += 1

        time.sleep(DELAY_BETWEEN_REQUESTS)

    try:
        driver.quit()
    except:
        pass

    print(f"[Worker {worker_id}] Done. Revenue records: {total_revenue}, Not found: {total_not_found}")
    return worker_id, total_revenue, total_not_found


def main():
    parser = argparse.ArgumentParser(description="DBD Revenue Scraper v2 (Fixed)")

    # Config file options
    parser.add_argument('--config', type=str, default='config.yaml',
                       help='Path to config file (default: config.yaml)')
    parser.add_argument('--generate-config', action='store_true',
                       help='Generate default config.yaml and exit')

    # Input file options
    parser.add_argument('--input', '-i', type=str, default=INPUT_CSV,
                       help=f'Input file path (.csv, .xlsx, .xls, .txt). Default: {INPUT_CSV}')
    parser.add_argument('--column', '-c', type=str, default=None,
                       help='Column name containing company names (default: auto-detect)')
    parser.add_argument('--reg-column', '-r', type=str, default=None,
                       help='Column name containing registration numbers (optional, skips search if provided)')
    parser.add_argument('--sheet', '-s', type=str, default=None,
                       help='Sheet name for Excel files (default: first sheet)')
    parser.add_argument('--no-filter', action='store_true',
                       help='Include all companies, not just Thai (จำกัด/มหาชน)')

    # Output options
    parser.add_argument('--output', '-o', type=str, default=OUTPUT_CSV,
                       help=f'Output CSV file path. Default: {OUTPUT_CSV}')
    parser.add_argument('--not-found-output', type=str, default=NOT_FOUND_CSV,
                       help=f'Not found CSV file path. Default: {NOT_FOUND_CSV}')
    parser.add_argument('--force', '-f', action='store_true',
                       help='Force overwrite output files without creating backups')

    # Processing options
    parser.add_argument('--test', type=int, help='Test with N companies')
    parser.add_argument('--start', type=int, default=0, help='Start from Nth company (0-indexed)')
    parser.add_argument('--resume', action='store_true', help='Resume from last progress')
    parser.add_argument('--visible', action='store_true', help='Show browser window')
    parser.add_argument('--debug', action='store_true', help='Save debug screenshots')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help=f'Batch size (default: {BATCH_SIZE})')
    parser.add_argument('--combine-only', action='store_true', help='Only combine existing batches')
    parser.add_argument('--workers', type=int, default=1, help='Number of parallel workers (max recommended: 2)')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='Max retries for "No revenue data" (default: 3)')
    parser.add_argument('--no-retry', action='store_true', help='Disable retry for "No revenue data"')
    parser.add_argument('--max-search-pages', type=int, default=20,
                       help='Max pages to search for exact company match (default: 20)')
    parser.add_argument('--similarity-threshold', type=float, default=0.95,
                       help='Minimum similarity score for fallback matching (default: 0.95)')
    args = parser.parse_args()

    # Handle --generate-config
    if args.generate_config:
        generate_default_config('config.yaml')
        return

    # Load and apply config file
    config = load_config(args.config)
    if config:
        print(f"Loaded config from: {args.config}")
        args = apply_config(args, config)

        # Update global variables from config (these are not in args)
        global PAGE_LOAD_WAIT, TAB_CLICK_WAIT, TABLE_LOAD_WAIT, EXTRA_WAIT, DELAY_BETWEEN_REQUESTS, RETRY_EXTRA_WAIT

        browser_cfg = config.get('browser', {})
        if browser_cfg.get('page_load_wait') is not None:
            PAGE_LOAD_WAIT = browser_cfg['page_load_wait']
        if browser_cfg.get('tab_click_wait') is not None:
            TAB_CLICK_WAIT = browser_cfg['tab_click_wait']
        if browser_cfg.get('table_load_wait') is not None:
            TABLE_LOAD_WAIT = browser_cfg['table_load_wait']
        if browser_cfg.get('extra_wait') is not None:
            EXTRA_WAIT = browser_cfg['extra_wait']

        proc_cfg = config.get('processing', {})
        if proc_cfg.get('delay_between_requests') is not None:
            DELAY_BETWEEN_REQUESTS = proc_cfg['delay_between_requests']

        retry_cfg = config.get('retry', {})
        if retry_cfg.get('extra_wait_per_retry') is not None:
            RETRY_EXTRA_WAIT = retry_cfg['extra_wait_per_retry']

        # Extraction settings
        global EXTRACTION_MODE, FINANCIAL_FIELDS, INCLUDE_BALANCE_SHEET, BALANCE_SHEET_FIELDS
        extraction_cfg = config.get('extraction', {})
        if extraction_cfg.get('mode'):
            EXTRACTION_MODE = extraction_cfg['mode']
        # Support both old 'fields' key and new 'income_statement_fields' key
        if extraction_cfg.get('income_statement_fields'):
            FINANCIAL_FIELDS = extraction_cfg['income_statement_fields']
        elif extraction_cfg.get('fields'):
            FINANCIAL_FIELDS = extraction_cfg['fields']
        # Balance sheet settings
        if extraction_cfg.get('include_balance_sheet') is not None:
            INCLUDE_BALANCE_SHEET = extraction_cfg['include_balance_sheet']
        if extraction_cfg.get('balance_sheet_fields'):
            BALANCE_SHEET_FIELDS = extraction_cfg['balance_sheet_fields']

    # Update global settings
    global SIMILARITY_THRESHOLD
    SIMILARITY_THRESHOLD = args.similarity_threshold

    # Update global retry settings based on arguments
    global MAX_RETRY_NO_REVENUE
    if args.no_retry:
        MAX_RETRY_NO_REVENUE = 0
    else:
        MAX_RETRY_NO_REVENUE = args.max_retries

    print("=" * 60)
    print("DBD DataWarehouse Revenue Scraper v2 (FIXED)")
    print("=" * 60)
    print()
    print("Input configuration:")
    print(f"  - Input file: {args.input}")
    if args.column:
        print(f"  - Name column: {args.column}")
    if args.reg_column:
        print(f"  - Registration column: {args.reg_column}")
    if args.sheet:
        print(f"  - Sheet: {args.sheet}")
    print(f"  - Filter Thai companies only: {not args.no_filter}")
    print()
    print("Output configuration:")
    print(f"  - Output file: {args.output}")
    print(f"  - Not found file: {args.not_found_output}")
    print()
    print("Wait times configured:")
    print(f"  - Page load: {PAGE_LOAD_WAIT}s")
    print(f"  - Tab click: {TAB_CLICK_WAIT}s")
    print(f"  - Table load: {TABLE_LOAD_WAIT}s")
    print(f"  - Extra wait: {EXTRA_WAIT}s")
    print()
    print("Retry configuration:")
    print(f"  - Max retries for 'No revenue data': {MAX_RETRY_NO_REVENUE}")
    print(f"  - Extra wait per retry: {RETRY_EXTRA_WAIT}s")
    print()
    print("Search configuration:")
    print(f"  - Max pages to search for exact match: {args.max_search_pages}")
    print(f"  - Similarity threshold for fallback: {SIMILARITY_THRESHOLD:.0%}")
    print()
    print("Processing configuration:")
    print(f"  - Workers: {args.workers}")
    print(f"  - Batch size: {args.batch_size}")
    print(f"  - Delay between requests: {DELAY_BETWEEN_REQUESTS}s")
    print()
    print("Extraction configuration:")
    print(f"  - Mode: {EXTRACTION_MODE}")
    if EXTRACTION_MODE == 'all':
        print(f"  - Income Statement fields ({len(FINANCIAL_FIELDS)}):")
        for field in FINANCIAL_FIELDS:
            print(f"      • {field}")
        print(f"  - Include Balance Sheet: {INCLUDE_BALANCE_SHEET}")
        if INCLUDE_BALANCE_SHEET:
            print(f"  - Balance Sheet fields ({len(BALANCE_SHEET_FIELDS)}):")
            for field in BALANCE_SHEET_FIELDS:
                print(f"      • {field}")
    print()

    os.makedirs(BATCH_DIR, exist_ok=True)

    if args.combine_only:
        print("Combining existing batches...")
        combine_batches(args.output, args.not_found_output, force_overwrite=args.force)
        return

    # Load companies from input file
    print("Loading companies from input file...")
    companies = get_companies_from_file(
        args.input,
        column=args.column,
        reg_column=args.reg_column,
        sheet=args.sheet,
        filter_thai=not args.no_filter
    )
    print(f"Found {len(companies)} companies to process")

    if args.test:
        companies = companies[:args.test]
        print(f"Test mode: {args.test} companies")

    start_index = load_progress() if args.resume else 0
    if args.resume and start_index > 0:
        print(f"Resuming from #{start_index + 1}")

    batch_size = args.batch_size
    num_workers = args.workers

    print(f"Batch size: {batch_size}")
    print(f"Workers: {num_workers}")
    print(f"Target years: {'ALL' if TARGET_YEARS is None else TARGET_YEARS}")

    # Clear old files if starting fresh
    if start_index == 0:
        for f in [args.output, args.not_found_output]:
            if os.path.exists(f):
                os.remove(f)
        import glob
        for old_batch in glob.glob(os.path.join(BATCH_DIR, "*.csv")):
            os.remove(old_batch)

    success = 0
    failed = 0

    if num_workers > 1:
        print(f"\nStarting {num_workers} parallel workers...")

        companies_to_process = companies[start_index:]
        chunk_size = len(companies_to_process) // num_workers
        chunks = []
        for i in range(num_workers):
            start = i * chunk_size
            end = start + chunk_size if i < num_workers - 1 else len(companies_to_process)
            chunks.append(companies_to_process[start:end])

        worker_args = [
            (i + 1, chunk, not args.visible, args.debug, batch_size, MAX_RETRY_NO_REVENUE, RETRY_EXTRA_WAIT, args.max_search_pages)
            for i, chunk in enumerate(chunks)
        ]

        try:
            with Pool(processes=num_workers) as pool:
                results = pool.map(process_company_chunk, worker_args)

            for worker_id, total_revenue, total_not_found in results:
                success += total_revenue
                failed += total_not_found

        except KeyboardInterrupt:
            print("\n\nInterrupted by user!")

    else:
        # Single worker mode
        result = process_company_chunk(
            (1, companies[start_index:], not args.visible, args.debug, batch_size, MAX_RETRY_NO_REVENUE, RETRY_EXTRA_WAIT, args.max_search_pages)
        )
        _, success, failed = result

    print("\n" + "=" * 60)
    print("Combining all batches...")
    combine_batches(args.output, args.not_found_output, force_overwrite=args.force)

    print("\n" + "=" * 60)
    print("Completed!")
    print(f"  Revenue records: {success}")
    print(f"  Not found: {failed}")
    print(f"  Output: {args.output}")


if __name__ == "__main__":
    main()
