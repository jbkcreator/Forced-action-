# Distressed Property Intelligence Platform

An AI-powered web scraping framework for collecting and analyzing distressed property data from Hillsborough County, Florida. This platform automates the extraction of foreclosure auctions, tax delinquencies, liens, violations, permits, and probate records using intelligent browser automation.

## 🎯 Features

- **Foreclosure Auction Scraping**: Automated extraction of auction records from RealForeclose calendar
- **Tax Delinquency Tracking**: Collection of properties with delinquent tax records
- **Lien Information Gathering**: Automated lien data extraction and processing
- **Violation Records**: Code violation and compliance data collection
- **Permit Tracking**: Construction and permit activity monitoring
- **Probate Property Data**: Estate property information extraction
- **Master Parcel Database**: Bulk download and management of county parcel records
- **Absentee Owner Filtering**: Identification of non-owner-occupied properties

## 🏗️ Architecture

```
distressed_property_intelligence/
├── config/                      # Configuration management
│   ├── settings.py             # Pydantic-based settings
│   ├── constants.py            # Application constants
│   ├── logging.yaml            # Logging configuration
│   └── prompts/                # YAML-based AI prompts
│       ├── foreclosure_prompts.yaml
│       ├── lien_prompts.yaml
│       ├── permit_prompts.yaml
│       ├── tax_delinquent_prompts.yaml
│       └── violation_prompts.yaml
├── src/
│   ├── core/
│   │   └── ftp_client.py       # Bulk download client (HTTP/FTP/HTTPS)
│   ├── scrappers/              # Specialized scraping engines
│   │   ├── deliquencies/       # Tax delinquency scraper
│   │   ├── foreclosures/       # Foreclosure auction scraper
│   │   ├── liens/              # Lien data scraper
│   │   ├── master/             # Parcel master data & filtering
│   │   ├── permit/             # Permit data scraper
│   │   ├── probate/            # Probate property scraper
│   │   └── violation/          # Code violation scraper
│   └── utils/
│       ├── logger.py           # Centralized logging
│       └── prompt_loader.py    # YAML prompt management
└── data/                        # Data storage (not in repo)
    ├── raw/                    # Raw scraped data
    ├── processed/              # Cleaned & transformed data
    └── reference/              # Reference datasets
```

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Anthropic API Key (Claude Sonnet 4.5)
- Firecrawl API Key (optional, for enhanced web scraping)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd distressed_property_intelligence
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the root directory:
```env
ANTHROPIC_API_KEY=your_anthropic_api_key_here
FIRECRAWL_API_KEY=your_firecrawl_api_key_here
```

### Configuration

The application uses environment-driven configuration via Pydantic. All settings are defined in `config/settings.py` and loaded from the `.env` file.

AI task prompts are managed in YAML files under `config/prompts/`, allowing for easy modification without code changes.

## 📊 Usage

### Running Individual Scrapers

Each scraper engine can be run independently:

```bash
# Foreclosure auction scraper
python -m src.scrappers.foreclosures.foreclosure_engine

# Tax delinquency scraper
python -m src.scrappers.deliquencies.tax_delinquent_engine

# Lien data scraper
python -m src.scrappers.liens.lien_engine

# Permit scraper
python -m src.scrappers.permit.permit_engine

# Violation scraper
python -m src.scrappers.violation.violation_engine

# Probate scraper
python -m src.scrappers.probate.probate_engine
```

### Master Parcel Data

Download and process the county master parcel spreadsheet:

```bash
python -m src.scrappers.master.master_engine
```

## 🤖 Technology Stack

- **AI Engine**: Claude Sonnet 4.5 (via Anthropic API)
- **Browser Automation**: browser_use library
- **Configuration**: Pydantic Settings
- **Logging**: Python logging with YAML configuration
- **HTTP Client**: requests library with streaming support
- **Data Format**: CSV, JSON, Excel (XLS/XLSX)

## 🔧 Key Components

### FTP/HTTP Client
The `BulkDownloader` class in `src/core/ftp_client.py` provides standardized streaming downloads for large files with progress logging and error handling.

### Prompt Management
The `PromptLoader` utility in `src/utils/prompt_loader.py` enables centralized management of AI task prompts in YAML format, improving maintainability and allowing non-developers to modify prompts.

### Logging
Centralized logging configuration via `config/logging.yaml` with structured logging support through `src/utils/logger.py`.

## 📁 Data Output

Scraped data is organized in the `data/` directory:

- `data/raw/`: Raw scraped data organized by source
- `data/processed/`: Cleaned and transformed datasets
- `data/reference/`: Reference data (e.g., master parcel spreadsheet)

## ⚠️ Legal & Ethical Considerations

This tool is designed for legitimate real estate research and analysis purposes. Users must:

- Comply with all applicable laws and regulations
- Respect website terms of service
- Use rate limiting and respectful scraping practices
- Ensure data usage complies with privacy laws

## 🤝 Contributing

Contributions are welcome! Please ensure:

- Code follows PEP 8 style guidelines
- New scrapers follow the established engine pattern
- Prompts are externalized to YAML files
- Comprehensive logging is implemented
- Error handling is robust

## 📝 License

[Add your license information here]

## 🔗 Data Sources

- Hillsborough County Property Appraiser
- RealForeclose Auction Calendar
- [Add other data sources as applicable]

## 📧 Contact

[Add your contact information here]

---

**Note**: This framework is specifically configured for Hillsborough County, Florida. Adapting it for other counties will require modifications to URLs, data structures, and prompts.
