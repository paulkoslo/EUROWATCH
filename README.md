# EUROWATCH - European Parliament Speech Analysis Platform

A comprehensive web application and analysis platform for European Parliament speeches, featuring real-time data collection, AI-powered topic classification, and advanced analytics for political research.

## 🎯 Project Overview

EUROWATCH is a sophisticated platform that collects, processes, and analyzes European Parliament speeches from multiple sources. It combines web scraping, natural language processing, and machine learning to provide insights into parliamentary proceedings, political discourse, and policy trends.

### Key Features

- **🌐 Real-time Data Collection**: Automated scraping of EU Parliament speeches and metadata
- **🤖 AI Topic Classification**: GPT-5-nano powered speech categorization with 50+ topic categories
- **📊 Interactive Web Dashboard**: Multi-tab interface for MEPs, speeches, and analytics
- **🔍 Advanced Search & Filtering**: Filter by date, speaker, political group, language, and topic
- **📈 Comprehensive Analytics**: Statistical analysis, trend detection, and data export
- **🌍 Multi-language Support**: Automatic language detection for 24 EU languages
- **💾 SQLite Database**: Efficient local storage with full-text search capabilities

## 🏗️ Architecture

### Frontend (Web Interface)
- **Main Dashboard** (`public/index.html`): MEP overview with charts and filtering
- **Speech Browser** (`public/speech.html`): Individual speech analysis with AI features
- **Newest Speeches** (`public/newest.html`): Latest parliamentary proceedings
- **MEP Details** (`public/mep-details.html`): Individual MEP profiles and speech history

### Backend (Node.js Server)
- **Express Server** (`server.js`): RESTful API with 20+ endpoints
- **Database Layer**: SQLite3 with optimized queries and full-text search
- **Data Processing**: Automated scraping, parsing, and normalization

### Analysis Scripts
- **Data Collection**: Automated speech fetching and parsing
- **AI Classification**: GPT-5-nano topic classification with rate limiting
- **Analytics**: Comprehensive statistical analysis and reporting
- **Export Tools**: CSV/JSON export for research purposes

## 📦 Installation & Setup

### Prerequisites
- Node.js 16+ 
- npm or yarn
- OpenAI API key (for AI classification features)

### Quick Setup
```bash
# Clone the repository
git clone <repository-url>
cd EUROWATCH

# Install dependencies
npm install

# Set up environment variables
echo "OPENAI_API_KEY=your-api-key-here" > .env

# Start the web server
npm start
```

The web application will be available at `http://localhost:3000`

## 🔄 Complete Data Generation Workflow

Since the database and analysis files are not included in the repository (due to size), you'll need to generate your own dataset. Here's the complete step-by-step workflow:

### Phase 1: Initial Data Collection

#### Step 1: Fetch Raw Parliamentary Data
```bash
# Option A: Fetch recent data (recommended for testing)
node perfect-fetch-parse.js 2024-01-01

# Option B: Fetch historical data by year range
node perfect-fetch-parse-yearly.js 2019-01-01 2024-12-31

# Option C: Quick update (incremental)
node update.js
```

**What this does:**
- Fetches speech metadata from EU Parliament API
- Downloads HTML content from parliament websites
- Creates `sittings` table with raw parliamentary session data
- Parses HTML to extract individual speeches
- Creates `individual_speeches` table with speaker, content, and metadata

### Phase 2: Data Processing & Enhancement

#### Step 2: Parse Individual Speeches (if needed)
```bash
# Enhanced parsing with better political group detection
node reparse-with-parentheses.js

# Or reparse all sittings with improved parsing
node reparse-all-with-parentheses.js
```

**What this does:**
- Improves speech parsing with better pattern recognition
- Handles complex speaker formats: "Name (Party), Role. – Speech"
- Extracts political groups from parentheses and role descriptions
- Updates `individual_speeches` with better speaker/group detection

#### Step 3: Language Detection
```bash
node detect-language.js
```

**What this does:**
- Adds `language` column to `individual_speeches`
- Uses CLD3 (neural) + franc (n-gram) for high-accuracy detection
- Supports all 24 EU official languages
- Handles multilingual content with majority voting

#### Step 4: Political Group Normalization
```bash
# Dry run to see what will be normalized
node group-normalizer.js

# Apply normalization to database
node group-normalizer.js --apply
```

**What this does:**
- Adds columns: `political_group_raw`, `political_group_std`, `political_group_kind`, `political_group_reason`
- Normalizes 400+ political group variations to 11 canonical groups
- Detects institutional roles vs. political groups
- Achieves 99.99% classification accuracy

#### Step 5: Topic Mapping (Original Topics)
```bash
# Map topics for a specific date
node map-topics-for-sitting.js --date 2024-01-15 --apply

# Map topics for all dates in database
node map-topics-for-sitting.js --all --apply
```

**What this does:**
- Fetches HTML agenda headers from parliament websites
- Extracts canonical topic titles from session agendas
- Maps topics to individual speeches using content matching
- Adds `topic` column with original parliamentary topics

### Phase 3: AI Enhancement

#### Step 6: AI Topic Classification
```bash
# Set up OpenAI API key first
echo "OPENAI_API_KEY=your-api-key-here" > .env

# Test classification on small sample
node test-gpt-classification.js

# Classify recent sessions (recommended)
node classify-sessions.js 5

# Or classify specific number of speeches
node classify-speeches-production.js 1000
```

**What this does:**
- Uses GPT-5-nano-2025-08-07 for advanced topic classification
- Applies 50+ topic categories (procedural, institutional, policy domains)
- Adds columns: `classified_topic`, `topic_classified_by`, `topic_classified_at`, `topic_classification_cost`
- Implements rate limiting for OpenAI Tier 2 (5,000 RPM, 2M TPM)
- Real-time progress tracking with cost estimation

### Phase 4: Quality Control & Analysis

#### Step 7: Data Quality Checks
```bash
# Check for and remove duplicates
node check-duplicates.js

# Or quick cleanup without prompts
node check-duplicates.js --quick

# Analyze topic distribution
node analyze-topics.js

# Comprehensive data analysis
node comprehensive-analysis.js
```

**What this does:**
- Identifies and removes duplicate sittings and speeches
- Analyzes topic patterns and distribution
- Provides comprehensive dataset statistics
- Validates data quality and completeness

#### Step 8: Research Data Export
```bash
# Export thesis-ready datasets
node export-thesis-data.js

# Deep dive analysis for academic research
node thesis-analysis-deep-dive.js
```

**What this does:**
- Exports 7 different CSV datasets optimized for research
- Creates Brexit analysis dataset with timeline markers
- Generates speaker statistics and political group summaries
- Produces topic modeling ready datasets
- Creates comprehensive codebook and metadata

### Phase 5: Web Interface

#### Step 9: Start Web Application
```bash
npm start
```

**What this provides:**
- Interactive dashboard at `http://localhost:3000`
- MEP overview with charts and filtering
- Speech browser with AI-classified topics
- Individual speech analysis with metadata
- Real-time search and filtering capabilities

## 📊 Expected Results

After completing the full workflow, you'll have:

- **Database**: SQLite database with 100,000+ speeches
- **Tables**: `sittings`, `individual_speeches`, `meps`
- **Languages**: 24 EU languages automatically detected
- **Political Groups**: 11 standardized groups from 400+ variations
- **Topics**: Both original parliamentary topics and AI-classified topics
- **Time Range**: Configurable (typically 2015-2025)
- **Export Data**: 7 research-ready CSV files with comprehensive metadata

## ⚠️ Important Notes

### Resource Requirements
- **Time**: Initial setup takes 2-6 hours depending on date range
- **Storage**: Database can grow to 500MB-2GB depending on scope
- **API Costs**: GPT-5-nano classification costs ~$0.05-0.10 per 1000 speeches
- **Rate Limits**: OpenAI Tier 2 limits automatically handled

### Recommended Workflow Order
1. **Start Small**: Use `perfect-fetch-parse.js 2024-01-01` for testing
2. **Validate**: Check data quality with `comprehensive-analysis.js`
3. **Enhance**: Run language detection and group normalization
4. **Classify**: Use AI classification on subset first
5. **Scale Up**: Expand to full historical data once workflow is validated

## 🚀 Core Functionality

### 1. Data Collection & Processing

#### Main Data Collection Scripts
- **`perfect-fetch-parse.js`**: Primary data collection script
  - Fetches speeches from EU Parliament API
  - Parses HTML content using Cheerio
  - Handles multiple session formats and languages
  - Implements robust error handling and retry logic

- **`perfect-fetch-parse-yearly.js`**: Batch processing for historical data
  - Processes multiple years of parliamentary data
  - Optimized for large-scale data collection
  - Progress tracking and resume capability

- **`update.js`**: Incremental updates
  - Fetches only new speeches since last update
  - Lightweight script for regular maintenance

#### Data Processing Features
- **Language Detection** (`detect-language.js`): Automatic detection of 24 EU languages
- **Political Group Normalization** (`group-normalizer.js`): Standardizes 400+ group variations
- **Content Parsing**: Extracts speech text from HTML with multiple fallback methods
- **Metadata Extraction**: Speaker info, dates, session details, and procedural context

### 2. AI-Powered Topic Classification

#### GPT-5-nano Integration
- **Model**: `gpt-5-nano-2025-08-07` with 400k token context window
- **Rate Limiting**: Tier 2 compliance (5,000 RPM, 2M TPM)
- **Cost Optimization**: ~$0.05/1M input tokens, $0.40/1M output tokens

#### Classification Scripts
- **`classify-speeches-production.js`**: Production-ready classification
  - Real-time progress bar with current topic display
  - Automatic rate limiting and error handling
  - Database integration with metadata tracking
  - Cost tracking and estimation

- **`classify-sessions.js`**: Session-based processing
  - Processes multiple recent sessions
  - Session-specific progress tracking
  - Web interface integration

- **`test-gpt-classification.js`**: Testing and validation
  - Small-scale testing without database writes
  - Cost estimation and model validation

#### Topic Categories (50+ Categories)
- **Procedural**: Opening/Closing Sessions, Order of Business, Voting Procedures
- **Institutional**: Commission Work Programme, European Council, State of the Union
- **Policy Domains**: Economic Affairs, Trade & Competition, Agriculture & Fisheries, Environment & Climate, Energy, Transport, Digital Affairs, Health & Consumer Protection, Employment & Social Affairs, Justice & Home Affairs, Education & Culture, Regional Development
- **External Relations**: Foreign Affairs, Security & Defence, Development Cooperation, Enlargement
- **Legislative**: Reports, Resolutions, Statements

### 3. Web Interface

#### Main Dashboard (`public/index.html`)
- **MEPs Tab**: 
  - Interactive charts (bar chart by country, pie chart by political group)
  - Advanced filtering (status, country, political group)
  - Paginated table with search functionality
  - Real-time statistics

- **Parliament Sittings Tab**:
  - Session overview with speech counts
  - Date-based filtering and sorting
  - Speech preview functionality
  - AI-classified topic display

#### Speech Analysis (`public/speech.html`)
- **Individual Speech View**: Full speech content with metadata
- **AI Speaker Finder**: Identifies speakers in complex parliamentary exchanges
- **AI Summary**: Generates summaries of long speeches
- **Topic Classification Display**: Shows both original and AI-classified topics

#### Additional Pages
- **Newest Speeches** (`public/newest.html`): Latest parliamentary activity
- **MEP Details** (`public/mep-details.html`): Individual MEP profiles and statistics

### 4. Analytics & Research Tools

#### Comprehensive Analysis (`comprehensive-analysis.js`)
- **Dataset Overview**: Total speeches, date ranges, volume statistics
- **Topic Analysis**: Topic distribution and trends over time
- **Language Distribution**: Multi-language speech analysis
- **Political Group Analysis**: Group participation and speech patterns
- **Speaker Statistics**: Most active speakers and participation rates
- **Temporal Trends**: Monthly/yearly speech patterns
- **Data Quality Metrics**: Content length, completeness, and accuracy

#### Thesis Research Tools (`thesis-analysis-deep-dive.js`)
- **Longitudinal Analysis**: Multi-year trend analysis
- **Political Group Evolution**: Changes in group participation over time
- **Topic Evolution**: How topics change across parliamentary terms
- **Cross-Group Analysis**: Comparative analysis between political groups
- **Research Export**: Structured data for academic research

#### Export Utilities (`export-thesis-data.js`)
- **CSV Export**: Structured data for statistical analysis
- **JSON Export**: Machine-readable format for further processing
- **Filtered Exports**: Date range, topic, or group-specific exports
- **Research-Ready Format**: Optimized for academic and policy research

### 5. Data Management

#### Database Schema
- **`sittings`**: Parliamentary session metadata
- **`individual_speeches`**: Speech content, metadata, and AI classifications
- **`meps`**: Member of Parliament information
- **Full-text search**: Optimized for content searching

#### Data Quality Features
- **Duplicate Detection** (`check-duplicates.js`): Identifies and handles duplicate speeches
- **Content Validation**: Ensures speech content quality and completeness
- **Metadata Verification**: Validates speaker, date, and session information
- **Error Logging**: Comprehensive error tracking and reporting

## 🔧 API Endpoints

### Core Data Endpoints
- `GET /api/meps` - Fetch all MEPs with optional filtering
- `GET /api/speeches` - Fetch speeches with pagination and filtering
- `GET /api/sittings` - Fetch parliamentary sessions
- `GET /api/speech-preview` - Get speech preview by date

### Analysis Endpoints
- `GET /api/stats` - Overall statistics and metrics
- `GET /api/topics` - Topic distribution and analysis
- `GET /api/languages` - Language distribution
- `GET /api/groups` - Political group statistics

### Search Endpoints
- `GET /api/search` - Full-text search across speeches
- `GET /api/search/speakers` - Search by speaker name
- `GET /api/search/topics` - Search by topic classification

## 📊 Usage Examples

### Data Collection
```bash
# Fetch speeches from a specific date
node perfect-fetch-parse.js 2024-01-01

# Update with latest speeches
node update.js

# Process multiple years
node perfect-fetch-parse-yearly.js
```

### AI Classification
```bash
# Classify recent speeches (production)
node classify-speeches-production.js

# Classify specific sessions
node classify-sessions.js 5

# Test classification (no database writes)
node test-gpt-classification.js
```

### Analysis
```bash
# Comprehensive analysis
node comprehensive-analysis.js

# Thesis research analysis
node thesis-analysis-deep-dive.js

# Export data for research
node export-thesis-data.js
```

### Data Management
```bash
# Detect languages
node detect-language.js

# Check for duplicates
node check-duplicates.js

# Analyze topics
node analyze-topics.js
```

## 🎨 Web Interface Features

### Interactive Charts
- **Country Distribution**: Bar chart showing MEPs by country
- **Political Group Distribution**: Pie chart of political affiliations
- **Speech Volume Trends**: Time-series charts of speech activity
- **Topic Distribution**: Visual representation of AI-classified topics

### Advanced Filtering
- **Date Range**: Filter speeches by specific date ranges
- **Political Groups**: Filter by political affiliation
- **Languages**: Filter by speech language
- **Topics**: Filter by AI-classified topics
- **Speakers**: Filter by individual MEPs

### Real-time Features
- **Live Updates**: Automatic refresh of latest data
- **Progress Tracking**: Real-time progress bars for long operations
- **Error Handling**: User-friendly error messages and recovery
- **Responsive Design**: Mobile-friendly interface

## 🔬 Research Applications

### Academic Research
- **Political Science**: Analysis of parliamentary discourse and voting patterns
- **Linguistics**: Multi-language speech analysis and language evolution
- **Policy Studies**: Topic evolution and policy focus analysis
- **Comparative Politics**: Cross-country and cross-group analysis

### Policy Analysis
- **Trend Detection**: Identify emerging policy topics and concerns
- **Stakeholder Analysis**: Understand different political group positions
- **Temporal Analysis**: Track policy evolution over time
- **Impact Assessment**: Measure policy discussion intensity

### Data Journalism
- **Fact Checking**: Verify political statements and claims
- **Trend Reporting**: Identify and report on parliamentary trends
- **Comparative Analysis**: Compare positions across political groups
- **Historical Context**: Provide historical perspective on current issues

## 🛠️ Technical Specifications

### Performance
- **Database**: SQLite3 with optimized indexes and full-text search
- **Rate Limiting**: OpenAI API compliance with automatic backoff
- **Caching**: Intelligent caching for improved performance
- **Batch Processing**: Efficient processing of large datasets

### Scalability
- **Modular Architecture**: Easy to extend and modify
- **API-First Design**: RESTful endpoints for external integration
- **Database Optimization**: Efficient queries and indexing
- **Error Recovery**: Robust error handling and retry mechanisms

### Security
- **Environment Variables**: Secure API key management
- **Input Validation**: Comprehensive input sanitization
- **Rate Limiting**: Protection against abuse
- **Error Handling**: Secure error messages without information leakage

## 📈 Future Enhancements

### Planned Features
- **Real-time Notifications**: Alerts for new speeches and topics
- **Advanced Analytics**: Machine learning for trend prediction
- **API Integration**: External data source integration
- **Mobile App**: Native mobile application
- **Collaborative Features**: Multi-user research capabilities

### Research Extensions
- **Sentiment Analysis**: Emotional tone analysis of speeches
- **Network Analysis**: Speaker interaction and influence mapping
- **Predictive Modeling**: Topic and policy trend prediction
- **Comparative Analysis**: Cross-parliamentary comparisons

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

### Code Style
- Follow existing code patterns
- Add comments for complex logic
- Use meaningful variable names
- Include error handling

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **European Parliament**: For providing open data access
- **OpenAI**: For GPT-5-nano model access
- **Node.js Community**: For excellent libraries and tools
- **Research Community**: For feedback and suggestions

## 📞 Support

For questions, issues, or contributions:
- Create an issue in the repository
- Contact the development team
- Check the documentation and examples

---

**EUROWATCH** - Empowering political research through data and AI