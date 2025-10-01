/*
  Comprehensive Speech Data Analysis
  
  Provides a complete overview of all speech data including:
  - Dataset overview (dates, volume, etc.)
  - Topic analysis and patterns
  - Language distribution
  - Political group analysis
  - Speaker statistics
  - Temporal trends
  - Data quality metrics
  
  Usage: node comprehensive-analysis.js
*/

const sqlite3 = require('sqlite3');

class DataAnalyzer {
  constructor() {
    this.db = new sqlite3.Database('ep_data.db');
    this.results = {};
  }

  // Utility function to format numbers with commas
  formatNumber(num) {
    return num.toLocaleString();
  }

  // Utility function to format percentages
  formatPercent(value, total) {
    return ((value / total) * 100).toFixed(1);
  }

  // Print section header
  printHeader(title, char = '=') {
    console.log('\n' + char.repeat(80));
    console.log(`🔍 ${title.toUpperCase()}`);
    console.log(char.repeat(80));
  }

  // Print subsection header
  printSubHeader(title) {
    console.log(`\n📊 ${title}`);
    console.log('-'.repeat(60));
  }

  // Dataset Overview
  async getDatasetOverview() {
    return new Promise((resolve, reject) => {
      const queries = [
        // Basic counts
        `SELECT COUNT(*) as total_speeches FROM individual_speeches`,
        `SELECT COUNT(DISTINCT sitting_id) as total_sittings FROM individual_speeches`,
        `SELECT COUNT(DISTINCT speaker_name) as unique_speakers FROM individual_speeches WHERE speaker_name IS NOT NULL`,
        `SELECT COUNT(*) as speeches_with_topics FROM individual_speeches WHERE topic IS NOT NULL`,
        `SELECT COUNT(*) as speeches_with_language FROM individual_speeches WHERE language IS NOT NULL`,
        `SELECT COUNT(*) as speeches_with_political_group FROM individual_speeches WHERE political_group IS NOT NULL`,
        
        // Date range
        `SELECT 
          MIN(s.activity_date) as earliest_date,
          MAX(s.activity_date) as latest_date,
          COUNT(DISTINCT strftime('%Y', s.activity_date)) as years_covered
        FROM individual_speeches i 
        JOIN sittings s ON i.sitting_id = s.id 
        WHERE s.activity_date IS NOT NULL`,
        
        // Content statistics
        `SELECT 
          AVG(length(speech_content)) as avg_speech_length,
          MIN(length(speech_content)) as min_speech_length,
          MAX(length(speech_content)) as max_speech_length
        FROM individual_speeches 
        WHERE speech_content IS NOT NULL AND length(speech_content) > 0`
      ];

      let results = {};
      let completed = 0;

      queries.forEach((query, index) => {
        this.db.get(query, (err, row) => {
          if (err) {
            reject(err);
            return;
          }
          results[index] = row;
          completed++;
          
          if (completed === queries.length) {
            resolve(results);
          }
        });
      });
    });
  }

  // Topic Analysis
  async getTopicAnalysis() {
    return new Promise((resolve, reject) => {
      const query = `
        SELECT 
          topic,
          COUNT(*) as frequency,
          COUNT(DISTINCT speaker_name) as unique_speakers,
          MIN(s.activity_date) as first_seen,
          MAX(s.activity_date) as last_seen
        FROM individual_speeches i
        JOIN sittings s ON i.sitting_id = s.id
        WHERE i.topic IS NOT NULL 
        GROUP BY topic
        ORDER BY frequency DESC
      `;
      
      this.db.all(query, (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Language Analysis
  async getLanguageAnalysis() {
    return new Promise((resolve, reject) => {
      const query = `
        SELECT 
          COALESCE(language, 'Unknown') as language,
          COUNT(*) as frequency,
          COUNT(DISTINCT speaker_name) as unique_speakers,
          COUNT(DISTINCT sitting_id) as unique_sittings
        FROM individual_speeches
        GROUP BY language
        ORDER BY frequency DESC
      `;
      
      this.db.all(query, (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Political Group Analysis
  async getPoliticalGroupAnalysis() {
    return new Promise((resolve, reject) => {
      const query = `
        SELECT 
          COALESCE(political_group, 'Unknown') as political_group,
          COUNT(*) as frequency,
          COUNT(DISTINCT speaker_name) as unique_speakers,
          AVG(length(speech_content)) as avg_speech_length
        FROM individual_speeches
        WHERE political_group IS NOT NULL
        GROUP BY political_group
        ORDER BY frequency DESC
      `;
      
      this.db.all(query, (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Speaker Analysis
  async getSpeakerAnalysis() {
    return new Promise((resolve, reject) => {
      const queries = [
        // Top speakers by speech count
        `SELECT 
          speaker_name,
          political_group,
          COUNT(*) as speech_count,
          AVG(length(speech_content)) as avg_speech_length
        FROM individual_speeches 
        WHERE speaker_name IS NOT NULL
        GROUP BY speaker_name, political_group
        ORDER BY speech_count DESC
        LIMIT 20`,
        
        // Speaker activity distribution
        `SELECT 
          CASE 
            WHEN speech_count = 1 THEN '1 speech'
            WHEN speech_count BETWEEN 2 AND 5 THEN '2-5 speeches'
            WHEN speech_count BETWEEN 6 AND 20 THEN '6-20 speeches'
            WHEN speech_count BETWEEN 21 AND 50 THEN '21-50 speeches'
            WHEN speech_count BETWEEN 51 AND 100 THEN '51-100 speeches'
            ELSE '100+ speeches'
          END as activity_level,
          COUNT(*) as speaker_count
        FROM (
          SELECT speaker_name, COUNT(*) as speech_count
          FROM individual_speeches 
          WHERE speaker_name IS NOT NULL
          GROUP BY speaker_name
        )
        GROUP BY activity_level
        ORDER BY MIN(speech_count)`
      ];

      let results = {};
      let completed = 0;

      queries.forEach((query, index) => {
        this.db.all(query, (err, rows) => {
          if (err) {
            reject(err);
            return;
          }
          results[index] = rows;
          completed++;
          
          if (completed === queries.length) {
            resolve(results);
          }
        });
      });
    });
  }

  // Temporal Analysis
  async getTemporalAnalysis() {
    return new Promise((resolve, reject) => {
      const queries = [
        // Speeches by year
        `SELECT 
          strftime('%Y', s.activity_date) as year,
          COUNT(*) as speech_count,
          COUNT(DISTINCT i.speaker_name) as unique_speakers,
          COUNT(DISTINCT i.sitting_id) as unique_sittings
        FROM individual_speeches i
        JOIN sittings s ON i.sitting_id = s.id
        WHERE s.activity_date IS NOT NULL
        GROUP BY strftime('%Y', s.activity_date)
        ORDER BY year`,
        
        // Speeches by month (recent year)
        `SELECT 
          strftime('%Y-%m', s.activity_date) as month,
          COUNT(*) as speech_count
        FROM individual_speeches i
        JOIN sittings s ON i.sitting_id = s.id
        WHERE s.activity_date >= date('now', '-2 years')
        GROUP BY strftime('%Y-%m', s.activity_date)
        ORDER BY month DESC
        LIMIT 24`
      ];

      let results = {};
      let completed = 0;

      queries.forEach((query, index) => {
        this.db.all(query, (err, rows) => {
          if (err) {
            reject(err);
            return;
          }
          results[index] = rows;
          completed++;
          
          if (completed === queries.length) {
            resolve(results);
          }
        });
      });
    });
  }

  // Data Quality Analysis
  async getDataQuality() {
    return new Promise((resolve, reject) => {
      const query = `
        SELECT 
          'Total Records' as metric,
          COUNT(*) as count,
          100.0 as percentage
        FROM individual_speeches
        UNION ALL
        SELECT 
          'With Speaker Name' as metric,
          COUNT(*) as count,
          ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM individual_speeches), 1) as percentage
        FROM individual_speeches 
        WHERE speaker_name IS NOT NULL
        UNION ALL
        SELECT 
          'With Political Group' as metric,
          COUNT(*) as count,
          ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM individual_speeches), 1) as percentage
        FROM individual_speeches 
        WHERE political_group IS NOT NULL
        UNION ALL
        SELECT 
          'With Topic' as metric,
          COUNT(*) as count,
          ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM individual_speeches), 1) as percentage
        FROM individual_speeches 
        WHERE topic IS NOT NULL
        UNION ALL
        SELECT 
          'With Language' as metric,
          COUNT(*) as count,
          ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM individual_speeches), 1) as percentage
        FROM individual_speeches 
        WHERE language IS NOT NULL
        UNION ALL
        SELECT 
          'With Content' as metric,
          COUNT(*) as count,
          ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM individual_speeches), 1) as percentage
        FROM individual_speeches 
        WHERE speech_content IS NOT NULL AND length(speech_content) > 0
      `;
      
      this.db.all(query, (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Main analysis function
  async runCompleteAnalysis() {
    console.log('🚀 EUROWATCH COMPREHENSIVE SPEECH DATA ANALYSIS');
    console.log('='.repeat(80));
    console.log('Starting comprehensive analysis...\n');

    try {
      // Dataset Overview
      this.printHeader('DATASET OVERVIEW');
      const overview = await this.getDatasetOverview();
      
      console.log(`📈 Total Speeches: ${this.formatNumber(overview[0].total_speeches)}`);
      console.log(`🏛️  Total Sittings: ${this.formatNumber(overview[1].total_sittings)}`);
      console.log(`👥 Unique Speakers: ${this.formatNumber(overview[2].unique_speakers)}`);
      console.log(`📅 Date Range: ${overview[6].earliest_date} to ${overview[6].latest_date}`);
      console.log(`⏱️  Years Covered: ${overview[6].years_covered} years`);
      console.log(`📝 Avg Speech Length: ${Math.round(overview[7].avg_speech_length)} characters`);
      
      console.log('\n🔢 Data Completeness:');
      console.log(`   Speeches with Topics: ${this.formatNumber(overview[3].speeches_with_topics)} (${this.formatPercent(overview[3].speeches_with_topics, overview[0].total_speeches)}%)`);
      console.log(`   Speeches with Language: ${this.formatNumber(overview[4].speeches_with_language)} (${this.formatPercent(overview[4].speeches_with_language, overview[0].total_speeches)}%)`);
      console.log(`   Speeches with Political Group: ${this.formatNumber(overview[5].speeches_with_political_group)} (${this.formatPercent(overview[5].speeches_with_political_group, overview[0].total_speeches)}%)`);

      // Topic Analysis
      this.printHeader('TOPIC ANALYSIS');
      const topics = await this.getTopicAnalysis();
      
      console.log(`📊 Total Unique Topics: ${this.formatNumber(topics.length)}`);
      
      this.printSubHeader('Top 15 Most Frequent Topics');
      console.log('Frequency | Speakers | Topic');
      console.log('-'.repeat(80));
      topics.slice(0, 15).forEach(topic => {
        const freq = this.formatNumber(topic.frequency).padStart(8);
        const speakers = topic.unique_speakers.toString().padStart(8);
        const topicText = topic.topic.length > 45 ? topic.topic.substring(0, 45) + '...' : topic.topic;
        console.log(`${freq} | ${speakers} | ${topicText}`);
      });

      // Topic patterns analysis
      const patterns = {
        'Opening/Closing': 0, 'Debate': 0, 'Vote': 0, 'Question': 0,
        'Statement': 0, 'Report': 0, 'Resolution': 0, 'Agenda': 0, 'Other': 0
      };
      
      const totalTopicSpeeches = topics.reduce((sum, topic) => sum + topic.frequency, 0);
      topics.forEach(topic => {
        const topicLower = topic.topic.toLowerCase();
        if (topicLower.includes('opening') || topicLower.includes('closing') || topicLower.includes('adjournment')) {
          patterns['Opening/Closing'] += topic.frequency;
        } else if (topicLower.includes('debate')) {
          patterns['Debate'] += topic.frequency;
        } else if (topicLower.includes('vote') || topicLower.includes('voting')) {
          patterns['Vote'] += topic.frequency;
        } else if (topicLower.includes('question')) {
          patterns['Question'] += topic.frequency;
        } else if (topicLower.includes('statement')) {
          patterns['Statement'] += topic.frequency;
        } else if (topicLower.includes('report')) {
          patterns['Report'] += topic.frequency;
        } else if (topicLower.includes('resolution')) {
          patterns['Resolution'] += topic.frequency;
        } else if (topicLower.includes('agenda')) {
          patterns['Agenda'] += topic.frequency;
        } else {
          patterns['Other'] += topic.frequency;
        }
      });

      this.printSubHeader('Topic Categories');
      Object.entries(patterns)
        .sort((a, b) => b[1] - a[1])
        .forEach(([pattern, count]) => {
          const percentage = this.formatPercent(count, totalTopicSpeeches);
          console.log(`${pattern.padEnd(15)}: ${this.formatNumber(count).padStart(8)} speeches (${percentage}%)`);
        });

      // Language Analysis
      this.printHeader('LANGUAGE ANALYSIS');
      const languages = await this.getLanguageAnalysis();
      
      console.log(`🌍 Languages Detected: ${languages.length}`);
      
      this.printSubHeader('Language Distribution');
      console.log('Language | Speeches | Speakers | Sittings | Percentage');
      console.log('-'.repeat(70));
      const totalSpeeches = overview[0].total_speeches;
      languages.forEach(lang => {
        const langCode = lang.language.padEnd(8);
        const speeches = this.formatNumber(lang.frequency).padStart(8);
        const speakers = lang.unique_speakers.toString().padStart(8);
        const sittings = lang.unique_sittings.toString().padStart(8);
        const percentage = this.formatPercent(lang.frequency, totalSpeeches).padStart(6) + '%';
        console.log(`${langCode} | ${speeches} | ${speakers} | ${sittings} | ${percentage}`);
      });

      // Political Group Analysis
      this.printHeader('POLITICAL GROUP ANALYSIS');
      const groups = await getPoliticalGroupAnalysis();
      
      console.log(`🏛️  Political Groups: ${groups.length}`);
      
      this.printSubHeader('Political Group Distribution');
      console.log('Group | Speeches | Speakers | Avg Length | Percentage');
      console.log('-'.repeat(80));
      groups.forEach(group => {
        const groupName = (group.political_group.length > 20 ? 
          group.political_group.substring(0, 20) + '...' : group.political_group).padEnd(23);
        const speeches = this.formatNumber(group.frequency).padStart(8);
        const speakers = group.unique_speakers.toString().padStart(8);
        const avgLen = Math.round(group.avg_speech_length).toString().padStart(8);
        const percentage = this.formatPercent(group.frequency, totalSpeeches).padStart(6) + '%';
        console.log(`${groupName} | ${speeches} | ${speakers} | ${avgLen} | ${percentage}`);
      });

      // Speaker Analysis
      this.printHeader('SPEAKER ANALYSIS');
      const speakers = await this.getSpeakerAnalysis();
      
      this.printSubHeader('Most Active Speakers (Top 15)');
      console.log('Speaker | Group | Speeches | Avg Length');
      console.log('-'.repeat(80));
      speakers[0].slice(0, 15).forEach(speaker => {
        const name = (speaker.speaker_name.length > 25 ? 
          speaker.speaker_name.substring(0, 25) + '...' : speaker.speaker_name).padEnd(28);
        const group = (speaker.political_group || 'Unknown').substring(0, 15).padEnd(18);
        const speeches = speaker.speech_count.toString().padStart(8);
        const avgLen = Math.round(speaker.avg_speech_length || 0).toString().padStart(8);
        console.log(`${name} | ${group} | ${speeches} | ${avgLen}`);
      });

      this.printSubHeader('Speaker Activity Distribution');
      speakers[1].forEach(level => {
        const activity = level.activity_level.padEnd(15);
        const count = this.formatNumber(level.speaker_count).padStart(8);
        console.log(`${activity}: ${count} speakers`);
      });

      // Temporal Analysis
      this.printHeader('TEMPORAL ANALYSIS');
      const temporal = await this.getTemporalAnalysis();
      
      this.printSubHeader('Speeches by Year');
      console.log('Year | Speeches | Speakers | Sittings');
      console.log('-'.repeat(50));
      temporal[0].forEach(year => {
        const yearStr = year.year.padEnd(4);
        const speeches = this.formatNumber(year.speech_count).padStart(8);
        const speakers = year.unique_speakers.toString().padStart(8);
        const sittings = year.unique_sittings.toString().padStart(8);
        console.log(`${yearStr} | ${speeches} | ${speakers} | ${sittings}`);
      });

      this.printSubHeader('Recent Activity (Last 24 Months)');
      console.log('Month | Speeches');
      console.log('-'.repeat(25));
      temporal[1].forEach(month => {
        const monthStr = month.month.padEnd(7);
        const speeches = this.formatNumber(month.speech_count).padStart(8);
        console.log(`${monthStr} | ${speeches}`);
      });

      // Data Quality
      this.printHeader('DATA QUALITY METRICS');
      const quality = await this.getDataQuality();
      
      console.log('Metric | Count | Completeness');
      console.log('-'.repeat(50));
      quality.forEach(metric => {
        const metricName = metric.metric.padEnd(20);
        const count = this.formatNumber(metric.count).padStart(10);
        const percentage = metric.percentage.toString().padStart(6) + '%';
        console.log(`${metricName} | ${count} | ${percentage}`);
      });

      // Summary and Recommendations
      this.printHeader('SUMMARY & RECOMMENDATIONS', '=');
      
      console.log('🎯 KEY INSIGHTS:');
      console.log(`   • Dataset contains ${this.formatNumber(overview[0].total_speeches)} speeches from ${overview[6].years_covered} years`);
      console.log(`   • ${languages.length} languages detected, with ${languages[0].language} being dominant`);
      console.log(`   • ${groups.length} political groups represented`);
      console.log(`   • ${this.formatNumber(topics.length)} unique topics (high diversity)`);
      console.log(`   • Data spans from ${overview[6].earliest_date} to ${overview[6].latest_date}`);
      
      console.log('\n💡 RECOMMENDATIONS:');
      if (topics.length > 1000) {
        console.log('   • High topic diversity suggests need for topic categorization/clustering');
      }
      if (overview[3].speeches_with_topics / overview[0].total_speeches < 0.8) {
        console.log('   • Consider topic extraction for speeches without topics');
      }
      if (languages.filter(l => l.language !== 'EN').length > 5) {
        console.log('   • Multi-language support important for comprehensive analysis');
      }
      console.log('   • Rich dataset suitable for trend analysis, sentiment analysis, and political research');

      console.log('\n✅ Analysis Complete!');
      console.log('='.repeat(80));

    } catch (error) {
      console.error('❌ Analysis Error:', error);
    } finally {
      this.db.close();
    }
  }
}

// Helper function for political group analysis (needed to be outside class for scope)
function getPoliticalGroupAnalysis() {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database('ep_data.db');
    const query = `
      SELECT 
        COALESCE(political_group, 'Unknown') as political_group,
        COUNT(*) as frequency,
        COUNT(DISTINCT speaker_name) as unique_speakers,
        AVG(length(speech_content)) as avg_speech_length
      FROM individual_speeches
      WHERE political_group IS NOT NULL
      GROUP BY political_group
      ORDER BY frequency DESC
    `;
    
    db.all(query, (err, rows) => {
      db.close();
      if (err) {
        reject(err);
        return;
      }
      resolve(rows);
    });
  });
}

// Run the analysis
const analyzer = new DataAnalyzer();
analyzer.runCompleteAnalysis();
