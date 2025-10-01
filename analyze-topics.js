/*
  Topic Analysis Script
  
  Analyzes all topics from the last 10 years to determine feasibility
  of creating simplified "keyword" categories.
  
  Usage: node analyze-topics.js
*/

const sqlite3 = require('sqlite3');

function analyzeTopics() {
  const db = new sqlite3.Database('ep_data.db');
  
  console.log('🔍 Analyzing topics from the last 10 years...\n');
  
  // Get all unique topics with their frequency
  const query = `
    SELECT 
      topic,
      COUNT(*) as frequency,
      MIN(activity_date) as first_seen,
      MAX(activity_date) as last_seen
    FROM individual_speeches i
    JOIN sittings s ON i.sitting_id = s.id
    WHERE i.topic IS NOT NULL 
      AND s.activity_date >= date('now', '-10 years')
    GROUP BY topic
    ORDER BY frequency DESC
  `;
  
  db.all(query, (err, rows) => {
    if (err) {
      console.error('❌ Error:', err);
      db.close();
      return;
    }
    
    console.log(`📊 Found ${rows.length} unique topics in the last 10 years\n`);
    
    // Show top 20 most frequent topics
    console.log('🏆 TOP 20 MOST FREQUENT TOPICS:');
    console.log('='.repeat(80));
    console.log('Frequency | Topic');
    console.log('-'.repeat(80));
    
    rows.slice(0, 20).forEach((row, index) => {
      const freq = row.frequency.toString().padStart(8);
      const topic = row.topic.length > 60 ? row.topic.substring(0, 60) + '...' : row.topic;
      console.log(`${freq} | ${topic}`);
    });
    
    // Analyze topic patterns
    console.log('\n📈 TOPIC ANALYSIS:');
    console.log('='.repeat(50));
    
    const totalSpeeches = rows.reduce((sum, row) => sum + row.frequency, 0);
    const top10Topics = rows.slice(0, 10);
    const top10Count = top10Topics.reduce((sum, row) => sum + row.frequency, 0);
    const top20Topics = rows.slice(0, 20);
    const top20Count = top20Topics.reduce((sum, row) => sum + row.frequency, 0);
    
    console.log(`Total speeches with topics: ${totalSpeeches.toLocaleString()}`);
    console.log(`Top 10 topics cover: ${top10Count.toLocaleString()} speeches (${((top10Count/totalSpeeches)*100).toFixed(1)}%)`);
    console.log(`Top 20 topics cover: ${top20Count.toLocaleString()} speeches (${((top20Count/totalSpeeches)*100).toFixed(1)}%)`);
    
    // Look for common patterns
    console.log('\n🔍 COMMON TOPIC PATTERNS:');
    console.log('='.repeat(50));
    
    const patterns = {
      'Opening/Closing': 0,
      'Debate': 0,
      'Vote': 0,
      'Question': 0,
      'Statement': 0,
      'Report': 0,
      'Resolution': 0,
      'Agenda': 0,
      'Other': 0
    };
    
    rows.forEach(row => {
      const topic = row.topic.toLowerCase();
      if (topic.includes('opening') || topic.includes('closing') || topic.includes('adjournment')) {
        patterns['Opening/Closing'] += row.frequency;
      } else if (topic.includes('debate')) {
        patterns['Debate'] += row.frequency;
      } else if (topic.includes('vote') || topic.includes('voting')) {
        patterns['Vote'] += row.frequency;
      } else if (topic.includes('question')) {
        patterns['Question'] += row.frequency;
      } else if (topic.includes('statement')) {
        patterns['Statement'] += row.frequency;
      } else if (topic.includes('report')) {
        patterns['Report'] += row.frequency;
      } else if (topic.includes('resolution')) {
        patterns['Resolution'] += row.frequency;
      } else if (topic.includes('agenda')) {
        patterns['Agenda'] += row.frequency;
      } else {
        patterns['Other'] += row.frequency;
      }
    });
    
    Object.entries(patterns)
      .sort((a, b) => b[1] - a[1])
      .forEach(([pattern, count]) => {
        const percentage = ((count / totalSpeeches) * 100).toFixed(1);
        console.log(`${pattern.padEnd(15)}: ${count.toLocaleString().padStart(8)} speeches (${percentage}%)`);
      });
    
    // Show some examples of very specific topics
    console.log('\n🎯 EXAMPLES OF VERY SPECIFIC TOPICS:');
    console.log('='.repeat(80));
    console.log('Frequency | Topic');
    console.log('-'.repeat(80));
    
    const specificTopics = rows.filter(row => row.frequency <= 5).slice(0, 15);
    specificTopics.forEach(row => {
      const freq = row.frequency.toString().padStart(8);
      const topic = row.topic.length > 60 ? row.topic.substring(0, 60) + '...' : row.topic;
      console.log(`${freq} | ${topic}`);
    });
    
    // Recommendations
    console.log('\n💡 RECOMMENDATIONS:');
    console.log('='.repeat(50));
    
    if (rows.length > 1000) {
      console.log('⚠️  High topic diversity: Creating keywords might be complex');
      console.log('   Consider grouping by common patterns instead of exact topics');
    } else if (rows.length > 500) {
      console.log('⚖️  Medium topic diversity: Keywords feasible with good categorization');
    } else {
      console.log('✅ Low topic diversity: Keywords should be straightforward');
    }
    
    if (patterns['Other'] / totalSpeeches > 0.5) {
      console.log('⚠️  Many unique topics: Consider broader keyword categories');
    }
    
    console.log('\n🎯 SUGGESTED KEYWORD CATEGORIES:');
    console.log('Based on the patterns above, consider these keyword categories:');
    Object.entries(patterns)
      .filter(([pattern, count]) => count > 0)
      .sort((a, b) => b[1] - a[1])
      .forEach(([pattern, count]) => {
        console.log(`- ${pattern}`);
      });
    
    db.close();
  });
}

// Run the analysis
analyzeTopics();
