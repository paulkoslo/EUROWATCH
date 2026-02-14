#!/usr/bin/env node

/*
  Production Speech Classification Script
  
  Optimized for GPT-5-nano with Tier 2 rate limits:
  - 5,000 RPM (requests per minute)
  - 2,000,000 TPM (tokens per minute)
  
  Features:
  - Real-time progress bar with current topic
  - Rate limiting to stay within Tier 2 limits
  - Batch processing for efficiency
  - Error handling and retry logic
  - Accurate cost tracking
  
  Usage: node classify-speeches-production.js [limit]
*/

// Load environment variables from .env file
require('dotenv').config();

const sqlite3 = require('sqlite3').verbose();
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');
const cliProgress = require('cli-progress');
const ora = require('ora');

// Configuration
const { DB_PATH: DB_FILE } = require('../../core/db');
const PROMPT_FILE = path.join(__dirname, 'gpt-topic-classification-prompt.md');
const MODEL = 'gpt-5-nano-2025-08-07';

// Tier 2 Rate Limits
const MAX_RPM = 5000; // Requests per minute
const MAX_TPM = 2000000; // Tokens per minute
const BATCH_SIZE = 10; // Process in batches for efficiency
const REQUEST_DELAY = 60 / MAX_RPM * 1000; // Delay between requests in ms

// GPT-5-nano Pricing (from the pricing table)
const INPUT_COST_PER_1M = 0.05; // $0.05 per 1M input tokens
const OUTPUT_COST_PER_1M = 0.40; // $0.40 per 1M output tokens

class ProductionClassifier {
  constructor(saveToDatabase = true) {
    this.db = new sqlite3.Database(DB_FILE);
    this.openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY
    });
    this.systemPrompt = '';
    this.results = [];
    this.progressBar = null;
    this.spinner = null;
    this.saveToDatabase = saveToDatabase;
    
    // Rate limiting tracking
    this.requestCount = 0;
    this.tokenCount = 0;
    this.lastMinute = Math.floor(Date.now() / 60000);
    
    // Cost tracking
    this.totalInputTokens = 0;
    this.totalOutputTokens = 0;
    this.totalCost = 0;
    this.totalRequests = 0;
  }

  // Load the classification prompt
  loadPrompt() {
    try {
      this.systemPrompt = fs.readFileSync(PROMPT_FILE, 'utf8');
      console.log('✅ Loaded classification prompt');
    } catch (error) {
      console.error('❌ Error loading prompt file:', error.message);
      process.exit(1);
    }
  }

  // Rate limiting to stay within Tier 2 limits
  async rateLimit() {
    const currentMinute = Math.floor(Date.now() / 60000);
    
    // Reset counters if we're in a new minute
    if (currentMinute !== this.lastMinute) {
      this.requestCount = 0;
      this.tokenCount = 0;
      this.lastMinute = currentMinute;
    }
    
    // Check if we're approaching limits
    if (this.requestCount >= MAX_RPM * 0.9) { // Use 90% of limit for safety
      const waitTime = 60000 - (Date.now() % 60000);
      console.log(`\n⏳ Rate limit approaching, waiting ${Math.round(waitTime/1000)}s...`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
      this.requestCount = 0;
      this.tokenCount = 0;
    }
    
    // Add small delay between requests
    await new Promise(resolve => setTimeout(resolve, REQUEST_DELAY));
  }

  // Get speeches to classify
  async getSpeeches(limit = null) {
    return new Promise((resolve, reject) => {
      let query = `
        SELECT 
          i.id,
          i.speaker_name,
          i.political_group,
          i.language,
          i.speech_content,
          i.topic as existing_topic,
          s.activity_date,
          s.label as sitting_label
        FROM individual_speeches i
        JOIN sittings s ON i.sitting_id = s.id
        WHERE i.speech_content IS NOT NULL 
        AND length(i.speech_content) > 50
        ORDER BY s.activity_date DESC, i.speech_order, i.id
      `;
      
      if (limit) {
        query += ` LIMIT ${limit}`;
      }

      this.db.all(query, (err, rows) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(rows);
      });
    });
  }

  // Format speech for API input
  formatSpeechInput(speech) {
    return [
      `Speaker: ${speech.speaker_name || 'Unknown'}`,
      `Political Group: ${speech.political_group || 'Unknown'}`,
      `Language: ${speech.language || 'EN'}`,
      `Speech:`,
      '```',
      speech.speech_content,
      '```'
    ].join('\n');
  }

  // Classify a single speech with retry logic
  async classifySpeech(speech, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        await this.rateLimit();
        
        const userInput = this.formatSpeechInput(speech);
        
        const response = await this.openai.chat.completions.create({
          model: MODEL,
          messages: [
            { role: 'system', content: this.systemPrompt },
            { role: 'user', content: userInput }
          ]
        });

        const classification = response.choices[0]?.message?.content?.trim() || 'Unknown';
        
        // Track usage
        this.requestCount++;
        this.totalRequests++;
        
        const inputTokens = response.usage?.prompt_tokens || 0;
        const outputTokens = response.usage?.completion_tokens || 0;
        const reasoningTokens = response.usage?.completion_tokens_details?.reasoning_tokens || 0;
        
        this.tokenCount += inputTokens + outputTokens;
        this.totalInputTokens += inputTokens;
        this.totalOutputTokens += outputTokens;
        
        const inputCost = (inputTokens / 1000000) * INPUT_COST_PER_1M;
        const outputCost = (outputTokens / 1000000) * OUTPUT_COST_PER_1M;
        this.totalCost += inputCost + outputCost;

        return {
          id: speech.id,
          speaker: speech.speaker_name,
          group: speech.political_group,
          language: speech.language,
          existingTopic: speech.existing_topic,
          classifiedTopic: classification,
          inputTokens,
          outputTokens,
          reasoningTokens,
          cost: inputCost + outputCost,
          speechLength: speech.speech_content.length,
          attempt
        };

      } catch (error) {
        console.error(`❌ Attempt ${attempt} failed for speech ${speech.id}:`, error.message);
        
        if (attempt === retries) {
          return {
            id: speech.id,
            speaker: speech.speaker_name,
            group: speech.political_group,
            language: speech.language,
            existingTopic: speech.existing_topic,
            classifiedTopic: 'ERROR',
            inputTokens: 0,
            outputTokens: 0,
            reasoningTokens: 0,
            cost: 0,
            speechLength: speech.speech_content.length,
            error: error.message,
            attempt
          };
        }
        
        // Exponential backoff for retries
        await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
      }
    }
  }

  // Update progress bar with current topic
  updateProgress(current, total, currentTopic, currentSpeaker) {
    if (!this.progressBar) {
      this.progressBar = new cliProgress.SingleBar({
        format: '🚀 Classification Progress |{bar}| {percentage}% | {value}/{total} | Current: {currentTopic} | {currentSpeaker} | Cost: ${cost}',
        barCompleteChar: '\u2588',
        barIncompleteChar: '\u2591',
        hideCursor: true
      });
      this.progressBar.start(total, 0, {
        currentTopic: 'Starting...',
        currentSpeaker: '',
        cost: '0.00'
      });
    }
    
    this.progressBar.update(current, {
      currentTopic: currentTopic || 'Processing...',
      currentSpeaker: currentSpeaker || '',
      cost: this.totalCost.toFixed(4)
    });
  }

  // Process speeches in batches
  async processBatch(speeches, startIndex) {
    const batch = speeches.slice(startIndex, startIndex + BATCH_SIZE);
    const promises = batch.map(speech => this.classifySpeech(speech));
    
    const results = await Promise.all(promises);
    
    // Update progress for each result
    results.forEach((result, index) => {
      const globalIndex = startIndex + index;
      this.updateProgress(
        globalIndex + 1, 
        speeches.length, 
        result.classifiedTopic,
        result.speaker
      );
    });
    
    return results;
  }

  // Save results to database
  async saveResultsToDatabase() {
    console.log('\n💾 Saving classified topics to database...');
    
    return new Promise((resolve, reject) => {
      // First, ensure the new columns exist
      this.db.exec(`
        ALTER TABLE individual_speeches ADD COLUMN classified_topic TEXT;
        ALTER TABLE individual_speeches ADD COLUMN topic_classified_by TEXT;
        ALTER TABLE individual_speeches ADD COLUMN topic_classified_at INTEGER;
        ALTER TABLE individual_speeches ADD COLUMN topic_classification_cost REAL;
      `, (err) => {
        // Ignore errors (columns might already exist)
      });
      
      const stmt = this.db.prepare(`
        UPDATE individual_speeches 
        SET classified_topic = ?, 
            topic_classified_by = 'gpt-5-nano-2025-08-07',
            topic_classified_at = strftime('%s', 'now'),
            topic_classification_cost = ?
        WHERE id = ?
      `);
      
      let completed = 0;
      let errors = 0;
      
      this.results.forEach(result => {
        stmt.run([result.classifiedTopic, result.cost, result.id], (err) => {
          if (err) {
            console.error(`❌ Error saving speech ${result.id}:`, err.message);
            errors++;
          }
          completed++;
          
          if (completed === this.results.length) {
            stmt.finalize();
            console.log(`✅ Saved ${completed - errors} topics to database`);
            if (errors > 0) {
              console.log(`⚠️  ${errors} errors occurred during save`);
            }
            resolve();
          }
        });
      });
    });
  }

  // Print final results
  printResults(speeches) {
    this.progressBar?.stop();
    
    console.log('\n' + '='.repeat(80));
    console.log('📊 CLASSIFICATION RESULTS SUMMARY');
    console.log('='.repeat(80));

    // Basic stats
    console.log(`\n📈 Processing Statistics:`);
    console.log(`   Total speeches processed: ${this.results.length}`);
    console.log(`   Successful classifications: ${this.results.filter(r => r.classifiedTopic !== 'ERROR').length}`);
    console.log(`   Failed classifications: ${this.results.filter(r => r.classifiedTopic === 'ERROR').length}`);

    // Token usage
    console.log(`\n🔢 Token Usage:`);
    console.log(`   Total input tokens: ${this.totalInputTokens.toLocaleString()}`);
    console.log(`   Total output tokens: ${this.totalOutputTokens.toLocaleString()}`);
    console.log(`   Total reasoning tokens: ${this.results.reduce((sum, r) => sum + (r.reasoningTokens || 0), 0).toLocaleString()}`);
    console.log(`   Average input tokens per speech: ${Math.round(this.totalInputTokens / this.totalRequests)}`);
    console.log(`   Average output tokens per speech: ${Math.round(this.totalOutputTokens / this.totalRequests)}`);

    // Cost breakdown
    console.log(`\n💰 Cost Analysis:`);
    console.log(`   Total cost: $${this.totalCost.toFixed(4)}`);
    console.log(`   Cost per speech: $${(this.totalCost / this.totalRequests).toFixed(6)}`);
    console.log(`   Input cost: $${((this.totalInputTokens / 1000000) * INPUT_COST_PER_1M).toFixed(4)}`);
    console.log(`   Output cost: $${((this.totalOutputTokens / 1000000) * OUTPUT_COST_PER_1M).toFixed(4)}`);

    // Extrapolation for full dataset
    const avgInputTokens = this.totalInputTokens / this.totalRequests;
    const avgOutputTokens = this.totalOutputTokens / this.totalRequests;
    const avgCostPerSpeech = this.totalCost / this.totalRequests;
    const fullDatasetSize = 160000;
    
    console.log(`\n📊 Full Dataset Projection (160,000 speeches):`);
    console.log(`   Estimated total input tokens: ${(avgInputTokens * fullDatasetSize).toLocaleString()}`);
    console.log(`   Estimated total output tokens: ${(avgOutputTokens * fullDatasetSize).toLocaleString()}`);
    console.log(`   Estimated total cost: $${(avgCostPerSpeech * fullDatasetSize).toFixed(2)}`);

    // Topic distribution
    console.log(`\n🏷️  Topic Distribution:`);
    const topicCounts = {};
    this.results.forEach(r => {
      if (r.classifiedTopic !== 'ERROR') {
        topicCounts[r.classifiedTopic] = (topicCounts[r.classifiedTopic] || 0) + 1;
      }
    });

    Object.entries(topicCounts)
      .sort((a, b) => b[1] - a[1])
      .forEach(([topic, count]) => {
        const percentage = ((count / this.results.length) * 100).toFixed(1);
        console.log(`   ${topic}: ${count} speeches (${percentage}%)`);
      });

    // Rate limiting stats
    console.log(`\n⚡ Rate Limiting:`);
    console.log(`   Average requests per minute: ${(this.totalRequests / ((Date.now() - this.startTime) / 60000)).toFixed(1)}`);
    console.log(`   Average tokens per minute: ${(this.tokenCount / ((Date.now() - this.startTime) / 60000)).toLocaleString()}`);
    console.log(`   Tier 2 limits: ${MAX_RPM} RPM, ${MAX_TPM.toLocaleString()} TPM`);
  }

  // Main execution
  async run(limit = null) {
    this.startTime = Date.now();
    
    console.log('🚀 EUROWATCH Production Speech Classification');
    console.log('=============================================\n');

    // Check API key
    if (!process.env.OPENAI_API_KEY) {
      console.error('❌ Error: OPENAI_API_KEY environment variable not set');
      process.exit(1);
    }

    try {
      // Load prompt
      this.loadPrompt();

      // Get speeches
      console.log('📥 Fetching speeches from database...');
      const speeches = await this.getSpeeches(limit);
      console.log(`✅ Found ${speeches.length} speeches to classify`);

      if (speeches.length === 0) {
        console.log('❌ No speeches found. Exiting.');
        return;
      }

      // Process speeches in batches
      console.log(`\n🤖 Starting classification with ${BATCH_SIZE} concurrent requests...`);
      console.log(`⚡ Rate limits: ${MAX_RPM} RPM, ${MAX_TPM.toLocaleString()} TPM\n`);

      for (let i = 0; i < speeches.length; i += BATCH_SIZE) {
        const batchResults = await this.processBatch(speeches, i);
        this.results.push(...batchResults);
      }

      // Save results to database (if enabled)
      if (this.saveToDatabase) {
        await this.saveResultsToDatabase();
      } else {
        console.log('\n⚠️  Database saving disabled (test mode)');
      }

      // Print results
      this.printResults(speeches);

      console.log('\n✅ Classification completed successfully!');
      console.log(`⏱️  Total time: ${((Date.now() - this.startTime) / 1000 / 60).toFixed(1)} minutes`);

    } catch (error) {
      console.error('❌ Classification failed:', error);
    } finally {
      this.db.close();
    }
  }
}

// Parse command line arguments
const args = process.argv.slice(2);
const limit = args[0] ? parseInt(args[0]) : null;
const saveToDatabase = !args.includes('--no-save');

// Run the classifier
const classifier = new ProductionClassifier(saveToDatabase);
classifier.run(limit).catch(console.error);
