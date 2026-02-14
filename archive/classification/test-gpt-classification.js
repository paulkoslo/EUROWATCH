#!/usr/bin/env node

/*
  GPT-5-nano Topic Classification Test Script
  
  Tests the topic classification prompt on speeches from the most recent sitting.
  Provides cost estimates and token usage tracking.
  
  Usage: 
    1. Set OPENAI_API_KEY environment variable
    2. Run: node test-gpt-classification.js
  
  Requirements:
    - OpenAI API key with access to gpt-5-nano
    - npm install openai (already added to package.json)
*/

// Load environment variables from .env file
require('dotenv').config();

const sqlite3 = require('sqlite3').verbose();
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');

// Configuration
const { DB_PATH: DB_FILE } = require('../../core/db');
const PROMPT_FILE = path.join(__dirname, 'gpt-topic-classification-prompt.md');
const MODEL = 'gpt-5-nano-2025-08-07';
const MAX_TOKENS = 8; // Just enough for a category name
const TEMPERATURE = 0; // Deterministic output

// Cost tracking
let totalInputTokens = 0;
let totalOutputTokens = 0;
let totalRequests = 0;
let totalCost = 0;

// Cost estimates (verify these on your OpenAI pricing page)
const INPUT_COST_PER_1M = 0.10; // $0.10 per 1M input tokens
const OUTPUT_COST_PER_1M = 0.80; // $0.80 per 1M output tokens

class TopicClassifier {
  constructor() {
    this.db = new sqlite3.Database(DB_FILE);
    this.openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY
    });
    this.systemPrompt = '';
    this.results = [];
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

  // Get speeches from the most recent sitting
  async getRecentSpeeches() {
    return new Promise((resolve, reject) => {
      const query = `
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
        WHERE s.activity_date = (
          SELECT MAX(activity_date) 
          FROM sittings 
          WHERE activity_date IS NOT NULL
        )
        AND i.speech_content IS NOT NULL 
        AND length(i.speech_content) > 50
        ORDER BY i.speech_order, i.id
        LIMIT 20
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

  // Classify a single speech
  async classifySpeech(speech) {
    try {
      const userInput = this.formatSpeechInput(speech);
      
      // Debug: Log the first speech to see what we're sending
      if (speech.id === 575688) {
        console.log('\n🔍 DEBUG - First speech input:');
        console.log('System prompt length:', this.systemPrompt.length);
        console.log('User input preview:', userInput.substring(0, 200) + '...');
        console.log('Full system prompt:', this.systemPrompt);
      }
      
      const response = await this.openai.chat.completions.create({
        model: MODEL,
        messages: [
          { role: 'system', content: this.systemPrompt },
          { role: 'user', content: userInput }
        ]
      });

      const classification = response.choices[0]?.message?.content?.trim() || 'Unknown';
      
      // Debug: Log the first response
      if (speech.id === 575688) {
        console.log('Raw response:', JSON.stringify(response.choices[0]?.message, null, 2));
        console.log('Full response object:', JSON.stringify(response, null, 2));
        console.log('Classification result:', classification);
      }
      
      // Track token usage
      const inputTokens = response.usage?.prompt_tokens || 0;
      const outputTokens = response.usage?.completion_tokens || 0;
      
      totalInputTokens += inputTokens;
      totalOutputTokens += outputTokens;
      totalRequests++;
      
      const inputCost = (inputTokens / 1000000) * INPUT_COST_PER_1M;
      const outputCost = (outputTokens / 1000000) * OUTPUT_COST_PER_1M;
      totalCost += inputCost + outputCost;

      return {
        id: speech.id,
        speaker: speech.speaker_name,
        group: speech.political_group,
        language: speech.language,
        existingTopic: speech.existing_topic,
        classifiedTopic: classification,
        inputTokens,
        outputTokens,
        cost: inputCost + outputCost,
        speechLength: speech.speech_content.length
      };

    } catch (error) {
      console.error(`❌ Error classifying speech ${speech.id}:`, error.message);
      return {
        id: speech.id,
        speaker: speech.speaker_name,
        group: speech.political_group,
        language: speech.language,
        existingTopic: speech.existing_topic,
        classifiedTopic: 'ERROR',
        inputTokens: 0,
        outputTokens: 0,
        cost: 0,
        speechLength: speech.speech_content.length,
        error: error.message
      };
    }
  }

  // Print results summary
  printResults() {
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
    console.log(`   Total input tokens: ${totalInputTokens.toLocaleString()}`);
    console.log(`   Total output tokens: ${totalOutputTokens.toLocaleString()}`);
    console.log(`   Average input tokens per speech: ${Math.round(totalInputTokens / totalRequests)}`);
    console.log(`   Average output tokens per speech: ${Math.round(totalOutputTokens / totalRequests)}`);

    // Cost breakdown
    console.log(`\n💰 Cost Analysis:`);
    console.log(`   Total cost for ${totalRequests} speeches: $${totalCost.toFixed(4)}`);
    console.log(`   Cost per speech: $${(totalCost / totalRequests).toFixed(6)}`);
    console.log(`   Input cost: $${((totalInputTokens / 1000000) * INPUT_COST_PER_1M).toFixed(4)}`);
    console.log(`   Output cost: $${((totalOutputTokens / 1000000) * OUTPUT_COST_PER_1M).toFixed(4)}`);

    // Extrapolation for full dataset
    const avgInputTokens = totalInputTokens / totalRequests;
    const avgOutputTokens = totalOutputTokens / totalRequests;
    const avgCostPerSpeech = totalCost / totalRequests;
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

    // Sample results
    console.log(`\n📝 Sample Classifications:`);
    console.log('ID | Speaker | Group | Existing Topic | Classified Topic | Length');
    console.log('-'.repeat(100));
    
    this.results.slice(0, 10).forEach(r => {
      const id = r.id.toString().padEnd(3);
      const speaker = (r.speaker || 'Unknown').substring(0, 15).padEnd(16);
      const group = (r.group || 'Unknown').substring(0, 8).padEnd(9);
      const existing = (r.existingTopic || 'None').substring(0, 20).padEnd(21);
      const classified = (r.classifiedTopic || 'ERROR').substring(0, 20).padEnd(21);
      const length = r.speechLength.toString().padEnd(6);
      console.log(`${id} | ${speaker} | ${group} | ${existing} | ${classified} | ${length}`);
    });

    if (this.results.length > 10) {
      console.log(`... and ${this.results.length - 10} more speeches`);
    }
  }

  // Main execution
  async run() {
    console.log('🚀 GPT-5-nano Topic Classification Test');
    console.log('=====================================\n');

    // Check API key
    if (!process.env.OPENAI_API_KEY) {
      console.error('❌ Error: OPENAI_API_KEY environment variable not set');
      console.log('Please set your OpenAI API key:');
      console.log('export OPENAI_API_KEY="your-api-key-here"');
      process.exit(1);
    }

    try {
      // Load prompt
      this.loadPrompt();

      // Get recent speeches
      console.log('📥 Fetching speeches from most recent sitting...');
      const speeches = await this.getRecentSpeeches();
      console.log(`✅ Found ${speeches.length} speeches to classify`);

      if (speeches.length === 0) {
        console.log('❌ No speeches found. Exiting.');
        return;
      }

      // Classify each speech
      console.log('\n🤖 Starting classification...');
      for (let i = 0; i < speeches.length; i++) {
        const speech = speeches[i];
        console.log(`Processing speech ${i + 1}/${speeches.length} (ID: ${speech.id})...`);
        
        const result = await this.classifySpeech(speech);
        this.results.push(result);
        
        // Small delay to respect rate limits
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      // Print results
      this.printResults();

      console.log('\n✅ Test completed successfully!');
      console.log('\n💡 Next steps:');
      console.log('   • Review the classification results above');
      console.log('   • Adjust the prompt if needed');
      console.log('   • Run on a larger sample if satisfied');
      console.log('   • Scale to full dataset when ready');

    } catch (error) {
      console.error('❌ Test failed:', error);
    } finally {
      this.db.close();
    }
  }
}

// Run the test
const classifier = new TopicClassifier();
classifier.run().catch(console.error);
