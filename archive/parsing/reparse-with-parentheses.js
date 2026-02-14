const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Database connection
const { DB_PATH } = require('../../core/db');
const db = new sqlite3.Database(DB_PATH);

console.log('🔄 REPARSING WITH PARENTHESES PARTY DETECTION');
console.log('=============================================');

// Enhanced parsing function that handles parentheses in names
function parseIndividualSpeechesWithParentheses(content, sittingId) {
  const speeches = [];
  
  console.log(`   🔍 Parsing content (${content.length} chars) for sitting ${sittingId}`);
  
  // Split content into lines for better processing
  const lines = content.split('\n');
  let currentSpeech = null;
  let speechOrder = 0;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    // Skip empty lines
    if (!line) continue;
    
    // Look for speech patterns with ". –" separator
    // Pattern 1: "Name, Role. – Speech content"
    // Pattern 2: "Name (Party). – Speech content"  
    // Pattern 3: "Name. – Speech content"
    // Pattern 4: "Name (Party), Role. – Speech content"
    let speechMatch = line.match(/^([^,]+),\s*(.+?)\.\s*–\s*(.+)$/);
    let speakerName, roleInfo, speechContent;
    
    if (speechMatch) {
      // Pattern 1: "Name, Role. – Speech"
      speakerName = speechMatch[1].trim();
      roleInfo = speechMatch[2].trim();
      speechContent = speechMatch[3].trim();
    } else {
      // Pattern 2: "Name (Party). – Speech"
      speechMatch = line.match(/^([^(]+)\s*\(([^)]+)\)\.\s*–\s*(.+)$/);
      if (speechMatch) {
        speakerName = speechMatch[1].trim();
        roleInfo = speechMatch[2].trim();
        speechContent = speechMatch[3].trim();
      } else {
        // Pattern 4: "Name (Party), Role. – Speech"
        speechMatch = line.match(/^([^(]+)\s*\(([^)]+)\),\s*(.+?)\.\s*–\s*(.+)$/);
        if (speechMatch) {
          speakerName = speechMatch[1].trim();
          roleInfo = speechMatch[3].trim(); // Use the role part, not the party
          speechContent = speechMatch[4].trim();
        } else {
          // Pattern 3: "Name. – Speech"
          speechMatch = line.match(/^([^.]+)\.\s*–\s*(.+)$/);
          if (speechMatch) {
            speakerName = speechMatch[1].trim();
            roleInfo = '';
            speechContent = speechMatch[2].trim();
          }
        }
      }
    }
    
    if (speechMatch) {
      // Save previous speech if exists
      if (currentSpeech) {
        speeches.push(currentSpeech);
      }
      
      // Start new speech
      
      // Determine political group and title based on the pattern matched
      let politicalGroup = null;
      let title = null;
      
      // Check if speaker name contains party in parentheses (for patterns 2 and 4)
      const nameWithPartyMatch = speakerName.match(/^(.+?)\s*\(([^)]+)\)$/);
      if (nameWithPartyMatch) {
        const partyInParentheses = nameWithPartyMatch[2].trim();
        if (partyInParentheses.match(/^(PPE|S&D|ECR|Renew|Verts\/ALE|ID|The Left|NI|ALDE)$/i)) {
          politicalGroup = partyInParentheses;
          speakerName = nameWithPartyMatch[1].trim(); // Remove party from name
          console.log(`   🏛️  Found party in parentheses: "${speakerName}" (${politicalGroup})`);
        }
      }
      
      // If we matched pattern 2 (Name (Party). – Speech), roleInfo contains the party
      if (!politicalGroup && roleInfo && roleInfo.match(/^(PPE|S&D|ECR|Renew|Verts\/ALE|ID|The Left|NI|ALDE)$/i)) {
        politicalGroup = roleInfo;
        console.log(`   🏛️  Found party in role: "${speakerName}" (${politicalGroup})`);
      } else if (roleInfo) {
        // Check if roleInfo contains party indicators
        if (roleInfo.includes('on behalf of') || roleInfo.includes('au nom de') || 
            roleInfo.includes('a nome del') || roleInfo.includes('en nombre del') ||
            roleInfo.includes('im Namen der') || roleInfo.includes('au nom du') ||
            roleInfo.includes('fraktion') || roleInfo.includes('gruppo') || 
            roleInfo.includes('grupo') || roleInfo.includes('group') || 
            roleInfo.includes('groupe') || roleInfo.includes('εξ ονόματος') ||
            roleInfo.includes('namens') || roleInfo.includes('w imieniu') ||
            roleInfo.includes('în numele') || roleInfo.includes('for ') ||
            roleInfo.includes('för ') || roleInfo.includes('thar ceann') ||
            roleInfo.includes('u ime') || roleInfo.includes('za skupinu') ||
            roleInfo.includes('em nome') || roleInfo.includes('f\'isem') ||
            roleInfo.includes('(PPE)') || roleInfo.includes('(S&D)') ||
            roleInfo.includes('(ECR)') || roleInfo.includes('(Renew)') ||
            roleInfo.includes('(Verts/ALE)') || roleInfo.includes('(ID)') ||
            roleInfo.includes('(The Left)') || roleInfo.includes('(NI)') ||
            roleInfo.includes('(ALDE)')) {
          politicalGroup = roleInfo;
        } else {
          title = roleInfo;
        }
      }
      
      currentSpeech = {
        sitting_id: sittingId,
        speaker_name: speakerName,
        political_group: politicalGroup,
        title: title,
        speech_content: speechContent,
        speech_order: ++speechOrder,
        mep_id: null
      };
      
      console.log(`   📝 Found speech: "${speakerName}"${politicalGroup ? ' (' + politicalGroup + ')' : ''}${title ? ' [' + title + ']' : ''}`);
      
    } else if (currentSpeech) {
      // Continue current speech (multiline content)
      // This handles speeches that span multiple lines
      currentSpeech.speech_content += ' ' + line;
    }
  }
  
  // Don't forget the last speech
  if (currentSpeech) {
    speeches.push(currentSpeech);
  }
  
  return speeches;
}

async function reparseSpecificSitting(date) {
  return new Promise((resolve, reject) => {
    console.log(`📅 Reparsing sitting for date: ${date}`);
    
    // Get the sitting
    db.get(`
      SELECT id, activity_date, content 
      FROM sittings 
      WHERE activity_date = ? AND content IS NOT NULL AND content != ''
    `, [date], (err, sitting) => {
      if (err) {
        reject(err);
        return;
      }
      
      if (!sitting) {
        console.log(`❌ No sitting found for date: ${date}`);
        resolve();
        return;
      }
      
      console.log(`📊 Found sitting: ${sitting.id} with ${sitting.content.length} chars of content`);
      
      // Delete existing individual speeches for this sitting
      db.run(`DELETE FROM individual_speeches WHERE sitting_id = ?`, [sitting.id], (err) => {
        if (err) {
          reject(err);
          return;
        }
        
        console.log(`🗑️  Deleted existing speeches for sitting ${sitting.id}`);
        
        try {
          const speeches = parseIndividualSpeechesWithParentheses(sitting.content, sitting.id);
          console.log(`   ✅ Parsed ${speeches.length} speeches`);
          
          if (speeches.length > 0) {
            // Insert speeches into database
            const stmt = db.prepare(`
              INSERT INTO individual_speeches 
              (sitting_id, speaker_name, political_group, title, speech_content, speech_order, mep_id)
              VALUES (?, ?, ?, ?, ?, ?, ?)
            `);
            
            speeches.forEach(speech => {
              stmt.run([
                speech.sitting_id,
                speech.speaker_name,
                speech.political_group,
                speech.title,
                speech.speech_content,
                speech.speech_order,
                speech.mep_id
              ]);
            });
            
            stmt.finalize();
            console.log(`✅ Inserted ${speeches.length} speeches into database`);
          }
          
          resolve();
          
        } catch (error) {
          console.error(`   ❌ Error parsing ${sitting.activity_date}:`, error.message);
          reject(error);
        }
      });
    });
  });
}

// Test on 2023-02-13
reparseSpecificSitting('2023-02-13')
  .then(() => {
    console.log('🎉 Reparsing complete!');
    
    // Show results
    db.all(`
      SELECT speaker_name, political_group, title, substr(speech_content, 1, 50) as content_preview 
      FROM individual_speeches 
      WHERE sitting_id IN (SELECT id FROM sittings WHERE activity_date = '2023-02-13')
      ORDER BY speech_order
      LIMIT 10
    `, [], (err, results) => {
      if (err) {
        console.error('Error getting results:', err);
      } else {
        console.log('\n📊 Sample results from 2023-02-13:');
        results.forEach(speech => {
          console.log(`   "${speech.speaker_name}"${speech.political_group ? ' (' + speech.political_group + ')' : ''}${speech.title ? ' [' + speech.title + ']' : ''}`);
        });
      }
      
      db.close();
    });
  })
  .catch(error => {
    console.error('❌ Error:', error);
    db.close();
  });

