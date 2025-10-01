const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Database connection
const dbPath = path.join(__dirname, 'ep_data.db');
const db = new sqlite3.Database(dbPath);

console.log('🔄 REPARSING ALL SITTINGS WITH PARENTHESES PARTY DETECTION');
console.log('========================================================');

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

async function reparseAllSittings() {
  return new Promise((resolve, reject) => {
    console.log('📊 Getting all sittings with raw content...');
    
    db.all(`
      SELECT id, activity_date, content 
      FROM sittings 
      WHERE content IS NOT NULL AND content != ''
      ORDER BY activity_date DESC
    `, [], (err, sittings) => {
      if (err) {
        reject(err);
        return;
      }
      
      console.log(`📊 Found ${sittings.length} sittings with raw content`);
      
      let totalSpeeches = 0;
      let processedCount = 0;
      let parenthesesCount = 0;
      
      const processNext = () => {
        if (processedCount >= sittings.length) {
          console.log(`\n✅ REPARSING COMPLETE!`);
          console.log(`📊 Total speeches parsed: ${totalSpeeches}`);
          console.log(`🏛️  Speeches with parentheses parties: ${parenthesesCount}`);
          resolve();
          return;
        }
        
        const sitting = sittings[processedCount];
        console.log(`\n📅 Processing ${sitting.activity_date} (${processedCount + 1}/${sittings.length})`);
        
        try {
          // Delete existing individual speeches for this sitting
          db.run(`DELETE FROM individual_speeches WHERE sitting_id = ?`, [sitting.id], (err) => {
            if (err) {
              console.error(`   ❌ Error deleting speeches for ${sitting.activity_date}:`, err.message);
              processedCount++;
              setTimeout(processNext, 10);
              return;
            }
            
            const speeches = parseIndividualSpeechesWithParentheses(sitting.content, sitting.id);
            console.log(`   ✅ Parsed ${speeches.length} speeches`);
            
            // Count speeches with parentheses parties
            const parenthesesSpeeches = speeches.filter(s => {
              const nameWithPartyMatch = s.speaker_name.match(/^(.+?)\s*\(([^)]+)\)$/);
              return nameWithPartyMatch && nameWithPartyMatch[2].match(/^(PPE|S&D|ECR|Renew|Verts\/ALE|ID|The Left|NI|ALDE)$/i);
            });
            parenthesesCount += parenthesesSpeeches.length;
            
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
              totalSpeeches += speeches.length;
            }
            
            processedCount++;
            setTimeout(processNext, 10); // Small delay to prevent overwhelming
          });
          
        } catch (error) {
          console.error(`   ❌ Error parsing ${sitting.activity_date}:`, error.message);
          processedCount++;
          setTimeout(processNext, 10);
        }
      };
      
      processNext();
    });
  });
}

// Run the reparsing
reparseAllSittings()
  .then(() => {
    console.log('🎉 All done!');
    db.close();
  })
  .catch(error => {
    console.error('❌ Error:', error);
    db.close();
  });

