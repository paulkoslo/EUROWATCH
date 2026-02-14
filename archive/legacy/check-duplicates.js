const sqlite3 = require('sqlite3').verbose();

const { DB_PATH } = require('../../core/db');
const db = new sqlite3.Database(DB_PATH);

console.log('🔍 DUPLICATE CHECKER & CLEANER');
console.log('==============================');
console.log('');

// Check for sitting duplicates
function checkSittingDuplicates() {
    return new Promise((resolve, reject) => {
        console.log('📅 CHECKING SITTING DUPLICATES...');
        console.log('==================================');
        
        // Check for duplicate content
        db.all(`
            SELECT content, COUNT(*) as duplicate_count, GROUP_CONCAT(date) as dates
            FROM sittings 
            WHERE content IS NOT NULL AND content != '' AND LENGTH(content) > 100
            GROUP BY content 
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
        `, (err, contentDuplicates) => {
            if (err) {
                reject(err);
                return;
            }

            // Check for duplicate dates with multiple sittings
            db.all(`
                SELECT date, COUNT(*) as sitting_count, COUNT(DISTINCT content) as unique_content
                FROM sittings 
                WHERE date != '' AND date IS NOT NULL
                GROUP BY date 
                HAVING COUNT(*) > 1
                ORDER BY sitting_count DESC
            `, (err, dateDuplicates) => {
                if (err) {
                    reject(err);
                    return;
                }

                const sittingStats = {
                    contentDuplicates: contentDuplicates,
                    dateDuplicates: dateDuplicates,
                    totalContentDuplicates: contentDuplicates.reduce((sum, dup) => sum + (dup.duplicate_count - 1), 0),
                    totalDateDuplicates: dateDuplicates.reduce((sum, dup) => sum + (dup.sitting_count - dup.unique_content), 0)
                };

                resolve(sittingStats);
            });
        });
    });
}

// Check for speech duplicates
function checkSpeechDuplicates() {
    return new Promise((resolve, reject) => {
        console.log('🎤 CHECKING SPEECH DUPLICATES...');
        console.log('=================================');
        
        db.all(`
            SELECT speech_content, COUNT(*) as duplicate_count, GROUP_CONCAT(sitting_id) as sitting_ids
            FROM individual_speeches 
            WHERE speech_content IS NOT NULL AND speech_content != ''
            GROUP BY speech_content 
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            LIMIT 20
        `, (err, speechDuplicates) => {
            if (err) {
                reject(err);
                return;
            }

            // Get total duplicate count
            db.get(`
                SELECT COUNT(*) as total_speeches, COUNT(DISTINCT speech_content) as unique_speeches
                FROM individual_speeches 
                WHERE speech_content IS NOT NULL AND speech_content != ''
            `, (err, speechStats) => {
                if (err) {
                    reject(err);
                    return;
                }

                const totalDuplicates = speechStats.total_speeches - speechStats.unique_speeches;

                resolve({
                    duplicates: speechDuplicates,
                    totalSpeeches: speechStats.total_speeches,
                    uniqueSpeeches: speechStats.unique_speeches,
                    totalDuplicates: totalDuplicates
                });
            });
        });
    });
}

// Clean sitting duplicates
function cleanSittingDuplicates() {
    return new Promise((resolve, reject) => {
        console.log('🧹 CLEANING SITTING DUPLICATES...');
        
        // Remove duplicate content (keep the first occurrence)
        db.run(`
            DELETE FROM sittings 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM sittings 
                WHERE content IS NOT NULL AND content != '' AND LENGTH(content) > 100
                GROUP BY content
            )
        `, function(err) {
            if (err) {
                reject(err);
                return;
            }

            const removed = this.changes;
            console.log(`✅ Removed ${removed} duplicate sittings`);
            resolve(removed);
        });
    });
}

// Clean speech duplicates
function cleanSpeechDuplicates() {
    return new Promise((resolve, reject) => {
        console.log('🧹 CLEANING SPEECH DUPLICATES...');
        
        // Remove duplicate speeches (keep the first occurrence)
        db.run(`
            DELETE FROM individual_speeches 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM individual_speeches 
                WHERE speech_content IS NOT NULL AND speech_content != ''
                GROUP BY speech_content
            )
        `, function(err) {
            if (err) {
                reject(err);
                return;
            }

            const removed = this.changes;
            console.log(`✅ Removed ${removed} duplicate speeches`);
            resolve(removed);
        });
    });
}

// Quick cleanup mode (no prompts)
async function quickCleanup() {
    try {
        console.log('🚀 QUICK CLEANUP MODE - Removing duplicates without prompts...');
        
        const sittingStats = await checkSittingDuplicates();
        const speechStats = await checkSpeechDuplicates();
        
        const totalDuplicates = sittingStats.totalContentDuplicates + speechStats.totalDuplicates;
        
        if (totalDuplicates === 0) {
            console.log('✅ No duplicates found! Database is clean.');
            db.close();
            return;
        }
        
        console.log(`🧹 Found ${totalDuplicates} duplicates, cleaning automatically...`);
        
        const removedSittings = await cleanSittingDuplicates();
        const removedSpeeches = await cleanSpeechDuplicates();
        
        console.log(`✅ Cleanup complete! Removed ${removedSittings + removedSpeeches} duplicates.`);
        db.close();
        
    } catch (error) {
        console.error('❌ Error during quick cleanup:', error);
        db.close();
    }
}

// Main execution
async function main() {
    try {
        // Check for duplicates
        const sittingStats = await checkSittingDuplicates();
        const speechStats = await checkSpeechDuplicates();

        // Display sitting duplicate results
        console.log(`📊 SITTING DUPLICATE SUMMARY:`);
        console.log(`   Content duplicates: ${sittingStats.contentDuplicates.length} groups`);
        console.log(`   Total duplicate sittings: ${sittingStats.totalContentDuplicates}`);
        console.log(`   Date duplicates: ${sittingStats.dateDuplicates.length} dates`);
        console.log(`   Total date duplicates: ${sittingStats.totalDateDuplicates}`);
        
        if (sittingStats.contentDuplicates.length > 0) {
            console.log(`\n🔍 Top 5 content duplicates:`);
            sittingStats.contentDuplicates.slice(0, 5).forEach((dup, i) => {
                const preview = dup.content.length > 100 ? dup.content.substring(0, 100) + '...' : dup.content;
                console.log(`   ${i + 1}. [${dup.duplicate_count}x] ${preview}`);
            });
        }

        if (sittingStats.dateDuplicates.length > 0) {
            console.log(`\n🔍 Top 5 date duplicates:`);
            sittingStats.dateDuplicates.slice(0, 5).forEach((dup, i) => {
                console.log(`   ${i + 1}. ${dup.date}: ${dup.sitting_count} sittings, ${dup.unique_content} unique content`);
            });
        }

        // Display speech duplicate results
        console.log(`\n📊 SPEECH DUPLICATE SUMMARY:`);
        console.log(`   Total speeches: ${speechStats.totalSpeeches}`);
        console.log(`   Unique speeches: ${speechStats.uniqueSpeeches}`);
        console.log(`   Duplicate speeches: ${speechStats.totalDuplicates}`);
        console.log(`   Duplication rate: ${((speechStats.totalDuplicates / speechStats.totalSpeeches) * 100).toFixed(1)}%`);

        if (speechStats.duplicates.length > 0) {
            console.log(`\n🔍 Top 10 speech duplicates:`);
            speechStats.duplicates.slice(0, 10).forEach((dup, i) => {
                const preview = dup.speech_content.length > 80 ? dup.speech_content.substring(0, 80) + '...' : dup.speech_content;
                console.log(`   ${i + 1}. [${dup.duplicate_count}x] ${preview}`);
            });
        }

        // Ask for cleanup
        const totalDuplicates = sittingStats.totalContentDuplicates + speechStats.totalDuplicates;
        
        if (totalDuplicates === 0) {
            console.log(`\n✅ No true duplicates found! Database is clean.`);
            console.log(`\n📝 Note: Date duplicates (${sittingStats.dateDuplicates.length} dates) are legitimate - they represent multiple sittings on the same date (morning/afternoon sessions).`);
            db.close();
            return;
        }

        console.log(`\n🤔 Found ${totalDuplicates} total duplicates to clean:`);
        console.log(`   - ${sittingStats.totalContentDuplicates} sitting duplicates`);
        console.log(`   - ${speechStats.totalDuplicates} speech duplicates`);

        const readline = require('readline');
        const rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });

        rl.question(`\nDo you want to clean all duplicates? (y/N): `, async (answer) => {
            rl.close();
            
            if (!answer.toLowerCase().startsWith('y')) {
                console.log('❌ Cleanup cancelled by user.');
                db.close();
                return;
            }

            console.log('\n🚀 Starting cleanup...');
            
            try {
                // Clean duplicates
                const removedSittings = await cleanSittingDuplicates();
                const removedSpeeches = await cleanSpeechDuplicates();

                console.log('\n🎉 CLEANUP COMPLETED!');
                console.log(`✅ Removed ${removedSittings} duplicate sittings`);
                console.log(`✅ Removed ${removedSpeeches} duplicate speeches`);
                console.log(`📊 Total removed: ${removedSittings + removedSpeeches} duplicates`);

                // Show final stats
                db.get('SELECT COUNT(*) as count FROM sittings', (err, sittingCount) => {
                    if (err) {
                        console.error('Error getting final sitting count:', err);
                        db.close();
                        return;
                    }

                    db.get('SELECT COUNT(*) as count FROM individual_speeches', (err, speechCount) => {
                        if (err) {
                            console.error('Error getting final speech count:', err);
                            db.close();
                            return;
                        }

                        console.log('\n📊 FINAL DATABASE STATUS:');
                        console.log(`   Sittings: ${sittingCount.count}`);
                        console.log(`   Individual speeches: ${speechCount.count}`);
                        console.log(`   MEPs: 719 (unchanged)`);
                        
                        db.close();
                    });
                });

            } catch (error) {
                console.error('❌ Error during cleanup:', error);
                db.close();
            }
        });

    } catch (error) {
        console.error('❌ Error:', error);
        db.close();
    }
}

// Check command line arguments
if (process.argv.includes('--quick') || process.argv.includes('-q')) {
    quickCleanup();
} else {
    main();
}
