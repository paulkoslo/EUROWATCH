// EP term number for Europarl URLs (matches parliament-fetch.js)
function getSessionNumber(date) {
  if (!date) return 10;
  if (date >= '2024-07-16') return 10;
  if (date >= '2019-07-02') return 9;
  if (date >= '2014-07-01') return 8;
  if (date >= '2009-07-14') return 7;
  if (date >= '2004-07-20') return 6;
  if (date >= '1999-07-20') return 5;
  if (date >= '1994-07-19') return 4;
  if (date >= '1989-07-25') return 3;
  if (date >= '1984-07-24') return 2;
  if (date >= '1979-07-17') return 1;
  return 1;
}

// Utility: shorten ID and prettify types
function shortId(id) {
  // Extracts the last part of an ID string, typically after the last '/'
  const parts = id.split('/');
  return parts[parts.length - 1] || id;
}
function prettyType(type) {
  // Converts a type string into a more human-readable format
  if (!type) return '';
  const raw = type.split('/').pop();
  return raw.split('_').map(w => w.charAt(0) + w.slice(1).toLowerCase()).join(' ');
}

async function fetchSpeechContent(speechId) {
  try {
    const response = await fetch(`https://data.europarl.europa.eu/api/v2/plenary-speeches/${speechId}`);
    if (!response.ok) {
      throw new Error(`Failed to fetch speech content: ${response.status}`);
    }
    const data = await response.json();
    return data.content || 'No content available via API.';
  } catch (error) {
    console.error('Error fetching speech content:', error);
    return 'No content available via API.';
  }
}

// Helper to render markdown safely
function renderMarkdown(md) {
  if (window.marked) {
    return marked.parse(md);
  }
  return md.replace(/\n/g, '<br>');
}

(async () => {
  let speech = null;
  
  const urlParams = new URLSearchParams(window.location.search);
  const speechId = urlParams.get('id');

  // Prefer URL id — fetch full sitting (with content) when opening
  if (speechId) {
    try {
      const response = await fetch(`/api/speeches/${encodeURIComponent(speechId)}`);
      if (response.ok) speech = await response.json();
    } catch (error) {
      console.error('❌ Error loading speech data:', error);
    }
  }
  if (!speech) {
    const recStr = sessionStorage.getItem('speechRecord');
    if (recStr) speech = JSON.parse(recStr);
  }
  
  if (!speech) {
    // If no speech data is found, display a message and provide a link back to the dashboard
    document.body.innerHTML = '<p>No speech data available. Please navigate via the dashboard list.</p><p><a href="index.html">Back</a></p>';
    return;
  }

  // Populate ID and Type for readability
  // Display the shortened ID and prettified type in the respective HTML elements
  document.getElementById('speechId').textContent = shortId(speech.id);
  document.getElementById('speechType').textContent = prettyType(speech.type);

  // Sittings use activity_date; ensure we have a date for content fetch
  const sittingDate = speech.date || speech.activity_date || (speech.id && String(speech.id).startsWith('sitting-') ? String(speech.id).replace(/^sitting-/, '') : null);

  // Title
  const sittingTitle = sittingDate ? `Sitting of ${sittingDate}` : 'Sitting';
  document.title = sittingTitle;
  document.getElementById('speechTitle').textContent = sittingTitle;
  document.getElementById('speechDate').textContent = sittingDate || '';

  // --- Main Content: fetch when opening a sitting ---
  const contentMainEl = document.getElementById('speechContentMain');
  const htmlLinkEl = document.getElementById('speechHtmlLink');
  const contentSection = document.getElementById('speechContentSection');
  if (sittingDate) {
    const apiUrl = speech.id ?
      `/api/speech-html-content?date=${sittingDate}&speechId=${encodeURIComponent(speech.id)}` :
      `/api/speech-html-content?date=${sittingDate}`;
    
    fetch(apiUrl)
      .then(res => res.ok ? res.json() : { content: '—' })
      .then(async data => {
        let content = data.content || '—';
        // Fallback: If no content, try to fetch the TOC page and extract something
        if (content === '—' || !content.trim()) {
          const session = getSessionNumber(sittingDate);
          const fallbackUrl = `https://www.europarl.europa.eu/doceo/document/CRE-${session}-${sittingDate}_EN.html`;
          try {
            const resp = await fetch(fallbackUrl);
            if (resp.ok) {
              const html = await resp.text();
              // Extract all agenda items from the TOC HTML
              const parser = new DOMParser();
              const doc = parser.parseFromString(html, 'text/html');
              const items = Array.from(doc.querySelectorAll('a[href*="ITM-"]'));
              if (items.length > 0) {
                content = `<strong>TOC Agenda Items:</strong><ul>` +
                  items.map(a => {
                    const href = a.getAttribute('href');
                    const absHref = href.startsWith('http') ? href : `https://www.europarl.europa.eu${href}`;
                    return `<li><a href="${absHref}" target="_blank">${a.textContent.trim()}</a></li>`;
                  }).join('') + '</ul>';
              } else {
                content = 'No agenda items found in TOC.';
              }
            } else {
              content = 'No content available (TOC fetch failed)';
            }
          } catch (err) {
            content = 'No content available (TOC fetch error)';
          }
        }
        // Replace newlines with <br> for better readability
        contentMainEl.innerHTML = content.replace(/\n/g, '<br>');
        // Make the content box collapsible
        if (contentSection && !contentSection.hasAttribute('data-collapsible')) {
          contentSection.innerHTML = `<details open><summary style="font-size:1.15em;font-weight:600;color:var(--eu-blue);cursor:pointer;outline:none;">Speech Content</summary><div id="speechContentMain">${content.replace(/\n/g, '<br>')}</div></details>`;
          contentSection.setAttribute('data-collapsible', 'true');
        }
        // Load individual speeches if available
        loadIndividualSpeeches(speech.id);

        // Set content and sitting ID for AI chat widget
        if (typeof window.setAIChatContent === 'function') {
          window.setAIChatContent(content, speech.id);
        }

        const session = getSessionNumber(sittingDate);
        const htmlUrl = `https://www.europarl.europa.eu/doceo/document/CRE-${session}-${sittingDate}_EN.html`;
        htmlLinkEl.href = htmlUrl;
        htmlLinkEl.textContent = 'Open HTML';
        htmlLinkEl.style.display = '';
      });
  } else {
    contentMainEl.textContent = '—';
    htmlLinkEl.style.display = 'none';
  }

  // Speaker info
  // Check if participation data is available and extract speaker details
  const participation = speech.had_participation || speech.participation;
  if (participation && Array.isArray(participation.had_participant_person) && participation.had_participant_person.length) {
    const personUri = participation.had_participant_person[0];
    const parts = personUri.split('/');
    const pid = parts.pop();
    try {
      // Fetch speaker details from the API using the participant ID
      const mepResp = await fetch(`/api/meps/${pid}`);
      const mepJson = await mepResp.json();
      const mep = mepJson.data;

      // Populate speaker details in the respective HTML elements
      document.getElementById('mepName').textContent = mep.label;
      document.getElementById('mepCountry').textContent = mep['api:country-of-representation'];
      document.getElementById('mepGroup').textContent = mep['api:political-group'];
    } catch (e) {
      // Log an error if fetching speaker details fails
      console.error('Error loading speaker details:', e);
    }
  } else {
    // Hide the speaker info section if no speaker information is available
    const mepDetailsEl = document.getElementById('mepDetails');
    if (mepDetailsEl) {
      mepDetailsEl.style.display = 'none';
    }
  }

  // External transcript link if available
  const extLinkEl = document.getElementById('externalLink');
  const extContainer = document.getElementById('externalLinkContainer');
  if (extLinkEl && speech.docIdentifier) {
    // Set the href attribute of the external link if a document identifier is available
    extLinkEl.href = `https://data.europa.eu/eli/dl/doc/${speech.docIdentifier}?lang=EN`;
  } else if (extContainer) {
    // Hide the external link container if no document identifier is available
    extContainer.style.display = 'none';
  }

  // --- Quick Overview Bar ---
  const quickSpeakerEl = document.getElementById('quickSpeaker');
  const aiSpeakersAnswerEl = document.getElementById('aiSpeakersAnswer');
  const quickOverviewErrorEl = document.getElementById('quickOverviewError');

  // Helper to set quick overview speaker names only
  async function setQuickSpeakerInfo() {
    quickOverviewErrorEl.textContent = '';
    const participation = speech.had_participation || speech.participation;
    if (participation && Array.isArray(participation.had_participant_person) && participation.had_participant_person.length) {
      const names = [];
      for (const personUri of participation.had_participant_person) {
        const parts = personUri.split('/');
        const pid = parts.pop();
        try {
          const mepResp = await fetch(`/api/meps/${pid}`);
          if (!mepResp.ok) throw new Error('MEP API returned ' + mepResp.status);
          const mepJson = await mepResp.json();
          const mep = mepJson.data;
          if (mep && mep.label) names.push(mep.label);
        } catch (e) {
          // skip missing
        }
      }
      quickSpeakerEl.textContent = names.length ? names.join(', ') : '(Unknown)';
    } else {
      quickSpeakerEl.textContent = 'No speaker information available.';
      quickOverviewErrorEl.textContent = 'No speaker participation data in this record.';
    }
  }

  // Helper to ask ChatGPT what speakers spoke today
  async function askAISpeakers(content) {
    aiSpeakersAnswerEl.textContent = 'Loading...';
    try {
      const response = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer YOUR_API_KEY_HERE'
        },
        body: JSON.stringify({
          model: 'gpt-4o-mini',
          messages: [
            {
              role: 'system',
              content: 'You are a helpful assistant for parliamentary data. List the names of the speakers who spoke today, based on the following content. If not clear, say so.'
            },
            {
              role: 'user',
              content: `What speakers spoke today?\n\nFull speech content:\n\n${content}`
            }
          ],
          temperature: 0.2
        })
      });
      if (!response.ok) throw new Error('OpenAI API error: ' + response.status);
      const data = await response.json();
      if (!data.choices || !data.choices[0] || !data.choices[0].message || !data.choices[0].message.content) {
        throw new Error('OpenAI response malformed or empty');
      }
      aiSpeakersAnswerEl.textContent = data.choices[0].message.content;
    } catch (e) {
      aiSpeakersAnswerEl.textContent = 'Failed to get AI answer: ' + (e.message || e);
    }
  }

  // Function to load and display speakers list
  async function loadSpeakersList(sittingId) {
    const speakersListEl = document.getElementById('speakersList');
    const viewAllLinkEl = document.getElementById('viewAllSpeakersLink');
    
    if (!speakersListEl) return;
    
    try {
      const response = await fetch(`/api/speeches/${encodeURIComponent(sittingId)}/individual`);
      const data = await response.json();
      
      if (data.individual_speeches && data.individual_speeches.length > 0) {
        // Extract unique speakers with their mep_id
        const speakerMap = new Map();
        data.individual_speeches.forEach(speech => {
          const speakerName = speech.speaker_name || 'Unknown Speaker';
          const mepId = speech.mep_id;
          // Use mep_id as key if available, otherwise use speaker name
          const key = mepId || speakerName;
          if (!speakerMap.has(key)) {
            speakerMap.set(key, {
              name: speakerName,
              mepId: mepId
            });
          }
        });
        
        const speakers = Array.from(speakerMap.values());
        
        if (speakers.length === 0) {
          speakersListEl.innerHTML = '<div style="color:#666;">No speakers found.</div>';
          return;
        }
        
        // Display first 3 speakers
        const firstThree = speakers.slice(0, 3);
        let html = '<div style="display:flex;flex-direction:column;gap:0.5rem;align-items:flex-end;">';
        firstThree.forEach(speaker => {
          if (speaker.mepId) {
            html += `<a href="/mep-details.html?id=${speaker.mepId}" target="_blank" style="color:var(--eu-blue);text-decoration:none;font-weight:500;text-align:right;">${speaker.name}</a>`;
          } else {
            html += `<span style="color:#333;text-align:right;">${speaker.name}</span>`;
          }
        });
        html += '</div>';
        speakersListEl.innerHTML = html;
        
        // Show "view all" link if there are more than 3 speakers
        if (speakers.length > 3) {
          viewAllLinkEl.style.display = 'block';
          viewAllLinkEl.onclick = (e) => {
            e.preventDefault();
            showAllSpeakersPopup(speakers);
          };
        } else {
          viewAllLinkEl.style.display = 'none';
        }
      } else {
        speakersListEl.innerHTML = '<div style="color:#666;">No speakers found.</div>';
      }
    } catch (error) {
      console.error('❌ Error loading speakers:', error);
      speakersListEl.innerHTML = '<div style="color:#b00;">Error loading speakers.</div>';
    }
  }
  
  // Function to show popup with all speakers
  function showAllSpeakersPopup(speakers) {
    // Create popup overlay
    const overlay = document.createElement('div');
    overlay.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:10000;display:flex;align-items:center;justify-content:center;';
    
    // Create popup content
    const popup = document.createElement('div');
    popup.style.cssText = 'background:white;border-radius:8px;padding:2rem;max-width:500px;max-height:80vh;overflow-y:auto;box-shadow:0 4px 20px rgba(0,0,0,0.3);';
    
    let html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;">';
    html += '<h3 style="margin:0;color:var(--eu-blue);">All Speakers</h3>';
    html += '<button id="closePopupBtn" style="background:none;border:none;font-size:1.5em;cursor:pointer;color:#666;">&times;</button>';
    html += '</div>';
    html += '<div style="display:flex;flex-direction:column;gap:0.75rem;">';
    
    speakers.forEach(speaker => {
      if (speaker.mepId) {
        html += `<a href="/mep-details.html?id=${speaker.mepId}" target="_blank" style="color:var(--eu-blue);text-decoration:none;font-weight:500;padding:0.5rem;border-radius:4px;transition:background 0.2s;" onmouseover="this.style.background='#f0f0f0'" onmouseout="this.style.background='transparent'">${speaker.name}</a>`;
      } else {
        html += `<div style="color:#333;padding:0.5rem;">${speaker.name}</div>`;
      }
    });
    
    html += '</div>';
    popup.innerHTML = html;
    
    overlay.appendChild(popup);
    document.body.appendChild(overlay);
    
    // Close button handler
    const closeBtn = popup.querySelector('#closePopupBtn');
    closeBtn.onclick = () => {
      document.body.removeChild(overlay);
    };
    
    // Click outside to close
    overlay.onclick = (e) => {
      if (e.target === overlay) {
        document.body.removeChild(overlay);
      }
    };
    
    // ESC key to close
    const escHandler = (e) => {
      if (e.key === 'Escape') {
        document.body.removeChild(overlay);
        document.removeEventListener('keydown', escHandler);
      }
    };
    document.addEventListener('keydown', escHandler);
  }

  // Call quick overview logic after content is loaded
  setQuickSpeakerInfo();
  
  // Load and display speakers from individual speeches
  loadSpeakersList(speech.id);
})();

// Function to load and display individual speeches
async function loadIndividualSpeeches(sittingId) {
  try {
    console.log(`🔍 Loading individual speeches for sitting: ${sittingId}`);
    const response = await fetch(`/api/speeches/${encodeURIComponent(sittingId)}/individual`);
    const data = await response.json();
    
    if (data.individual_speeches && data.individual_speeches.length > 0) {
      console.log(`✅ Found ${data.individual_speeches.length} individual speeches`);
      displayIndividualSpeeches(data.individual_speeches);
    } else {
      console.log('ℹ️ No individual speeches found, showing raw content');
    }
  } catch (error) {
    console.error('❌ Error loading individual speeches:', error);
  }
}

// Function to display individual speeches
function displayIndividualSpeeches(speeches) {
  const contentSection = document.getElementById('speechContentSection');
  if (!contentSection) return;
  
  let html = '<h2>Individual Speeches</h2>';
  html += `<div style="margin-bottom: 1rem; color: #666;">Found ${speeches.length} individual speeches in this sitting</div>`;
  
  let currentTopic = null;
  
  speeches.forEach((speech, index) => {
    const speaker = speech.speaker_name || speech.title || 'Unknown Speaker';
    // Prioritize standardized political group, fallback to raw political group
    const group = speech.political_group_std || speech.political_group || '';
    const title = speech.title || '';
    const content = speech.speech_content || 'No content available';
    const language = speech.language || 'EN';
    const macroTopic = speech.macro_topic || null;
    const macroFocus = speech.macro_specific_focus || null;
    const topic = speech.topic || null; // legacy html topic as fallback
    
    // Create a unique ID for each speech
    const speechId = `speech-${speech.speech_order}`;
    
    // Determine what to show in the blue box: political group OR title/role
    const blueBoxText = group || title;
    
    // Language mapping for display
    const languageNames = {
      'EN': 'English', 'DE': 'German', 'FR': 'French', 'IT': 'Italian', 'ES': 'Spanish',
      'PL': 'Polish', 'NL': 'Dutch', 'SV': 'Swedish', 'DA': 'Danish', 'EL': 'Greek',
      'PT': 'Portuguese', 'RO': 'Romanian', 'HU': 'Hungarian', 'CS': 'Czech', 'SK': 'Slovak',
      'SL': 'Slovenian', 'HR': 'Croatian', 'BG': 'Bulgarian', 'FI': 'Finnish', 'ET': 'Estonian',
      'LV': 'Latvian', 'LT': 'Lithuanian', 'MT': 'Maltese', 'GA': 'Irish', 'CY': 'Cypriot'
    };
    
    const languageDisplay = languageNames[language] || language;
    
    // Blue separator: show original agenda title when it changes
    if (topic && topic !== currentTopic) {
      html += `
        <div style="background: linear-gradient(135deg, #003399, #0044cc); color: #ffffff !important; padding: 0.8rem 1rem; margin: 1rem 0 0.5rem 0; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,51,153,0.3);">
          <h3 style="margin: 0; font-size: 1.1em; font-weight: 600; color: #ffffff !important;">📋 ${topic}</h3>
        </div>
      `;
      currentTopic = topic;
    }
    
    html += `
      <div style="border: 1px solid #ddd; border-radius: 8px; margin-bottom: 0.5rem; background: #fff; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
        ${macroTopic ? `
          <div style="background: #e8f5e8; color: #2d5a2d; padding: 0.2rem 0.5rem; border-radius: 8px 8px 0 0; font-size: 0.65em; font-weight: 500; text-align: center; border-bottom: 1px solid #d4edda;">
            AI: ${macroFocus ? `${macroTopic} — ${macroFocus}` : macroTopic}
          </div>
        ` : ''}
        <details style="padding: 0;">
          <summary style="padding: 1rem; cursor: pointer; display: flex; justify-content: space-between; align-items: center; background: #f8f9fa; border-radius: ${macroTopic ? '0 0 8px 8px' : '8px 8px 0 0'}; list-style: none; outline: none;">
            <div style="display: flex; align-items: center; gap: 1rem;">
              <h3 style="margin: 0; color: var(--eu-blue); font-size: 1.1em;">${index + 1}. ${speaker}</h3>
              <span style="font-size: 0.8em; color: #666; background: #e9ecef; padding: 0.2em 0.5em; border-radius: 4px;">#${speech.speech_order}</span>
            </div>
            <div style="display: flex; align-items: center; gap: 0.5rem;">
              ${blueBoxText ? `<div style="background: var(--eu-blue); color: white; padding: 0.2em 0.6em; border-radius: 6px; font-size: 0.8em; font-weight: 600; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${blueBoxText}">${blueBoxText}</div>` : ''}
              <div style="background: #28a745; color: white; padding: 0.2em 0.6em; border-radius: 6px; font-size: 0.8em; font-weight: 600;" title="Language: ${languageDisplay}">${language}</div>
              <div style="color: #666; font-size: 1.2em;">▼</div>
            </div>
          </summary>
          <div style="padding: 1rem; line-height: 1.6; color: #333; border-top: 1px solid #eee;">
            ${content.replace(/\n/g, '<br>')}
          </div>
        </details>
      </div>
    `;
  });
  
  contentSection.innerHTML = html;
  
  // Add CSS for smooth dropdown animations
  const style = document.createElement('style');
  style.textContent = `
    details[open] summary .dropdown-arrow {
      transform: rotate(180deg);
    }
    .dropdown-arrow {
      transition: transform 0.2s ease;
    }
    details summary::-webkit-details-marker {
      display: none;
    }
    details summary::marker {
      display: none;
    }
  `;
  document.head.appendChild(style);
  
  // Update the dropdown arrows to use the CSS class
  setTimeout(() => {
    const arrows = contentSection.querySelectorAll('details summary div:last-child');
    arrows.forEach(arrow => {
      arrow.className = 'dropdown-arrow';
    });
  }, 100);
}
