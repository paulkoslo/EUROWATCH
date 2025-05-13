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

async function splitIntoChunks(content, maxChunkSize = 10000) {
  const chunks = [];
  let currentChunk = '';
  const sentences = content.split(/(?<=[.!?])\s+/);
  
  for (const sentence of sentences) {
    if ((currentChunk + sentence).length > maxChunkSize) {
      if (currentChunk) {
        chunks.push(currentChunk.trim());
        currentChunk = '';
      }
      // If a single sentence is longer than maxChunkSize, split it by words
      if (sentence.length > maxChunkSize) {
        const words = sentence.split(/\s+/);
        let tempChunk = '';
        for (const word of words) {
          if ((tempChunk + word).length > maxChunkSize) {
            chunks.push(tempChunk.trim());
            tempChunk = word;
          } else {
            tempChunk += (tempChunk ? ' ' : '') + word;
          }
        }
        if (tempChunk) {
          currentChunk = tempChunk;
        }
      } else {
        currentChunk = sentence;
      }
    } else {
      currentChunk += (currentChunk ? ' ' : '') + sentence;
    }
  }
  
  if (currentChunk) {
    chunks.push(currentChunk.trim());
  }
  
  return chunks;
}

async function generateChunkSummary(chunk, chunkNumber, totalChunks) {
  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer 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'
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          {
            role: 'system',
            content: 'You are a helpful assistant that summarizes parliamentary speeches. Create a concise bullet-point summary of the key points from this section of the speech.'
          },
          {
            role: 'user',
            content: `This is part ${chunkNumber} of ${totalChunks} of a parliamentary speech. Please provide a bullet-point summary of the key points from this section:\n\n${chunk}`
          }
        ],
        temperature: 0.3
      })
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(`OpenAI API error: ${response.status} - ${errorData.error?.message || 'Unknown error'}`);
    }

    const data = await response.json();
    return data.choices[0].message.content;
  } catch (error) {
    console.error(`Error generating summary for chunk ${chunkNumber}:`, error);
    return `[Error summarizing section ${chunkNumber}]`;
  }
}

async function generateFinalSummary(chunkSummaries) {
  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer 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'
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          {
            role: 'system',
            content: 'You are a helpful assistant that creates comprehensive summaries of parliamentary speeches. Create a well-structured, detailed summary that combines all the section summaries into a coherent whole. Use bullet points and organize the information logically.'
          },
          {
            role: 'user',
            content: `Please create a comprehensive summary of this parliamentary speech by combining the following section summaries:\n\n${chunkSummaries.join('\n\n')}`
          }
        ],
        temperature: 0.3
      })
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(`OpenAI API error: ${response.status} - ${errorData.error?.message || 'Unknown error'}`);
    }

    const data = await response.json();
    return data.choices[0].message.content;
  } catch (error) {
    console.error('Error generating final summary:', error);
    return `Error generating final summary: ${error.message}`;
  }
}

function updateProgress(current, total, status) {
  const progressDiv = document.getElementById('summaryProgress');
  const progressBar = document.getElementById('progressBar');
  const progressStatus = document.getElementById('progressStatus');
  const progressPercentage = document.getElementById('progressPercentage');
  
  progressDiv.style.display = 'block';
  const percentage = Math.round((current / total) * 100);
  progressBar.style.width = `${percentage}%`;
  progressPercentage.textContent = `${percentage}%`;
  progressStatus.textContent = status;
}

async function generateAISummary(content) {
  try {
    // Show initial progress
    updateProgress(0, 100, 'Analyzing content...');
    
    // Split content into chunks
    const chunks = await splitIntoChunks(content);
    const totalChunks = chunks.length;
    
    // Update progress for chunk splitting
    updateProgress(1, totalChunks + 2, 'Content split into sections');
    
    // Generate summaries for each chunk
    const chunkSummaries = [];
    for (let i = 0; i < chunks.length; i++) {
      updateProgress(i + 2, totalChunks + 2, `Summarizing section ${i + 1} of ${totalChunks}`);
      const summary = await generateChunkSummary(chunks[i], i + 1, totalChunks);
      chunkSummaries.push(summary);
    }
    
    // Update progress for final summary
    updateProgress(totalChunks + 1, totalChunks + 2, 'Generating final summary');
    
    // Generate final summary from all chunk summaries
    const finalSummary = await generateFinalSummary(chunkSummaries);
    
    // Hide progress bar and show completion
    updateProgress(totalChunks + 2, totalChunks + 2, 'Summary complete');
    setTimeout(() => {
      document.getElementById('summaryProgress').style.display = 'none';
    }, 1000);
    
    return finalSummary;
  } catch (error) {
    console.error('Error in summary generation process:', error);
    // Show error in progress bar
    updateProgress(0, 100, `Error: ${error.message}`);
    return `Error generating summary: ${error.message}`;
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
  // Read speech record from sessionStorage
  const recStr = sessionStorage.getItem('speechRecord');
  if (!recStr) {
    // If no speech data is found, display a message and provide a link back to the dashboard
    document.body.innerHTML = '<p>No speech data available. Please navigate via the dashboard list.</p><p><a href="index.html">Back</a></p>';
    return;
  }

  // Parse the speech record from sessionStorage
  const speech = JSON.parse(recStr);

  // Populate ID and Type for readability
  // Display the shortened ID and prettified type in the respective HTML elements
  document.getElementById('speechId').textContent = shortId(speech.id);
  document.getElementById('speechType').textContent = prettyType(speech.type);

  // Title
  // Display the speech title or 'Untitled' if not available
  const sittingTitle = speech.date ? `Sitting of ${speech.date}` : 'Sitting';
  document.title = sittingTitle;
  document.getElementById('speechTitle').textContent = sittingTitle;

  // Date
  // Display the speech date or an empty string if not available
  document.getElementById('speechDate').textContent = speech.date || '';

  // --- Main Content (from Europarl HTML, first 2000 chars) ---
  const contentMainEl = document.getElementById('speechContentMain');
  const htmlLinkEl = document.getElementById('speechHtmlLink');
  const contentSection = document.getElementById('speechContentSection');
  if (speech.date) {
    fetch(`/api/speech-html-content?date=${speech.date}`)
      .then(res => res.ok ? res.json() : { content: '—' })
      .then(async data => {
        let content = data.content || '—';
        // Fallback: If no content, try to fetch the TOC page and extract something
        if (content === '—' || !content.trim()) {
          const tocHtmlUrl = `https://www.europarl.europa.eu/doceo/document/CRE-10-${speech.date}-TOC_EN.html`;
          try {
            const resp = await fetch(tocHtmlUrl);
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
        // Run AI Speaker Finder FIRST
        runSpeakerFinder(content);
        // Generate and display AI summary
        const summaryEl = document.getElementById('aiSummary');
        if (summaryEl) {
          summaryEl.innerHTML = 'Generating summary...';
          try {
            const summary = await generateAISummary(content);
            summaryEl.innerHTML = renderMarkdown(summary);
          } catch (error) {
            summaryEl.textContent = `Error generating summary: ${error.message}`;
          }
        }
        // Ask AI for speakers
        askAISpeakers(content);

        // --- AI Chat Logic ---
        const chatForm = document.getElementById('chatForm');
        const chatInput = document.getElementById('chatInput');
        const chatWindow = document.getElementById('chatWindow');
        let chatHistory = [];
        let summaryText = '';

        // Wait for summary to be generated, then set summaryText
        if (summaryEl) {
          const observer = new MutationObserver(() => {
            summaryText = summaryEl.textContent || '';
          });
          observer.observe(summaryEl, { childList: true, subtree: true, characterData: true });
          // Set initial value
          summaryText = summaryEl.textContent || '';
        }

        function appendChat(role, text) {
          const msgDiv = document.createElement('div');
          msgDiv.style.marginBottom = '0.5rem';
          msgDiv.style.whiteSpace = 'pre-wrap';
          msgDiv.innerHTML = `<strong>${role === 'user' ? 'You' : 'AI'}:</strong> ` + renderMarkdown(text);
          chatWindow.appendChild(msgDiv);
          chatWindow.scrollTop = chatWindow.scrollHeight;
        }

        chatForm.onsubmit = async (e) => {
          e.preventDefault();
          const question = chatInput.value.trim();
          if (!question) return;
          appendChat('user', question);
          chatInput.value = '';
          chatInput.disabled = true;
          chatForm.querySelector('button').disabled = true;

          // Use the full speech content as context for the AI chat
          const contextMessages = [
            {
              role: 'system',
              content: 'You are an expert assistant for European Parliament sessions. Answer questions based on the following full speech content. If the answer is not in the content, say so.'
            },
            {
              role: 'user',
              content: `Full speech content:\n\n${content}\n\n(Only use the information in this content.)`
            }
          ];
          // Add previous chat turns (last 3 exchanges)
          chatHistory.slice(-6).forEach(msg => contextMessages.push(msg));
          contextMessages.push({ role: 'user', content: question });

          try {
            const response = await fetch('https://api.openai.com/v1/chat/completions', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer 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'
              },
              body: JSON.stringify({
                model: 'gpt-4o-mini',
                messages: contextMessages,
                temperature: 0.2
              })
            });
            if (!response.ok) {
              throw new Error('OpenAI API error');
            }
            const data = await response.json();
            const answer = data.choices[0].message.content;
            appendChat('assistant', answer);
            chatHistory.push({ role: 'user', content: question });
            chatHistory.push({ role: 'assistant', content: answer });
          } catch (err) {
            appendChat('assistant', 'Sorry, there was an error contacting OpenAI.');
          } finally {
            chatInput.disabled = false;
            chatForm.querySelector('button').disabled = false;
            chatInput.focus();
          }
        };

        // Set the HTML link, with fallback if the main page is missing
        const mainHtmlUrl = `https://www.europarl.europa.eu/doceo/document/CRE-10-${speech.date}_EN.html`;
        const tocHtmlUrl = `https://www.europarl.europa.eu/doceo/document/CRE-10-${speech.date}-TOC_EN.html`;
        try {
          const resp = await fetch(mainHtmlUrl, { method: 'HEAD' });
          if (resp.ok) {
            htmlLinkEl.href = mainHtmlUrl;
            htmlLinkEl.textContent = 'Open HTML';
            htmlLinkEl.style.display = '';
          } else {
            // Fallback to TOC
            htmlLinkEl.href = tocHtmlUrl;
            htmlLinkEl.textContent = 'Open TOC HTML';
            htmlLinkEl.style.display = '';
          }
        } catch (e) {
          // Fallback to TOC
          htmlLinkEl.href = tocHtmlUrl;
          htmlLinkEl.textContent = 'Open TOC HTML';
          htmlLinkEl.style.display = '';
        }
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
    // Display a fallback message if no speaker information is available
    document.getElementById('mepDetails').innerHTML = '<p>No speaker information available.</p>';
  }

  // External transcript link if available
  const extLinkEl = document.getElementById('externalLink');
  const extContainer = document.getElementById('externalLinkContainer');
  if (speech.docIdentifier) {
    // Set the href attribute of the external link if a document identifier is available
    extLinkEl.href = `https://data.europa.eu/eli/dl/doc/${speech.docIdentifier}?lang=EN`;
  } else {
    // Hide the external link container if no document identifier is available
    extContainer.style.display = 'none';
  }

  // Show raw JSON
  const rawEl = document.getElementById('rawJson');
  if (rawEl) {
    // Display the raw JSON data of the speech record
    rawEl.textContent = JSON.stringify(speech, null, 2);
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
          'Authorization': 'Bearer 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'
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

  // Helper to ask AI who spoke today (for the Speaker Finder section)
  async function runSpeakerFinder(content) {
    aiSpeakerFinderWindow.innerHTML = '<em>Loading...</em>';
    try {
      const response = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer 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'
        },
        body: JSON.stringify({
          model: 'gpt-4o-mini',
          messages: [
            {
              role: 'system',
              content: 'You are a helpful assistant for parliamentary data. ONLY return a bulletpoint list of the names of the speakers who spoke in this session, based on the following content. Do not add any explanations or extra text. If not clear, return "Unknown". After each name start a new line. use HTML to format it correctly.' 
            },
            {
              role: 'user',
              content: `Who were the speakers in this session? Use the full content.\n\nFull speech content:\n\n${content}`
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
      aiSpeakerFinderWindow.innerHTML = data.choices[0].message.content;
    } catch (e) {
      aiSpeakerFinderWindow.innerHTML = '<span style="color:#b00">Failed to get AI answer: ' + (e.message || e) + '</span>';
    }
  }

  // Call quick overview logic after content is loaded
  setQuickSpeakerInfo();
})();