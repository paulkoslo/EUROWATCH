// Ask AI Chat Widget - Floating button with chat interface
// This module provides a floating AI chat button that opens a chat interface

(function() {
  'use strict';

  // Configuration
  const config = {
    buttonPosition: 'bottom-left',
    buttonSize: '56px',
    chatWidth: '400px',
    chatHeight: '600px'
  };

  // State
  let chatHistory = [];
  let speechContent = '';
  let sittingId = null;
  let isOpen = false;

  // Create floating button
  function createFloatingButton() {
    const button = document.createElement('button');
    button.id = 'aiChatFloatingBtn';
    button.innerHTML = '💬 Ask AI about this Sitting';
    button.setAttribute('aria-label', 'Ask AI about this Sitting');
    button.style.cssText = `
      position: fixed;
      bottom: 20px;
      left: 20px;
      padding: 0.75rem 1.25rem;
      border-radius: 50px;
      background: var(--eu-blue, #003399);
      color: white;
      border: none;
      cursor: pointer;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
      font-size: 0.95em;
      font-weight: 600;
      z-index: 9999;
      transition: transform 0.2s, box-shadow 0.2s;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
      white-space: nowrap;
    `;
    
    button.onmouseover = () => {
      button.style.transform = 'scale(1.1)';
      button.style.boxShadow = '0 6px 16px rgba(0, 0, 0, 0.4)';
    };
    
    button.onmouseout = () => {
      button.style.transform = 'scale(1)';
      button.style.boxShadow = '0 4px 12px rgba(0, 0, 0, 0.3)';
    };
    
    button.onclick = () => {
      toggleChat();
    };
    
    document.body.appendChild(button);
    return button;
  }

  // Create chat interface
  function createChatInterface() {
    const overlay = document.createElement('div');
    overlay.id = 'aiChatOverlay';
    overlay.style.cssText = `
      position: fixed;
      bottom: 100px;
      left: 20px;
      width: ${config.chatWidth};
      height: ${config.chatHeight};
      background: white;
      border-radius: 12px;
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
      z-index: 10000;
      display: none;
      flex-direction: column;
      overflow: hidden;
    `;

    // Header
    const header = document.createElement('div');
    header.style.cssText = `
      background: var(--eu-blue, #003399);
      color: white;
      padding: 1rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
    `;
    header.innerHTML = `
      <h3 style="margin: 0; font-size: 1.1em;">Ask AI about this Session</h3>
      <button id="aiChatCloseBtn" style="background: none; border: none; color: white; font-size: 1.5em; cursor: pointer; padding: 0; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center;">&times;</button>
    `;

    // Chat window
    const chatWindow = document.createElement('div');
    chatWindow.id = 'aiChatWindow';
    chatWindow.style.cssText = `
      flex: 1;
      overflow-y: auto;
      padding: 1rem;
      background: #f8f9fa;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    `;

    // Welcome message
    const welcomeMsg = document.createElement('div');
    welcomeMsg.style.cssText = 'color: #666; font-size: 0.9em; padding: 0.5rem;';
    welcomeMsg.textContent = 'Ask me anything about this parliamentary session!';
    chatWindow.appendChild(welcomeMsg);

    // Input form
    const form = document.createElement('form');
    form.id = 'aiChatForm';
    form.style.cssText = `
      display: flex;
      gap: 0.5rem;
      padding: 1rem;
      background: white;
      border-top: 1px solid #e0e0e0;
    `;

    const input = document.createElement('input');
    input.id = 'aiChatInput';
    input.type = 'text';
    input.placeholder = 'Ask a question...';
    input.style.cssText = `
      flex: 1;
      padding: 0.75rem;
      border: 1px solid #ddd;
      border-radius: 6px;
      font-size: 0.95em;
      outline: none;
    `;

    const sendBtn = document.createElement('button');
    sendBtn.type = 'submit';
    sendBtn.textContent = 'Send';
    sendBtn.style.cssText = `
      padding: 0.75rem 1.5rem;
      background: var(--eu-blue, #003399);
      color: white;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-weight: 600;
      font-size: 0.95em;
    `;

    form.appendChild(input);
    form.appendChild(sendBtn);
    
    overlay.appendChild(header);
    overlay.appendChild(chatWindow);
    overlay.appendChild(form);
    
    document.body.appendChild(overlay);

    // Close button handler
    const closeBtn = overlay.querySelector('#aiChatCloseBtn');
    closeBtn.onclick = () => {
      toggleChat();
    };

    // Form submission
    form.onsubmit = async (e) => {
      e.preventDefault();
      await handleChatSubmit(input, chatWindow);
    };

    return overlay;
  }

  // Append chat message
  function appendChatMessage(chatWindow, role, text) {
    const msgDiv = document.createElement('div');
    msgDiv.style.cssText = `
      padding: 0.75rem 1rem;
      border-radius: 8px;
      max-width: 85%;
      word-wrap: break-word;
      ${role === 'user' 
        ? 'background: var(--eu-blue, #003399); color: white; align-self: flex-end; margin-left: auto;'
        : 'background: white; color: #333; border: 1px solid #e0e0e0; align-self: flex-start;'
      }
    `;
    
    const roleLabel = document.createElement('strong');
    roleLabel.textContent = role === 'user' ? 'You' : 'AI';
    roleLabel.style.cssText = `display: block; margin-bottom: 0.25rem; font-size: 0.85em; opacity: 0.9;`;
    
    const content = document.createElement('div');
    content.style.cssText = 'white-space: pre-wrap; line-height: 1.5;';
    content.innerHTML = renderMarkdown(text);
    
    msgDiv.appendChild(roleLabel);
    msgDiv.appendChild(content);
    chatWindow.appendChild(msgDiv);
    
    // Remove welcome message if it exists
    const welcomeMsg = chatWindow.querySelector('div:first-child');
    if (welcomeMsg && welcomeMsg.textContent.includes('Ask me anything')) {
      welcomeMsg.remove();
    }
    
    chatWindow.scrollTop = chatWindow.scrollHeight;
  }

  // Render markdown safely
  function renderMarkdown(md) {
    if (window.marked) {
      return marked.parse(md);
    }
    return md.replace(/\n/g, '<br>');
  }

  // Handle chat submission
  async function handleChatSubmit(input, chatWindow) {
    const question = input.value.trim();
    if (!question) return;

    appendChatMessage(chatWindow, 'user', question);
    input.value = '';
    input.disabled = true;
    const sendBtn = chatWindow.parentElement.querySelector('button[type="submit"]');
    sendBtn.disabled = true;
    sendBtn.textContent = 'Sending...';

    // Build optimized content (3-4 speeches per topic)
    const optimizedContent = await getOptimizedSpeechContent();
    
    // Build context messages
    const contextMessages = [
      {
        role: 'system',
        content: 'You are an expert assistant for European Parliament sessions. Answer questions based on the following speech content, which includes a sample of speeches organized by topic. If the answer is not in the content, say so.'
      },
      {
        role: 'user',
        content: `Parliamentary session content (sample speeches by topic):\n\n${optimizedContent}\n\n(Only use the information in this content.)`
      }
    ];

    // Add previous chat history (last 6 messages)
    chatHistory.slice(-6).forEach(msg => contextMessages.push(msg));
    contextMessages.push({ role: 'user', content: question });

    try {
      const response = await fetch('/api/ai/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          messages: contextMessages,
          model: 'gpt-4o-mini',
          temperature: 0.2
        })
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || 'Failed to get AI response');
      }

      const data = await response.json();
      const answer = data.content || 'Sorry, I could not generate a response.';
      
      appendChatMessage(chatWindow, 'assistant', answer);
      chatHistory.push({ role: 'user', content: question });
      chatHistory.push({ role: 'assistant', content: answer });
    } catch (err) {
      appendChatMessage(chatWindow, 'assistant', `Sorry, there was an error: ${err.message}`);
    } finally {
      input.disabled = false;
      sendBtn.disabled = false;
      sendBtn.textContent = 'Send';
      input.focus();
    }
  }

  // Toggle chat visibility
  function toggleChat() {
    const overlay = document.getElementById('aiChatOverlay');
    if (!overlay) return;

    isOpen = !isOpen;
    overlay.style.display = isOpen ? 'flex' : 'none';
    
    if (isOpen) {
      const input = document.getElementById('aiChatInput');
      if (input) {
        setTimeout(() => input.focus(), 100);
      }
    }
  }

  // Initialize: Wait for page to load and get speech content
  function init() {
    // Wait for DOM to be ready
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', init);
      return;
    }

    // Create UI elements
    createFloatingButton();
    createChatInterface();

    // Try to get speech content from the page
    // Check if we're on a speech page and can get content
    const urlParams = new URLSearchParams(window.location.search);
    const speechId = urlParams.get('id');
    
    if (speechId) {
      sittingId = speechId;
      // Try to get content from speechContentMain or fetch it
      const contentEl = document.getElementById('speechContentMain');
      if (contentEl && contentEl.textContent) {
        speechContent = contentEl.textContent;
      } else {
        // Fetch speech content asynchronously
        fetchSpeechContent(speechId);
      }
    }

    // Listen for content updates
    const observer = new MutationObserver(() => {
      const contentEl = document.getElementById('speechContentMain');
      if (contentEl && contentEl.textContent && !speechContent) {
        speechContent = contentEl.textContent;
      }
    });
    
    const contentSection = document.getElementById('speechContentSection');
    if (contentSection) {
      observer.observe(contentSection, { childList: true, subtree: true });
    }
  }

  // Fetch speech content from API and store sitting ID
  async function fetchSpeechContent(speechId) {
    try {
      sittingId = speechId;
      const response = await fetch(`/api/speeches/${encodeURIComponent(speechId)}`);
      if (response.ok) {
        const speech = await response.json();
        // Store raw content as fallback
        if (speech.content) {
          speechContent = speech.content;
        } else if (speech.activity_date) {
          // Fetch HTML content as fallback
          const htmlResponse = await fetch(`/api/speech-html-content?date=${speech.activity_date}&speechId=${encodeURIComponent(speechId)}`);
          if (htmlResponse.ok) {
            const htmlData = await htmlResponse.json();
            if (htmlData.content) {
              speechContent = htmlData.content;
            }
          }
        }
      }
    } catch (error) {
      console.error('Error fetching speech content for AI:', error);
    }
  }

  // Get optimized speech content: 3-4 speeches per topic
  async function getOptimizedSpeechContent() {
    if (!sittingId) {
      // Fallback to raw content if no sitting ID
      return speechContent || 'No content available.';
    }

    try {
      // Fetch individual speeches
      const response = await fetch(`/api/speeches/${encodeURIComponent(sittingId)}/individual`);
      if (!response.ok) {
        return speechContent || 'No content available.';
      }

      const data = await response.json();
      const speeches = data.individual_speeches || [];

      if (speeches.length === 0) {
        return speechContent || 'No content available.';
      }

      // Group speeches by topic (prefer macro_topic, fallback to topic)
      const topicGroups = new Map();
      
      speeches.forEach(speech => {
        const topic = speech.macro_topic || speech.topic || 'Other';
        if (!topicGroups.has(topic)) {
          topicGroups.set(topic, []);
        }
        topicGroups.get(topic).push(speech);
      });

      // Build optimized content: max 3-4 speeches per topic
      const MAX_SPEECHES_PER_TOPIC = 4;
      let optimizedContent = '';

      for (const [topic, topicSpeeches] of topicGroups.entries()) {
        // Take up to MAX_SPEECHES_PER_TOPIC speeches for this topic
        const selectedSpeeches = topicSpeeches.slice(0, MAX_SPEECHES_PER_TOPIC);
        
        optimizedContent += `\n=== Topic: ${topic} ===\n`;
        optimizedContent += `(Showing ${selectedSpeeches.length} of ${topicSpeeches.length} speeches)\n\n`;
        
        selectedSpeeches.forEach((speech, index) => {
          const speaker = speech.speaker_name || 'Unknown Speaker';
          const content = speech.speech_content || 'No content available';
          const group = speech.political_group_std || speech.political_group || '';
          
          optimizedContent += `Speech ${index + 1} - ${speaker}`;
          if (group) optimizedContent += ` (${group})`;
          optimizedContent += `:\n${content}\n\n`;
        });
      }

      return optimizedContent.trim() || speechContent || 'No content available.';
    } catch (error) {
      console.error('Error optimizing speech content:', error);
      // Fallback to raw content on error
      return speechContent || 'No content available.';
    }
  }

  // Public API to set speech content and sitting ID (can be called from speech.js)
  window.setAIChatContent = function(content, id) {
    speechContent = content || '';
    if (id) {
      sittingId = id;
    }
  };

  // Initialize when script loads
  init();
})();
