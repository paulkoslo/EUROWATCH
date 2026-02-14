/**
 * Fetches speech content from Europarl HTML plenary documents.
 * Uses core/parliament-fetch for HTTP (correct session by term); this module
 * only extracts text from the HTML (notation anchors, paragraphs, body, TOC).
 */
const cheerio = require('cheerio');
const { fetchSittingHTML } = require('../core/parliament-fetch');

async function fetchSpeechContentFromHTML(date, speechId) {
  const html = await fetchSittingHTML(date);
  if (!html || html.length < 100) return '';

  const $ = cheerio.load(html);

  let content = '';

  const notationMatch = speechId && speechId.match(/(\d+)$/);
  const notationId = notationMatch ? notationMatch[1] : null;

  if (notationId) {
    const anchor = $(`a[name="creitem${notationId}"]`);
    if (anchor.length > 0) {
      let next = anchor[0].nextSibling;
      while (next) {
        if (next.attribs && next.attribs.id && next.attribs.id.startsWith('creitem')) break;
        if (next.type === 'text' || next.name === 'p' || next.name === 'div') {
          content += $(next).text() + '\n';
        }
        next = next.nextSibling;
      }
    }

    if (!content) {
      const anchors = $(`a[name*="${notationId}"]`);
      if (anchors.length > 0) {
        const a = anchors.first();
        let next = a[0].nextSibling;
        while (next) {
          if (next.attribs && next.attribs.id && next.attribs.id.startsWith('creitem')) break;
          if (next.type === 'text' || next.name === 'p' || next.name === 'div') {
            content += $(next).text() + '\n';
          }
          next = next.nextSibling;
        }
      }
    }
  }

  if (!content || content.length < 100) {
    const paragraphs = $('p').toArray().map(p => $(p).text().trim()).filter(Boolean);
    content = paragraphs.join('\n\n');
  }

  if (!content || content.length < 100) {
    content = $('body').text().replace(/\s+/g, ' ').trim();
  }

  if (!content || content.length < 100) {
    try {
      const axios = require('axios');
      const { getSessionNumber } = require('../core/parliament-fetch');
      const session = getSessionNumber(date);
      const tocUrl = `https://www.europarl.europa.eu/doceo/document/CRE-${session}-${date}-TOC_EN.html`;
      const tocResponse = await axios.get(tocUrl, {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; SpeechDashboardBot/1.0)' },
        timeout: 8000
      });
      const $toc = cheerio.load(tocResponse.data);
      const items = $toc('a[href*="ITM-"]').toArray();
      if (items.length > 0) {
        content = `TOC Agenda Items:\n` + items.map(a => $toc(a).text().trim()).join('\n');
      }
    } catch (tocErr) {
      // TOC fetch failed
    }
  }

  content = content.replace(/\n\s*\n/g, '\n\n').trim();
  return content;
}

module.exports = { fetchSpeechContentFromHTML };
