/**
 * Shared parsing helpers for sitting HTML and speech extraction
 */

const cheerio = require('cheerio');

function normalizeText(text) {
  return (text || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeForSearch(text) {
  return (text || '')
    .toLowerCase()
    .replace(/[\u00A0]/g, ' ')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseTopicsFromHTML(html) {
  const $ = cheerio.load(html);
  const topics = [];
  $('td.doc_title:has(img[src*="arrow_title_doc.gif"])').each((_, cell) => {
    const td = $(cell);
    const raw = td.text().replace(/\s+/g, ' ').trim();
    if (!raw) return;
    const ordinalMatch = raw.match(/^(\d+(?:\.\d+)*)\s*\./);
    const ordinal = ordinalMatch ? ordinalMatch[1] : null;
    let docIdentifier = null;
    const link = td.find('a[href*="/doceo/document/"]').filter((i, a) => /_EN\.html$/i.test($(a).attr('href') || ''))[0];
    if (link) {
      const href = $(link).attr('href');
      const idMatch = href && href.match(/\/doceo\/document\/([^/_]+(?:-[^/_]+)*)_EN\.html/i);
      if (idMatch) docIdentifier = idMatch[1];
    }
    let title = raw;
    if (ordinal) title = title.replace(new RegExp('^' + ordinal.replace('.', '\\.') + '\\.'), '').trim();
    title = title.replace(/\s*\([^)]*\)\s*$/, '').trim();
    if (!title) return;
    topics.push({ ordinal, title, docIdentifier, raw });
  });
  const seen = new Set();
  return topics.filter(t => {
    const key = `${t.docIdentifier || ''}::${normalizeText(t.title)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function splitHtmlIntoSections(html) {
  const $ = cheerio.load(html);
  const headers = $('td.doc_title:has(img[src*="arrow_title_doc.gif"])').toArray();
  const sections = [];
  if (headers.length === 0) return sections;
  const rawHtml = $.root().html();
  let searchFrom = 0;
  const headerSpans = headers.map((el) => {
    const tdHtml = $.html(el);
    const start = rawHtml.indexOf(tdHtml, searchFrom);
    if (start !== -1) searchFrom = start + tdHtml.length;
    const rawText = $(el).text().replace(/\s+/g, ' ').trim();
    let title = rawText.replace(/^(\d+(?:\.\d+)*)\s*\./, '').trim();
    title = title.replace(/\s*\([^)]*\)\s*$/, '').trim();
    let docIdentifier = null;
    const link = $(el).find('a[href*="/doceo/document/"]').filter((i, a) => /_EN\.html$/i.test($(a).attr('href') || ''))[0];
    if (link) {
      const href = $(link).attr('href');
      const idMatch = href && href.match(/\/doceo\/document\/([^/_]+(?:-[^/_]+)*)_EN\.html/i);
      if (idMatch) docIdentifier = idMatch[1];
    }
    return { start, title, docIdentifier };
  }).filter(h => h.start !== -1);

  for (let i = 0; i < headerSpans.length; i++) {
    const start = headerSpans[i].start;
    const end = i + 1 < headerSpans.length ? headerSpans[i + 1].start : rawHtml.length;
    const slice = rawHtml.slice(start, end);
    const text = cheerio.load(slice)('body').text().replace(/\s+/g, ' ').trim();
    sections.push({ title: headerSpans[i].title, docIdentifier: headerSpans[i].docIdentifier, text });
  }
  return sections;
}

function extractTextFromHTML(html) {
  const $ = cheerio.load(html);
  const main = $('.doc-content, .ep_text, .content, #content, .main-content').first();
  if (main.length > 0 && main.text().trim().length > 100) return main.text().trim();
  const body = $('body').text().trim();
  if (body.length > 100) return body;
  return $('p').map((i, el) => $(el).text()).get().join('\n').trim();
}

function parseIndividualSpeeches(content, sittingId) {
  const speeches = [];
  const lines = content.split('\n');
  let currentSpeech = null;
  let speechOrder = 0;
  const partyPattern = /^(PPE|S&D|ECR|Renew|Verts\/ALE|ID|The Left|NI|ALDE)$/i;
  const partyIndicators = ['on behalf of', 'au nom de', 'a nome del', 'en nombre del', 'im Namen der', 'fraktion', 'gruppo', 'grupo', 'group', 'groupe', 'εξ ονόματος', 'namens', 'w imieniu', 'în numele', 'for ', 'för ', 'thar ceann', 'u ime', 'za skupinu', 'em nome', "(PPE)", "(S&D)", "(ECR)", "(Renew)", "(Verts/ALE)", "(ID)", "(The Left)", "(NI)", "(ALDE)"];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    let speechMatch = line.match(/^([^,]+),\s*(.+?)\.\s*–\s*(.+)$/);
    let speakerName, roleInfo, speechContent;

    if (speechMatch) {
      speakerName = speechMatch[1].trim();
      roleInfo = speechMatch[2].trim();
      speechContent = speechMatch[3].trim();
    } else {
      speechMatch = line.match(/^([^(]+)\s*\(([^)]+)\)\.\s*–\s*(.+)$/);
      if (speechMatch) {
        speakerName = speechMatch[1].trim();
        roleInfo = speechMatch[2].trim();
        speechContent = speechMatch[3].trim();
      } else {
        speechMatch = line.match(/^([^(]+)\s*\(([^)]+)\),\s*(.+?)\.\s*–\s*(.+)$/);
        if (speechMatch) {
          speakerName = speechMatch[1].trim();
          roleInfo = speechMatch[3].trim();
          speechContent = speechMatch[4].trim();
        } else {
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
      if (currentSpeech) speeches.push(currentSpeech);
      let politicalGroup = null;
      let title = null;
      const nameWithPartyMatch = speakerName.match(/^(.+?)\s*\(([^)]+)\)$/);
      if (nameWithPartyMatch && partyPattern.test(nameWithPartyMatch[2].trim())) {
        politicalGroup = nameWithPartyMatch[2].trim();
        speakerName = nameWithPartyMatch[1].trim();
      }
      if (!politicalGroup && roleInfo && partyPattern.test(roleInfo)) politicalGroup = roleInfo;
      else if (roleInfo && partyIndicators.some(p => roleInfo.includes(p))) politicalGroup = roleInfo;
      else if (roleInfo) title = roleInfo;

      currentSpeech = {
        sitting_id: sittingId,
        speaker_name: speakerName,
        political_group: politicalGroup,
        title,
        speech_content: speechContent,
        speech_order: ++speechOrder,
        mep_id: null
      };
    } else if (currentSpeech) {
      currentSpeech.speech_content += ' ' + line;
    }
  }
  if (currentSpeech) speeches.push(currentSpeech);
  return speeches;
}

function bestSectionForSpeech(speech, rangedSections) {
  const content = speech.speech_content || '';
  const speechNorm = normalizeForSearch(content);
  if (!speechNorm || speechNorm.length < 50) return null;

  const candidates = [];
  for (const s of [0, 40, 80, 120]) {
    const snip = speechNorm.slice(s, s + 160);
    if (snip.length >= 80) candidates.push(snip);
  }
  for (const sec of rangedSections) {
    if (!sec.textNorm || sec.textNorm.length < 100) continue;
    if (candidates.some(sn => sec.textNorm.indexOf(sn) !== -1))
      return { section: { title: sec.title, docIdentifier: sec.docIdentifier }, score: 1.0 };
  }

  const speechTokens = speechNorm.split(' ').filter(Boolean);
  const speechSet = new Set(speechTokens);
  let best = null, bestScore = 0;
  for (const sec of rangedSections) {
    if (!sec.textNorm) continue;
    let hit = 0;
    for (const tok of speechSet) {
      if (tok.length <= 3) continue;
      if (sec.textNorm.indexOf(tok) !== -1) hit++;
    }
    const score = hit / Math.max(1, speechSet.size);
    if (score > bestScore) { bestScore = score; best = sec; }
  }
  if (!best || bestScore < 0.08) return null;
  return { section: { title: best.title, docIdentifier: best.docIdentifier }, score: bestScore };
}

module.exports = {
  normalizeText,
  normalizeForSearch,
  parseTopicsFromHTML,
  splitHtmlIntoSections,
  extractTextFromHTML,
  parseIndividualSpeeches,
  bestSectionForSpeech
};
