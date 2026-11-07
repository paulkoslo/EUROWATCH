const axios = require("axios");
const { load } = require("cheerio");
const { unzipSync } = require("fflate");
const { getSessionNumber } = require("../core/parliament-fetch");
const { parseSitting } = require("../scripts/step-3-parse-sitting");
const {
  parseIndividualSpeeches,
  bestSectionForSpeech,
} = require("../core/parse-helpers");

const DATA_ORIGIN = "https://data.europarl.europa.eu";
const PAGE_SIZE = 50;
const MAX_DOCUMENT_BYTES = 24 * 1024 * 1024;
const PLENARY_SITTING = "def/ep-activities/PLENARY_SITTING";

function creIdentifier(value, date) {
  const id = typeof value === "string" && value.split("/").at(-1);
  return /^CRE-\d+-\d{4}-\d{2}-\d{2}$/.test(id) && id.endsWith(date)
    ? id
    : null;
}

function htmlUrl(id) {
  return `https://www.europarl.europa.eu/doceo/document/${id}_EN.html`;
}

async function request(url, options = {}) {
  for (let attempt = 1; ; attempt++) {
    try {
      const response = await axios.get(url, {
        timeout: 25000,
        maxContentLength: MAX_DOCUMENT_BYTES,
        headers: { "User-Agent": "Mozilla/5.0 (compatible; EUROWATCH/1.0)" },
        ...options,
      });
      return response.data;
    } catch (error) {
      const status = error.response?.status;
      if (attempt === 3 || (status && status !== 429 && status < 500))
        throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
    }
  }
}

async function apiRows(get, resource, params) {
  const result = await get(`${DATA_ORIGIN}/api/v2/${resource}`, {
    params,
    headers: { Accept: "application/ld+json" },
    responseType: "json",
  });
  if (!Array.isArray(result?.data))
    throw new Error(`Unexpected Parliament API response for ${resource}`);
  return result.data;
}

async function discoverSittings(start, end, get = request) {
  const sittings = [];
  const seen = new Set();
  for (
    let year = Number(start.slice(0, 4));
    year <= Number(end.slice(0, 4));
    year++
  ) {
    for (let offset = 0; ; offset += PAGE_SIZE) {
      const rows = await apiRows(get, "meetings", {
        year,
        limit: PAGE_SIZE,
        offset,
      });
      for (const row of rows) {
        if (typeof row?.had_activity_type !== "string")
          throw new Error(
            `Missing activity type in Parliament calendar for ${year}`,
          );
        if (row.had_activity_type !== PLENARY_SITTING) continue;
        const date = row.activity_date;
        if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !date.startsWith(`${year}-`))
          throw new Error(
            `Unexpected sitting date in Parliament calendar for ${year}`,
          );
        if (seen.has(date))
          throw new Error(`Repeated Parliament calendar date ${date}`);
        seen.add(date);
        if (date < start || date > end) continue;
        const publishedId = (row.recorded_in_a_realization_of ?? [])
          .map((value) => creIdentifier(value, date))
          .find(Boolean);
        const term = /^org\/ep-(\d+)$/.exec(row.parliamentary_term)?.[1];
        sittings.push({
          date,
          documentId:
            publishedId || `CRE-${term || getSessionNumber(date)}-${date}`,
          published: Boolean(publishedId),
          startDate: row.activity_start_date ?? null,
        });
      }
      if (rows.length < PAGE_SIZE) break;
    }
  }
  return sittings.sort((a, b) => a.date.localeCompare(b.date));
}

function parseHtml(content, sittingId) {
  if (typeof content !== "string") throw new Error("Expected transcript HTML");
  const { topics, sections, speeches } = parseSitting(content, sittingId);
  if (!topics.length) throw new Error("HTML has no recognizable agenda");
  if (!speeches.length) throw new Error("HTML has no recognizable speeches");
  return {
    content,
    speeches: speeches.map((speech) => ({
      ...speech,
      topic: bestSectionForSpeech(speech, sections)?.section.title ?? null,
    })),
  };
}

function escapeHtml(text) {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function parseDocx(buffer, sittingId) {
  const files = unzipSync(new Uint8Array(buffer), {
    filter({ name, originalSize }) {
      if (name !== "word/document.xml" && name !== "word/styles.xml")
        return false;
      if (originalSize > MAX_DOCUMENT_BYTES)
        throw new Error("DOCX transcript exceeds the XML size limit");
      return true;
    },
  });
  if (!files["word/document.xml"] || !files["word/styles.xml"])
    throw new Error("DOCX transcript is missing its document or styles");
  const stylesXml = load(Buffer.from(files["word/styles.xml"]).toString(), {
    xml: true,
  });
  const styles = new Map();
  stylesXml("w\\:style").each((_, element) => {
    const style = stylesXml(element);
    styles.set(
      style.attr("w:styleId"),
      style.children("w\\:name").attr("w:val"),
    );
  });
  const xml = load(Buffer.from(files["word/document.xml"]).toString(), {
    xml: true,
  });
  const englishTitles = new Map();
  const sections = [];
  let englishToc = false;
  for (const element of xml("w\\:body w\\:p")) {
    const paragraph = xml(element);
    const style = styles.get(paragraph.find("w\\:pStyle").attr("w:val"));
    const text = paragraph.find("w\\:t").text().trim();
    if (style === "Contents_TOC") {
      englishToc = /^EN\s*CONTENTS$/i.test(text);
    } else if (englishToc && style === "toc 1") {
      let title = "";
      for (const element of paragraph.find("w\\:hyperlink > w\\:r")) {
        const run = xml(element);
        if (run.find("w\\:tab").length) break;
        title += run.find("w\\:t").text();
      }
      const match = /^(\d+(?:\.\d+)*)\s*-\s*(.+)$/.exec(title);
      if (match) englishTitles.set(match[1], match[2]);
    } else if (style === "Heading1Custom" || style === "Heading2Custom") {
      const match = /^(\d+(?:\.\d+)*)\s*\.\s*(.+)$/.exec(text);
      const heading = englishTitles.get(match?.[1]) ?? match?.[2] ?? text;
      const title = heading.replace(/\s*\([^)]*\)\s*$/, "").trim();
      if (title)
        sections.push({
          heading: match ? `${match[1]}. ${heading}` : heading,
          title,
          lines: [],
        });
      englishToc = false;
    } else if (style === "Normal_1" && text && sections.length) {
      sections.at(-1).lines.push(text);
    }
  }
  const speeches = sections.flatMap(({ title, lines }) =>
    parseIndividualSpeeches(lines.join("\n"), sittingId).map((speech) => ({
      ...speech,
      topic: title,
    })),
  );
  if (!speeches.length) throw new Error("DOCX has no recognizable speeches");
  speeches.forEach((speech, index) => {
    speech.speech_order = index + 1;
  });
  // Keep sittings.content as HTML for existing offline readers. DOCX is multilingual.
  const body = sections
    .map(
      ({ heading, lines }) =>
        `<table><tr><td class="doc_title"><img src="arrow_title_doc.gif" alt="">${escapeHtml(heading)}</td></tr></table>\n` +
        lines.map((line) => `<p>${escapeHtml(line)}</p>`).join("\n"),
    )
    .join("\n");
  return {
    content: `<html><body data-eurowatch-source="docx">\n${body}\n</body></html>`,
    speeches,
  };
}

async function fetchTranscript(sitting, get = request) {
  const { date, documentId, published } = sitting;
  const sittingId = `sitting-${date}`;
  let htmlError;
  try {
    const content = await get(htmlUrl(documentId), { responseType: "text" });
    return { ...parseHtml(content, sittingId), source: "html" };
  } catch (error) {
    htmlError = error.message;
  }

  try {
    const records = await apiRows(get, `documents/${documentId}`);
    const expectedPath = `/distribution/doc/${documentId}_mul.docx`;
    const available = records
      .flatMap((record) => record.is_realized_by ?? [])
      .flatMap((realization) => realization.is_embodied_by ?? [])
      .some(({ is_exemplified_by: url }) => {
        return (
          typeof url === "string" &&
          new URL(url, DATA_ORIGIN).href === DATA_ORIGIN + expectedPath
        );
      });
    if (!available) throw new Error("no multilingual DOCX distribution listed");
    const buffer = await get(DATA_ORIGIN + expectedPath, {
      responseType: "arraybuffer",
    });
    return { ...parseDocx(buffer, sittingId), source: "docx", htmlError };
  } catch (error) {
    if (!published && error.response?.status === 404)
      return { source: "pending", htmlError };
    throw new Error(`HTML: ${htmlError}; DOCX: ${error.message}`, {
      cause: error,
    });
  }
}

module.exports = { discoverSittings, fetchTranscript };
