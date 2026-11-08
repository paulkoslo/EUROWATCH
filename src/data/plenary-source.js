const axios = require("axios");
const { createHash } = require("node:crypto");
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
  if (!Array.isArray(result?.data)) {
    const error = new Error(
      `Unexpected Parliament API response for ${resource}`,
    );
    error.outcome = "parser_error";
    throw error;
  }
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
    if (style === "Contents_TOC" || style === "Contents") {
      englishToc = /^EN\s*CONTENTS$/i.test(text);
    } else if (englishToc && style === "toc 1") {
      let title = "";
      for (const element of paragraph.find("w\\:r")) {
        const run = xml(element);
        if (run.find("w\\:tab").length) break;
        title += run.find("w\\:t").text();
      }
      const match = /^(\d+(?:\.\d+)*)\s*-\s*(.+)$/.exec(title);
      if (match) englishTitles.set(match[1], match[2]);
    } else if (
      ["Heading1Custom", "Heading2Custom", "heading 1", "heading 2"].includes(
        style,
      )
    ) {
      const match = /^(\d+(?:\.\d+)*)\s*\.\s*(.+)$/.exec(text);
      // Older DOCX files put the agenda number in Word TC fields, not visible text.
      const fields = paragraph.find("w\\:instrText").text();
      const fieldNumbers = [
        ...new Set(
          [...fields.matchAll(/TC\s+"(\d+(?:\.\d+)*)\s*-\s*/g)].map(
            (entry) => entry[1],
          ),
        ),
      ];
      if (fieldNumbers.length > 1)
        throw new Error("DOCX heading has conflicting agenda numbers");
      const number = match?.[1] ?? fieldNumbers[0];
      const heading = englishTitles.get(number) ?? match?.[2] ?? text;
      const title = heading.replace(/\s*\([^)]*\)\s*$/, "").trim();
      if (title)
        sections.push({
          heading: number ? `${number}. ${heading}` : heading,
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

// Stable categories are safe to persist; upstream exception text may contain credentials.
function sourceOutcome(error, phase) {
  if (phase === "parse") return "parser_error";
  const status = error.response?.status;
  if (status === 401 || status === 403 || status === 429)
    return "source_refusal";
  if (status === 404) return "not_found";
  return "network_error";
}

async function fetchTranscript(
  sitting,
  get = request,
  { preference = "html", only = false } = {},
) {
  if (!["html", "docx"].includes(preference))
    throw new Error("Unknown transcript source preference");
  const { documentId, published } = sitting;
  const sittingId = `sitting-${sitting.date}`;
  const readerUrl = htmlUrl(documentId);
  const attempts = [];
  const sources = only
    ? [preference]
    : [preference, preference === "html" ? "docx" : "html"];
  for (const source of sources) {
    let phase = "fetch";
    let url = readerUrl;
    const began = Date.now();
    try {
      let payload;
      if (source === "html") {
        payload = await get(url, { responseType: "text" });
      } else {
        url = `${DATA_ORIGIN}/api/v2/documents/${documentId}`;
        const records = await apiRows(get, `documents/${documentId}`);
        const expectedPath = `/distribution/doc/${documentId}_mul.docx`;
        const available = records
          .flatMap((record) => record.is_realized_by ?? [])
          .flatMap((realization) => realization.is_embodied_by ?? [])
          .some(
            ({ is_exemplified_by: value }) =>
              typeof value === "string" &&
              new URL(value, DATA_ORIGIN).href === DATA_ORIGIN + expectedPath,
          );
        if (!available) {
          const error = new Error("No official DOCX distribution listed");
          error.outcome = "distribution_unavailable";
          throw error;
        }
        url = DATA_ORIGIN + expectedPath;
        payload = await get(url, { responseType: "arraybuffer" });
      }
      phase = "parse";
      const parsed =
        source === "html"
          ? parseHtml(payload, sittingId)
          : parseDocx(payload, sittingId);
      attempts.push({
        source,
        url,
        outcome: "available",
        durationMs: Date.now() - began,
      });
      return {
        ...parsed,
        source,
        readerUrl,
        attempts,
        payload,
        provenance: {
          sourceUrl: url,
          format: source,
          retrievedAt: new Date().toISOString(),
          sha256: createHash("sha256")
            .update(
              typeof payload === "string" ? payload : Buffer.from(payload),
            )
            .digest("hex"),
        },
        htmlError: attempts.find(
          (attempt) =>
            attempt.source === "html" && attempt.outcome !== "available",
        )?.outcome,
      };
    } catch (error) {
      attempts.push({
        source,
        url,
        outcome: error.outcome || sourceOutcome(error, phase),
        ...(error.response?.status ? { status: error.response.status } : {}),
        durationMs: Date.now() - began,
      });
    }
  }
  // A refused or broken published document must never be presented as awaiting publication.
  const pending =
    !published &&
    attempts.every((attempt) =>
      ["not_found", "distribution_unavailable"].includes(attempt.outcome),
    );
  if (pending)
    return {
      source: "pending",
      readerUrl,
      attempts,
      htmlError: attempts.find((a) => a.source === "html")?.outcome,
    };
  const error = new Error(
    attempts
      .map((attempt) => `${attempt.source.toUpperCase()}: ${attempt.outcome}`)
      .join("; "),
  );
  error.outcome =
    attempts.find((attempt) => attempt.outcome === "parser_error")?.outcome ||
    attempts.find((attempt) => attempt.outcome === "source_refusal")?.outcome ||
    attempts.find((attempt) => attempt.outcome === "network_error")?.outcome ||
    "distribution_unavailable";
  error.attempts = attempts;
  throw error;
}

module.exports = { discoverSittings, fetchTranscript, sourceOutcome };
