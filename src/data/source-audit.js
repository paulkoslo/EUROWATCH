const { createHash } = require("node:crypto");
const { parseArgs } = require("node:util");
const { discoverSittings, fetchTranscript } = require("./plenary-source");
const { isDate, lastClosedDate } = require("./plenary-import");

const fingerprint = (value) =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");
const normalized = (value) =>
  String(value || "")
    .normalize("NFC")
    .replace(/\s+/g, " ")
    .trim();

function summarize(transcript) {
  const speeches = transcript.speeches || [];
  return {
    source: transcript.source,
    attempts: transcript.attempts,
    speeches: speeches.length,
    orderedTextHash: fingerprint(
      speeches.map((s) => normalized(s.speech_content)),
    ),
    orderedSpeakerHash: fingerprint(
      speeches.map((s) => normalized(s.speaker_name)),
    ),
    orderedAgendaHash: fingerprint(speeches.map((s) => normalized(s.topic))),
    textCharacters: speeches.reduce(
      (sum, s) => sum + normalized(s.speech_content).length,
      0,
    ),
  };
}

async function auditSources({ start, end, request, onEvent = console.log }) {
  if (!isDate(start) || !isDate(end) || start > end || end > lastClosedDate())
    throw new Error("Use valid closed sitting dates for --start and --end");
  const sittings = await discoverSittings(start, end, request);
  onEvent({
    stage: "calendar",
    start,
    end,
    expectedDates: sittings.map((sitting) => sitting.date),
  });
  const results = [];
  for (const sitting of sittings) {
    const sources = {};
    for (const preference of ["html", "docx"]) {
      try {
        sources[preference] = summarize(
          await fetchTranscript(sitting, request, { preference, only: true }),
        );
      } catch (error) {
        sources[preference] = {
          outcome: error.outcome,
          attempts: error.attempts,
        };
      }
    }
    const comparable = [sources.html, sources.docx].every(
      (s) => s.speeches > 0,
    );
    const result = {
      date: sitting.date,
      documentId: sitting.documentId,
      sources,
      comparable,
      comparison: comparable
        ? {
            sameCount: sources.html.speeches === sources.docx.speeches,
            sameOrderedSpeakers:
              sources.html.orderedSpeakerHash ===
              sources.docx.orderedSpeakerHash,
            sameOrderedText:
              sources.html.orderedTextHash === sources.docx.orderedTextHash,
            sameAgendaBoundaries:
              sources.html.orderedAgendaHash === sources.docx.orderedAgendaHash,
          }
        : null,
      limitations:
        "HTML can be translated while DOCX is multilingual. Speaker links are not extracted by either parser; matching hashes do not validate identities.",
    };
    results.push(result);
    onEvent(result);
  }
  return results;
}

if (require.main === module) {
  const { values } = parseArgs({
    options: { start: { type: "string" }, end: { type: "string" } },
  });
  auditSources({
    start: values.start,
    end: values.end || values.start,
    onEvent: (event) => console.log(JSON.stringify(event)),
  }).catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}

module.exports = { auditSources, summarize };
