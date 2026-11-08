const fs = require("node:fs");
const path = require("node:path");
const { createHash, randomUUID } = require("node:crypto");
const { open, all, get, exec, close } = require("../data/sqlite");
const { hash } = require("./model");
const { sampler } = require("./pilot");

const sha = (value) => createHash("sha256").update(value).digest("hex");
function parliamentUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === "https:" &&
      (url.hostname === "europarl.europa.eu" ||
        url.hostname.endsWith(".europarl.europa.eu"))
      ? url.href
      : null;
  } catch {
    return null;
  }
}
function sourceMetadata(sitting) {
  const content = sitting.content || "";
  let sourceUrl = parliamentUrl(sitting.source_url);
  if (!sourceUrl) {
    for (const link of content.match(/<link\b[^>]*>/gi) || []) {
      if (!/\brel\s*=\s*["']canonical["']/i.test(link)) continue;
      sourceUrl = parliamentUrl(
        link.match(/\bhref\s*=\s*["']([^"']+)["']/i)?.[1],
      );
      if (sourceUrl) break;
    }
  }
  const term =
    content.match(
      /<meta\b[^>]*name=["']sipade-leg["'][^>]*content=["'](\d+)["']/i,
    )?.[1] || sourceUrl?.match(/\/CRE-(\d+)-/)?.[1];
  return {
    sourceUrl,
    term: term ? Number(term) : null,
    sourceRevision: `sha256:${sitting.payload_sha256 || sha(JSON.stringify(content))}`,
  };
}
function validDate(value) {
  return (
    typeof value === "string" &&
    /^\d{4}-\d{2}-\d{2}$/.test(value) &&
    Number.isFinite(Date.parse(value)) &&
    new Date(value).toISOString().slice(0, 10) === value
  );
}

function makeRecords(
  sitting,
  rows,
  {
    maxChars = 24000,
    maxPassages = 80,
    kinds = ["debate", "sitting", "speech"],
  } = {},
) {
  if (
    !Number.isInteger(maxChars) ||
    maxChars < 256 ||
    !Number.isInteger(maxPassages) ||
    maxPassages < 1
  )
    throw new Error("Positive evidence bounds required (maxChars >= 256)");
  const source = sourceMetadata(sitting);
  if (!source.sourceUrl)
    return {
      records: [],
      diagnostics: [
        {
          sittingId: sitting.id,
          reason: "missing_source_url",
          speeches: rows.length,
        },
      ],
    };
  if (!validDate(sitting.activity_date))
    return {
      records: [],
      diagnostics: [
        {
          sittingId: sitting.id,
          reason: "invalid_activity_date",
          speeches: rows.length,
        },
      ],
    };
  const diagnostics = [],
    passages = [],
    groups = [];
  let group;
  for (const row of rows) {
    const title = row.topic?.trim() || "Unassigned agenda";
    if (!group || group.title !== title) {
      group = { id: `debate:${sitting.id}:${row.id}`, title, passages: [] };
      groups.push(group);
    }
    if (!row.speech_content?.trim()) {
      diagnostics.push({ speechId: row.id, reason: "empty_text" });
      continue;
    }
    const chars = Array.from(row.speech_content);
    const speechId = `speech:${row.id}`;
    for (let offset = 0; offset < chars.length; offset += maxChars) {
      const passage = {
        id: chars.length > maxChars ? `${speechId}:part:${offset}` : speechId,
        speechId,
        text: chars.slice(offset, offset + maxChars).join(""),
        speakerId: row.mep_id == null ? null : `mep:${row.mep_id}`,
        speakerName: row.speaker_name || null,
        sourceUrl: parliamentUrl(row.source_url) || source.sourceUrl,
        sourceRevision: row.payload_sha256
          ? `sha256:${row.payload_sha256}`
          : source.sourceRevision,
        language: row.language || "unknown",
        officialIds: row.official_id ? [row.official_id] : [],
        legacyLabels: row.macro_topic
          ? [
              {
                label: row.macro_topic,
                confidence: row.macro_confidence,
                model: row.macro_classified_by || "legacy",
                effectiveAt: row.macro_classified_at || null,
              },
            ]
          : [],
      };
      passages.push(passage);
      group.passages.push(passage);
    }
  }
  const records = [];
  function emit(kind, id, agendaTitle, members, officialIds) {
    const chunks = [];
    let chunk = [],
      size = 0;
    for (const p of members) {
      if (
        chunk.length &&
        (size + Array.from(p.text).length > maxChars ||
          chunk.length >= maxPassages)
      ) {
        chunks.push(chunk);
        chunk = [];
        size = 0;
      }
      chunk.push(p);
      size += Array.from(p.text).length;
    }
    if (chunk.length) chunks.push(chunk);
    chunks.forEach((selected, index) => {
      const languages = [...new Set(selected.map((p) => p.language))];
      const record = {
        id: chunks.length > 1 ? `${id}:part:${selected[0].id}` : id,
        parentId: id,
        sittingId: sitting.id,
        kind,
        inputRevision: hash({
          sourceRevision: source.sourceRevision,
          selected,
          officialIds,
        }),
        effectiveAt: `${sitting.activity_date}T00:00:00.000Z`,
        effectiveTimePrecision: "day",
        term: source.term,
        language: languages.length === 1 ? languages[0] : "mul",
        agendaTitle,
        officialIds,
        passages: selected,
        legacyLabels: selected.flatMap((p) => p.legacyLabels),
        scope: {
          complete: chunks.length === 1 && !diagnostics.length,
          part: index + 1,
          parts: chunks.length,
          selectedPassages: selected.length,
          totalPassages: members.length,
          omittedEmptySpeeches: diagnostics.length,
          agendaMembership:
            kind === "debate" ? "legacy_contiguous_heading" : null,
        },
      };
      records.push(record);
    });
  }
  if (kinds.includes("speech")) {
    const speechGroups = new Map();
    for (const p of passages) {
      if (!speechGroups.has(p.speechId)) speechGroups.set(p.speechId, []);
      speechGroups.get(p.speechId).push(p);
    }
    for (const [id, members] of speechGroups)
      emit(
        "speech",
        id,
        rows.find((r) => `speech:${r.id}` === id)?.topic || "Unassigned agenda",
        members,
        members[0].officialIds,
      );
  }
  // Transcript/sitting identity is NOT a debate/procedure identity. Do not
  // propagate it to debate groups or infer adopted texts from shared dates.
  if (kinds.includes("debate"))
    for (const g of groups) emit("debate", g.id, g.title, g.passages, []);
  if (kinds.includes("sitting"))
    emit(
      "sitting",
      sitting.id,
      sitting.label || sitting.activity_date,
      passages,
      sitting.official_id ? [sitting.official_id] : [],
    );
  return { records, diagnostics };
}

async function* streamCorpus({ filename, from, to, ...limits }) {
  if (
    (from && !validDate(from)) ||
    (to && !validDate(to)) ||
    (from && to && from > to)
  )
    throw new Error("Invalid corpus date range");
  const db = await open(filename, { readonly: true });
  try {
    await exec(db, "PRAGMA query_only=ON; BEGIN;");
    const canonical = await get(
      db,
      "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='canonical_migrations'",
    );
    if (canonical) {
      const version = await get(
        db,
        "SELECT MAX(version) AS version FROM canonical_migrations",
      );
      if (version.version !== 1)
        throw new Error("Unsupported canonical schema version");
    }
    const columns = new Set(
      (await all(db, "PRAGMA table_info(individual_speeches)")).map(
        (c) => c.name,
      ),
    );
    const optional = [
      "mep_id",
      "language",
      "topic",
      "macro_topic",
      "macro_confidence",
      "macro_classified_by",
      "macro_classified_at",
    ]
      .map((c) => (columns.has(c) ? `i.${c}` : `NULL AS ${c}`))
      .join(",");
    let after = "";
    while (true) {
      const sittings = await all(
        db,
        `SELECT s.id,s.activity_date,s.label ${canonical ? ",v.payload_sha256,v.source_url,CASE WHEN r.provider!='legacy' THEN r.official_id END AS official_id" : ""} FROM sittings s ${canonical ? "LEFT JOIN sitting_provenance p ON p.sitting_id=s.id LEFT JOIN source_revisions v ON v.id=p.revision_id LEFT JOIN source_records r ON r.id=v.source_id" : ""} WHERE s.id>? AND (? IS NULL OR s.activity_date>=?) AND (? IS NULL OR s.activity_date<=?) ORDER BY s.id LIMIT 64`,
        [after, from || null, from || null, to || null, to || null],
      );
      if (!sittings.length) break;
      for (const sitting of sittings) {
        const content = await get(
          db,
          "SELECT content FROM sittings WHERE id=?",
          [sitting.id],
        );
        const rows = await all(
          db,
          `SELECT i.id,i.speaker_name,i.speech_content,i.speech_order,${optional} ${canonical ? ",v.payload_sha256,v.source_url,CASE WHEN r.provider!='legacy' THEN p.official_id END AS official_id" : ""} FROM individual_speeches i ${canonical ? "LEFT JOIN speech_provenance p ON p.speech_id=i.id LEFT JOIN source_revisions v ON v.id=p.revision_id LEFT JOIN source_records r ON r.id=v.source_id" : ""} WHERE i.sitting_id=? ORDER BY i.speech_order,i.id`,
          [sitting.id],
        );
        yield {
          sittingId: sitting.id,
          sourceKind: canonical ? "canonical-v1" : "legacy",
          ...makeRecords(
            { ...sitting, content: content.content },
            rows,
            limits,
          ),
        };
      }
      after = sittings.at(-1).id;
    }
  } finally {
    await close(db);
  }
}

async function exportCorpus({
  output,
  config,
  officialEvidence = [],
  sample,
  ...options
}) {
  if (path.resolve(output) === path.resolve(options.filename))
    throw new Error("Output must not replace source archive");
  fs.mkdirSync(path.dirname(path.resolve(output)), { recursive: true });
  const temp = `${output}.${randomUUID()}.tmp`;
  const fd = fs.openSync(temp, "wx", 0o600);
  let count = 0,
    sittings = 0;
  const diagnostics = [];
  const selected = sample === undefined ? null : sampler(sample);
  try {
    const limits = {
      maxChars: options.maxChars,
      maxPassages: options.maxPassages,
    };
    fs.writeSync(
      fd,
      `{"limits":${JSON.stringify(limits)},"config":${JSON.stringify(config)},"officialEvidence":${JSON.stringify(officialEvidence)},"records":[`,
    );
    for await (const batch of streamCorpus(options)) {
      sittings++;
      diagnostics.push(...batch.diagnostics);
      for (const record of batch.records) {
        if (selected) selected.add(record);
        else fs.writeSync(fd, `${count++ ? "," : ""}${JSON.stringify(record)}`);
      }
    }
    for (const record of selected?.records() ?? [])
      fs.writeSync(fd, `${count++ ? "," : ""}${JSON.stringify(record)}`);
    fs.writeSync(
      fd,
      `],"exportReport":${JSON.stringify({ records: count, sittings, diagnostics })}}\n`,
    );
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fs.renameSync(temp, output);
    return { records: count, sittings, diagnostics };
  } catch (error) {
    try {
      fs.closeSync(fd);
    } catch {
      /* closed before rename */
    }
    fs.rmSync(temp, { force: true });
    throw error;
  }
}

if (require.main === module) {
  const { options } = require("./cli");
  const opts = options(process.argv.slice(2));
  if (!opts.db || !opts.config || !opts.output)
    throw new Error(
      "Use --db archive.db --config config.json --output corpus.json",
    );
  exportCorpus({
    filename: opts.db,
    output: opts.output,
    config: JSON.parse(fs.readFileSync(opts.config, "utf8")),
    officialEvidence: opts.evidence
      ? JSON.parse(fs.readFileSync(opts.evidence, "utf8"))
      : [],
    from: opts.from,
    to: opts.to,
    maxChars: Number(opts["max-chars"] || 24000),
    maxPassages: Number(opts["max-passages"] || 80),
    kinds: opts.kinds ? opts.kinds.split(",") : undefined,
    sample: opts.sample === undefined ? undefined : Number(opts.sample),
  })
    .then((report) => console.log(JSON.stringify(report)))
    .catch((error) => {
      console.error(error.message);
      process.exitCode = 1;
    });
}
module.exports = { sourceMetadata, makeRecords, streamCorpus, exportCorpus };
