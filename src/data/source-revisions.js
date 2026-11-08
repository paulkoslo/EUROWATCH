const { all, get, run } = require("./sqlite");

function normalized(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ");
}

function speechKey(name, text) {
  return JSON.stringify([normalized(name).toLowerCase(), normalized(text)]);
}

async function currentRevision(db, sittingId) {
  return get(
    db,
    `
    SELECT r.provider,v.id,v.format,v.payload_sha256 AS sha256,r.official_id AS officialId
    FROM sitting_provenance p JOIN source_revisions v ON v.id=p.revision_id
    JOIN source_records r ON r.id=v.source_id WHERE p.sitting_id=?
  `,
    [sittingId],
  );
}

async function saveRevision(db, sittingId, sitting, transcript) {
  const { documentId } = sitting;
  const { sourceUrl, format, retrievedAt, sha256 } = transcript.provenance;
  await run(
    db,
    `
    INSERT INTO source_records (provider,kind,source_key,official_id)
    VALUES ('europarl','transcript',?,?) ON CONFLICT(provider,kind,source_key) DO NOTHING
  `,
    [documentId, documentId],
  );
  const { id: sourceId } = await get(
    db,
    `
    SELECT id FROM source_records WHERE provider='europarl' AND kind='transcript' AND source_key=?
  `,
    [documentId],
  );
  await run(
    db,
    "INSERT INTO sitting_sources VALUES (?,?) ON CONFLICT(sitting_id,source_id) DO NOTHING",
    [sittingId, sourceId],
  );
  await run(
    db,
    `
    INSERT INTO source_revisions (source_id,format,payload_sha256,source_url,retrieved_at,payload)
    VALUES (?,?,?,?,?,?) ON CONFLICT(source_id,format,payload_sha256) DO NOTHING
  `,
    [
      sourceId,
      format,
      sha256,
      sourceUrl,
      retrievedAt,
      Buffer.from(transcript.payload),
    ],
  );
  const { id } = await get(
    db,
    `
    SELECT id FROM source_revisions WHERE source_id=? AND format=? AND payload_sha256=?
  `,
    [sourceId, format, sha256],
  );
  await run(
    db,
    `
    INSERT INTO sitting_provenance VALUES (?,?)
    ON CONFLICT(sitting_id) DO UPDATE SET revision_id=excluded.revision_id
  `,
    [sittingId, id],
  );
  return id;
}

async function linkSpeech(db, speechId, revisionId, officialId) {
  await run(
    db,
    `
    INSERT INTO speech_provenance VALUES (?,?,?)
    ON CONFLICT(speech_id) DO UPDATE SET revision_id=excluded.revision_id, official_id=excluded.official_id
  `,
    [speechId, revisionId, officialId ?? null],
  );
}

async function matchSpeeches(db, sittingId, speeches) {
  const previous = await all(
    db,
    `
    SELECT i.id,i.speaker_name,i.speech_content,i.speech_order,p.official_id
    FROM individual_speeches i LEFT JOIN speech_provenance p ON p.speech_id=i.id
    WHERE i.sitting_id=? ORDER BY i.speech_order,i.id
  `,
    [sittingId],
  );
  const unmatched = new Set(previous);
  const byOfficialId = new Map(
    previous.filter((s) => s.official_id).map((s) => [s.official_id, s]),
  );
  const byText = new Map();
  for (const speech of previous) {
    const key = speechKey(speech.speaker_name, speech.speech_content);
    if (!byText.has(key)) byText.set(key, []);
    byText.get(key).push(speech);
  }
  const matches = speeches.map((speech) => {
    const exact =
      byText.get(speechKey(speech.speaker_name, speech.speech_content)) || [];
    const match =
      (speech.officialId && byOfficialId.get(speech.officialId)) ||
      (exact.length === 1
        ? exact[0]
        : exact.find((row) => row.speech_order === speech.speech_order));
    if (!unmatched.delete(match)) return null;
    return match.id;
  });

  // Only use position for an edited contribution when the shape of the sitting
  // is unchanged and the remaining speaker at that position also matches.
  if (previous.length === speeches.length) {
    const byOrder = new Map(
      [...unmatched].map((row) => [row.speech_order, row]),
    );
    for (const [index, speech] of speeches.entries()) {
      if (matches[index]) continue;
      const row = byOrder.get(speech.speech_order);
      if (
        !row ||
        normalized(row.speaker_name) !== normalized(speech.speaker_name)
      )
        continue;
      if (
        row.official_id &&
        speech.officialId &&
        row.official_id !== speech.officialId
      )
        continue;
      if (unmatched.delete(row)) matches[index] = row.id;
    }
  }
  if (unmatched.size)
    throw new Error(
      "Transcript revision has unmatched existing contributions. Review its source and speaker alignment before publishing.",
    );
  return matches;
}

module.exports = { currentRevision, saveRevision, linkSpeech, matchSpeeches };
