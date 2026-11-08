const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { strToU8, zipSync } = require("fflate");
const { all, close, exec, get, open } = require("../src/data/sqlite");
const { migrate } = require("../src/data/schema");
const {
  discoverSittings,
  fetchTranscript,
} = require("../src/data/plenary-source");
const { importSittings } = require("../src/data/plenary-import");

const date = "2026-03-09";
const next = "2026-03-10";
const start = "2026-03-08";
const text =
  "We should discuss how the European Parliament can support international research.";
const html = `<html><body>
<table><tr><td class="doc_title"><img src="arrow_title_doc.gif">1. European research</td></tr></table>
<p>Ada Example (ESN). – ${text}</p>
<p>Another Speaker (PfE). – ${text}</p>
</body></html>`;
const activityType = "def/ep-activities/PLENARY_SITTING";
const meeting = (day, { term = 10, published = true } = {}) => ({
  activity_date: day,
  activity_start_date: `${day}T17:00:00+01:00`,
  had_activity_type: activityType,
  parliamentary_term: `org/ep-${term}`,
  recorded_in_a_realization_of: published
    ? [`eli/dl/doc/CRE-${term}-${day}`]
    : [],
});
const status = (code) =>
  Object.assign(new Error(`HTTP ${code}`), { response: { status: code } });

test("discovery paginates calendar records and uses the Parliament transcript identifier", async () => {
  const offsets = [];
  const days = await discoverSittings(start, next, async (url, { params }) => {
    assert.match(url, /\/meetings$/);
    assert.equal(params.year, 2026);
    offsets.push(params.offset);
    return {
      data: params.offset
        ? [meeting(date, { term: 9 }), meeting(next, { published: false })]
        : Array.from({ length: params.limit }, () => ({
            had_activity_type: "other",
          })),
    };
  });
  assert.deepEqual(offsets, [0, 50]);
  assert.deepEqual(
    days.map(({ date, documentId, published }) => [
      date,
      documentId,
      published,
    ]),
    [
      [date, `CRE-9-${date}`, true],
      [next, `CRE-10-${next}`, false],
    ],
  );
  await assert.rejects(
    discoverSittings(start, next, async () => "<html>blocked</html>"),
    /Unexpected Parliament API response/,
  );
});

test("parses HTML before requesting any fallback", async () => {
  const sitting = { date, documentId: `CRE-9-${date}` };
  const result = await fetchTranscript(sitting, async (url) => {
    assert.match(url, /CRE-9-2026-03-09_EN\.html$/);
    return html;
  });
  assert.equal(result.source, "html");
  assert.equal(result.content, html);
  assert.deepEqual(
    result.speeches.map(({ political_group, topic }) => [
      political_group,
      topic,
    ]),
    [
      ["ESN", "European research"],
      ["PfE", "European research"],
    ],
  );
});

test("falls back from a blocked HTML page to the official multilingual DOCX", async () => {
  const documentId = `CRE-10-${date}`;
  const path = `distribution/doc/${documentId}_mul.docx`;
  const styles = [
    ["h", "Heading1Custom"],
    ["speech", "Normal_1"],
    ["toc", "toc 1"],
    ["contents", "Contents_TOC"],
  ];
  const p = (style, contents) =>
    `<w:p><w:pPr><w:pStyle w:val="${style}"/></w:pPr>${contents}</w:p>`;
  const t = (text) => `<w:r><w:t>${text}</w:t></w:r>`;
  const archive = zipSync({
    "word/styles.xml": strToU8(
      `<w:styles>${styles
        .map(
          ([id, name]) =>
            `<w:style w:styleId="${id}"><w:name w:val="${name}"/></w:style>`,
        )
        .join("")}</w:styles>`,
    ),
    "word/document.xml": strToU8(`<w:document><w:body>
      ${p("contents", t("EN") + t("CONTENTS"))}
      ${p("toc", `<w:hyperlink>${t("1 - European ")}${t("research (debate)")}<w:r><w:tab/></w:r>${t("22")}</w:hyperlink>`)}
      ${p("h", t("1. Europäische Forschung"))}
      ${p("speech", t("Ada Example (ESN). – ") + t("Wortlaut &amp; Zahlen."))}
      ${p("speech", t("Weitere Informationen."))}
      ${p("h", t("2. Next topic"))}
      ${p("speech", t("Another Speaker (PfE). – Next contribution."))}
    </w:body></w:document>`),
  });
  const result = await fetchTranscript(
    { date, documentId, published: true },
    async (url) => {
      if (url.endsWith(".html")) throw status(403);
      if (url.endsWith(`/documents/${documentId}`))
        return {
          data: [
            {
              is_realized_by: [
                { is_embodied_by: [{ is_exemplified_by: path }] },
              ],
            },
          ],
        };
      assert.equal(url, `https://data.europarl.europa.eu/${path}`);
      return archive;
    },
  );
  assert.equal(result.source, "docx");
  assert.match(result.content, /data-eurowatch-source="docx"/);
  assert.match(result.content, /Wortlaut &amp; Zahlen/);
  assert.deepEqual(
    result.speeches.map((s) => [s.speech_order, s.topic, s.speech_content]),
    [
      [1, "European research", "Wortlaut & Zahlen. Weitere Informationen."],
      [2, "Next topic", "Next contribution."],
    ],
  );
});

test("dry run distinguishes pending publication and does not open a database", async () => {
  const result = await importSittings({
    start,
    end: next,
    dryRun: true,
    log() {},
    request: async (url) => {
      if (url.endsWith("/meetings"))
        return { data: [meeting(date, { published: false }), meeting(next)] };
      if (url.endsWith(`CRE-10-${next}_EN.html`)) return html;
      throw status(404);
    },
  });
  assert.deepEqual(
    Object.fromEntries(
      Object.entries(result).filter(([key]) => key !== "items"),
    ),
    {
      confirmed: 2,
      imported: 0,
      previewed: 1,
      existing: 0,
      pending: 1,
      failed: 0,
    },
  );
});

test("a failed sitting rolls back fully and a rerun fills the gap once", async (t) => {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "eurowatch-import-test-"),
  );
  const filename = path.join(root, "staging.db");
  const db = await open(filename);
  t.after(async () => {
    await close(db);
    await fs.rm(root, { recursive: true, force: true });
  });
  await migrate(db);
  await exec(
    db,
    `
    INSERT INTO meps (id, label) VALUES (1, 'Ada EXAMPLE'), (2, 'Another Speaker'), (3, 'Another Speaker');
    CREATE TRIGGER stop_second_speech BEFORE INSERT ON individual_speeches
    WHEN NEW.sitting_id = 'sitting-${date}' AND NEW.speech_order = 2
    BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END;
  `,
  );
  const options = {
    start,
    end: next,
    filename,
    log() {},
    request: async (url) =>
      url.endsWith("/meetings")
        ? { data: [meeting(date), meeting(next)] }
        : html,
  };
  const first = await importSittings(options);
  assert.equal(first.failed, 1);
  assert.equal(first.imported, 1);
  for (const table of ["sittings", "speeches", "individual_speeches"]) {
    const column = table === "individual_speeches" ? "sitting_id" : "id";
    assert.equal(
      (
        await get(db, `SELECT COUNT(*) AS n FROM ${table} WHERE ${column}=?`, [
          `sitting-${date}`,
        ])
      ).n,
      0,
    );
  }
  await exec(db, "DROP TRIGGER stop_second_speech");
  const rerun = await importSittings(options);
  assert.equal(rerun.imported, 1);
  assert.equal(rerun.existing, 1);
  assert.equal((await importSittings(options)).existing, 2);
  const rows = await all(
    db,
    `
    SELECT mep_id, language, topic, macro_topic, political_group_std FROM individual_speeches
    WHERE sitting_id=? ORDER BY speech_order
  `,
    [`sitting-${date}`],
  );
  assert.deepEqual(rows, [
    {
      mep_id: 1,
      language: "EN",
      topic: "European research",
      macro_topic: null,
      political_group_std: "ESN",
    },
    {
      mep_id: null,
      language: "EN",
      topic: "European research",
      macro_topic: null,
      political_group_std: "PfE",
    },
  ]);
  assert.equal(
    (
      await get(
        db,
        "SELECT docIdentifier FROM sittings WHERE activity_date=?",
        [date],
      )
    ).docIdentifier,
    `CRE-10-${date}`,
  );

  await exec(
    db,
    "INSERT INTO dataset_metadata (key, value) VALUES ('release', '{}')",
  );
  await assert.rejects(
    importSittings(options),
    /Prepared releases are read-only/,
  );
});

test("DOCX preference avoids reader requests and retains original payload provenance", async () => {
  const archive = zipSync({
    "word/styles.xml": strToU8(
      '<w:styles><w:style w:styleId="h"><w:name w:val="Heading1Custom"/></w:style><w:style w:styleId="s"><w:name w:val="Normal_1"/></w:style></w:styles>',
    ),
    "word/document.xml": strToU8(
      '<w:document><w:body><w:p><w:pPr><w:pStyle w:val="h"/></w:pPr><w:r><w:t>1. Research</w:t></w:r></w:p><w:p><w:pPr><w:pStyle w:val="s"/></w:pPr><w:r><w:t>Ada Example (PPE). – Research matters.</w:t></w:r></w:p></w:body></w:document>',
    ),
  });
  const result = await fetchTranscript(
    { date, documentId: `CRE-10-${date}`, published: true },
    async (url) => {
      assert.ok(!url.endsWith(".html"));
      if (url.endsWith(".docx")) return archive;
      return {
        data: [
          {
            is_realized_by: [
              {
                is_embodied_by: [
                  {
                    is_exemplified_by: `distribution/doc/CRE-10-${date}_mul.docx`,
                  },
                ],
              },
            ],
          },
        ],
      };
    },
    { preference: "docx" },
  );
  assert.equal(result.source, "docx");
  assert.equal(result.speeches.length, 1);
  assert.match(result.readerUrl, /_EN.html$/);
  assert.equal(
    result.provenance.sha256,
    require("node:crypto").createHash("sha256").update(archive).digest("hex"),
  );
  assert.deepEqual(result.payload, archive);
});

test("typed source failures distinguish refusal, parsing, network and pending publication without leaking error text", async () => {
  const sitting = { date, documentId: `CRE-10-${date}`, published: false };
  for (const [failure, expected] of [
    [status(403), "source_refusal"],
    [
      Object.assign(new Error("secret-token-in-upstream-url"), {
        code: "ETIMEDOUT",
      }),
      "network_error",
    ],
  ]) {
    await assert.rejects(
      fetchTranscript(sitting, async () => {
        throw failure;
      }),
      (error) => {
        assert.equal(error.outcome, expected);
        assert.equal(error.attempts.length, 2);
        assert.ok(!JSON.stringify(error.attempts).includes("secret-token"));
        assert.ok(!error.message.includes("secret-token"));
        return true;
      },
    );
  }
  await assert.rejects(
    fetchTranscript(sitting, async () => "malformed", { only: true }),
    (error) => error.outcome === "parser_error",
  );
  const pending = await fetchTranscript(sitting, async () => {
    throw status(404);
  });
  assert.equal(pending.source, "pending");
  await assert.rejects(
    fetchTranscript({ ...sitting, published: true }, async () => {
      throw status(404);
    }),
    (error) => error.outcome === "distribution_unavailable",
  );
});

test("calendar reconciliation reports non-sitting days separately from pending expected sittings", async () => {
  const events = [];
  const result = await importSittings({
    start,
    end: date,
    dryRun: true,
    log() {},
    onEvent: (event) => events.push(event),
    request: async (url) => {
      if (url.endsWith("/meetings"))
        return { data: [meeting(date, { published: false })] };
      throw status(404);
    },
  });
  assert.equal(events[0].nonSittingDays, 1);
  assert.deepEqual(events[0].expectedDates, [date]);
  assert.equal(result.items[0].outcome, "pending_publication");
  assert.equal(result.items[0].retryable, true);
});

test("older DOCX heading styles and TC agenda numbers preserve topic boundaries", async () => {
  const archive = zipSync({
    "word/styles.xml": strToU8(
      '<w:styles><w:style w:styleId="h"><w:name w:val="heading 1"/></w:style><w:style w:styleId="s"><w:name w:val="Normal_1"/></w:style><w:style w:styleId="contents"><w:name w:val="Contents"/></w:style><w:style w:styleId="toc"><w:name w:val="toc 1"/></w:style></w:styles>',
    ),
    "word/document.xml": strToU8(
      '<w:document><w:body><w:p><w:pPr><w:pStyle w:val="contents"/></w:pPr><w:r><w:t>ENCONTENTS</w:t></w:r></w:p><w:p><w:pPr><w:pStyle w:val="toc"/></w:pPr><w:r><w:t>1 - Research (debate)</w:t></w:r><w:r><w:tab/></w:r><w:r><w:t>27</w:t></w:r></w:p><w:p><w:pPr><w:pStyle w:val="h"/></w:pPr><w:r><w:instrText>TC "1 - Forschung"</w:instrText></w:r><w:r><w:t>Forschung</w:t></w:r></w:p><w:p><w:pPr><w:pStyle w:val="s"/></w:pPr><w:r><w:t>Ada Example (PPE). – Forschung ist wichtig.</w:t></w:r></w:p></w:body></w:document>',
    ),
  });
  const result = await fetchTranscript(
    { date, documentId: `CRE-9-${date}`, published: true },
    async (url) =>
      url.endsWith(".docx")
        ? archive
        : {
            data: [
              {
                is_realized_by: [
                  {
                    is_embodied_by: [
                      {
                        is_exemplified_by: `distribution/doc/CRE-9-${date}_mul.docx`,
                      },
                    ],
                  },
                ],
              },
            ],
          },
    { preference: "docx", only: true },
  );
  assert.equal(result.speeches[0].topic, "Research");
  assert.equal(result.speeches[0].speech_content, "Forschung ist wichtig.");
  assert.match(result.content, /1\. Research/);
});
