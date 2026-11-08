const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const express = require("express");
const { hash, requestFor, resultFor } = require("../src/insights/model");
const {
  buildPublication,
  writePublication,
  readPublication,
  html,
  rss,
} = require("../src/insights/publication");
const { createInsightsRouter } = require("../src/insights/router");
const {
  createWatchlistStore,
  createWatchlistRouter,
} = require("../src/insights/watchlists");

function fixture() {
  const config = {
    model: "fake",
    promptVersion: "v1",
    taxonomyVersion: "t1",
    topics: ["Climate"],
  };
  const record = {
    id: "debate:1",
    kind: "debate",
    agendaTitle: "<script>alert(1)</script> & Climate",
    inputRevision: "r1",
    effectiveAt: "2026-03-09T00:00:00Z",
    officialIds: [],
    passages: [
      {
        id: "p1",
        text: "Cleaner energy.",
        speakerId: "mep:1",
        sourceUrl: "https://www.europarl.europa.eu/test",
        sourceRevision: "s1",
      },
    ],
  };
  const request = requestFor(record, config);
  const result = resultFor(request, {
    candidate: {
      topics: [{ label: "Climate", confidence: 0.8, passageIds: ["p1"] }],
      claims: [
        {
          text: "Cleaner energy is supported.",
          quote: "Cleaner energy.",
          passageId: "p1",
          speakerId: "mep:1",
        },
      ],
      uncertain: false,
    },
  });
  const approval = {
    key: result.key,
    candidateHash: hash(result.candidate),
    approved: true,
    reviewer: "private-reviewer",
    reviewedAt: "2026-03-15T12:00:00Z",
  };
  return {
    corpus: { config, records: [record] },
    report: { results: [result] },
    approvals: [approval],
    releaseId: "dataset-1",
    weekStart: "2026-03-09",
    publicBaseUrl: "https://example.test",
    publishedAt: "2026-03-16T12:00:00Z",
  };
}
async function serve(t, app) {
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  t.after(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  return `http://127.0.0.1:${server.address().port}`;
}
test("publication binds approvals to exact candidate and input; partial sources require separate approval", () => {
  const options = fixture();
  const publication = buildPublication(options);
  assert.equal(publication.public.entries.length, 1);
  assert.ok(!JSON.stringify(publication.public).includes("private-reviewer"));
  assert.ok(!html(publication.public).includes("<script>"));
  assert.match(rss(publication.public), /&lt;script&gt;/);
  assert.throws(
    () =>
      buildPublication({
        ...options,
        approvals: [{ ...options.approvals[0], candidateHash: "wrong" }],
      }),
    /bind/,
  );
  assert.throws(
    () =>
      buildPublication({
        ...options,
        corpus: {
          ...options.corpus,
          records: [{ ...options.corpus.records[0], inputRevision: "new" }],
        },
      }),
    /No approved/,
  );
  const partial = fixture();
  partial.corpus.records[0].scope = { complete: false, part: 1, parts: 2 };
  const request = requestFor(partial.corpus.records[0], partial.corpus.config);
  partial.report.results[0] = {
    ...partial.report.results[0],
    key: request.key,
  };
  partial.approvals[0].key = request.key;
  assert.throws(() => buildPublication(partial), /Partial/);
  partial.approvals[0].allowPartial = true;
  assert.equal(buildPublication(partial).public.coverage.partialEntries, 1);
});
test("public and private artifacts are separated and tampered publications fail integrity checks", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "publication-test-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const publication = buildPublication(fixture());
  const directory = path.join(dir, "public"),
    privateDirectory = path.join(dir, "private");
  const written = writePublication({
    publication,
    directory,
    privateDirectory,
  });
  assert.deepEqual(
    readPublication(directory, written.publicationId),
    publication.public,
  );
  assert.ok(
    !fs
      .readFileSync(path.join(written.directory, "public.json"), "utf8")
      .includes("private-reviewer"),
  );
  assert.throws(
    () =>
      writePublication({
        publication,
        directory,
        privateDirectory: path.join(directory, "reviews"),
      }),
    /separate/,
  );
  const data = { ...publication.public, releaseId: "wrong" };
  fs.writeFileSync(
    path.join(written.directory, "public.json"),
    JSON.stringify(data),
  );
  assert.throws(
    () => readPublication(directory, written.publicationId),
    /integrity/,
  );
});
test("shareable pages, API, RSS and read-only MCP use the reviewed release and reject mutation", async (t) => {
  const publication = buildPublication(fixture()).public;
  let releaseId = publication.releaseId;
  const app = express();
  app.use(
    createInsightsRouter({
      getPublication: async (id) =>
        !id || id === publication.publicationId ? publication : null,
      getDatasetId: () => releaseId,
      publicOrigin: "https://example.test",
    }),
  );
  const base = await serve(t, app);
  const response = await fetch(`${base}/api/insights?q=cleaner`);
  assert.equal(response.headers.get("x-dataset-id"), "dataset-1");
  assert.equal((await response.json()).total, 1);
  assert.equal(
    (
      await fetch(
        `${base}/briefings/${publication.publicationId}/${publication.entries[0].slug}`,
      )
    ).status,
    200,
  );
  assert.equal((await fetch(`${base}/feed.xml`)).status, 200);
  const headers = {
    "Content-Type": "application/json",
    Accept: "application/json, text/event-stream",
  };
  const call = async (body) =>
    fetch(`${base}/mcp`, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
  assert.equal(
    (
      await (
        await call({
          jsonrpc: "2.0",
          id: 1,
          method: "initialize",
          params: { protocolVersion: "2025-11-25" },
        })
      ).json()
    ).result.protocolVersion,
    "2025-11-25",
  );
  assert.equal(
    (await (await call({ jsonrpc: "2.0", id: 2, method: "tools/list" })).json())
      .result.tools.length,
    2,
  );
  const search = await (
    await call({
      jsonrpc: "2.0",
      id: 3,
      method: "tools/call",
      params: { name: "search_briefings", arguments: { query: "cleaner" } },
    })
  ).json();
  assert.equal(
    JSON.parse(search.result.content[0].text).releaseId,
    "dataset-1",
  );
  assert.equal(
    (
      await (
        await call({
          jsonrpc: "2.0",
          id: 4,
          method: "tools/call",
          params: { name: "publish" },
        })
      ).json()
    ).error.code,
    -32602,
  );
  assert.equal(
    (
      await fetch(`${base}/mcp`, {
        method: "POST",
        headers: { ...headers, Origin: "https://attacker.test" },
        body: "{}",
      })
    ).status,
    403,
  );
  assert.equal(
    (await call({ jsonrpc: "2.0", method: "notifications/initialized" }))
      .status,
    202,
  );
  releaseId = "dataset-2";
  assert.equal((await fetch(`${base}/api/insights`)).status, 503);
  assert.equal(
    (await fetch(`${base}/briefings/${publication.publicationId}`)).status,
    200,
  );
});
test("watchlist pilot is private, tenant-isolated, durable and prepares once without sending", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "watchlist-test-"));
  const store = await createWatchlistStore(path.join(dir, "watchlists.db"));
  t.after(async () => {
    await store.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });
  const publication = buildPublication(fixture()).public;
  const app = express();
  app.use(
    "/private",
    createWatchlistRouter({
      store,
      getPublication: () => publication,
      publicOrigin: "https://example.test",
      authorize: (req) =>
        req.get("x-test-owner")
          ? {
              id: req.get("x-test-owner"),
              watchlistPilot: req.get("x-test-entitled") === "yes",
            }
          : null,
    }),
  );
  const base = await serve(t, app);
  assert.equal((await fetch(`${base}/private/watchlists`)).status, 401);
  assert.equal(
    (
      await fetch(`${base}/private/watchlists`, {
        headers: { "x-test-owner": "alice" },
      })
    ).status,
    403,
  );
  const headers = {
    "x-test-owner": "alice",
    "x-test-entitled": "yes",
    Origin: "https://example.test",
    "Content-Type": "application/json",
  };
  const created = await (
    await fetch(`${base}/private/watchlists`, {
      method: "POST",
      headers,
      body: JSON.stringify({ name: "Energy", topics: ["Climate"] }),
    })
  ).json();
  const preview = async (owner) =>
    fetch(`${base}/private/watchlists/${created.id}/preview`, {
      method: "POST",
      headers: { ...headers, "x-test-owner": owner },
    });
  assert.equal((await preview("bob")).status, 404);
  assert.equal(
    (await (await preview("alice")).json()).previouslyPrepared,
    false,
  );
  assert.equal(
    (await (await preview("alice")).json()).previouslyPrepared,
    true,
  );
  assert.equal((await store.list("alice")).length, 1);
  assert.equal((await store.list("bob")).length, 0);
  assert.equal(
    (
      await fetch(`${base}/private/watchlists/${created.id}`, {
        method: "DELETE",
        headers: { ...headers, Origin: "https://attacker.test" },
      })
    ).status,
    403,
  );
});
