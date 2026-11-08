const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");
const os = require("node:os");
const { Readable } = require("node:stream");
const crypto = require("node:crypto");
const {
  createReleaseStorage,
  validateDirectory,
} = require("../src/data/release-storage");
const { createFixture } = require("./fixtures/create");
const { prepare } = require("../src/data/dataset");

class MemoryS3 {
  objects = new Map();
  uploads = new Map();
  calls = [];
  async send(command) {
    const type = command.constructor.name;
    const input = command.input;
    this.calls.push({ type, ...input, Body: input.Body?.length });
    const previous = this.objects.get(input.Key);
    const fail = (status) => {
      const error = new Error(`S3 ${status}`);
      error.$metadata = { httpStatusCode: status };
      throw error;
    };
    const put = (body) => {
      if (
        (input.IfNoneMatch === "*" && previous) ||
        (input.IfMatch && previous?.etag !== input.IfMatch)
      )
        fail(412);
      const etag = `"${crypto.createHash("md5").update(body).digest("hex")}"`;
      this.objects.set(input.Key, { body: Buffer.from(body), etag });
      return { ETag: etag };
    };
    if (type === "GetObjectCommand") {
      if (!previous) fail(404);
      return { Body: Readable.from([previous.body]), ETag: previous.etag };
    }
    if (type === "PutObjectCommand") return put(input.Body);
    if (type === "CreateMultipartUploadCommand") {
      const UploadId = crypto.randomUUID();
      this.uploads.set(UploadId, []);
      return { UploadId };
    }
    if (type === "UploadPartCommand") {
      if (this.failPart) fail(500);
      this.uploads.get(input.UploadId)[input.PartNumber - 1] = Buffer.from(
        input.Body,
      );
      return { ETag: `part-${input.PartNumber}` };
    }
    if (type === "CompleteMultipartUploadCommand") {
      const result = put(Buffer.concat(this.uploads.get(input.UploadId)));
      this.uploads.delete(input.UploadId);
      return result;
    }
    if (type === "AbortMultipartUploadCommand") {
      this.uploads.delete(input.UploadId);
      return {};
    }
    if (type === "ListObjectsV2Command")
      return {
        Contents: [...this.objects.keys()]
          .filter((key) => key.startsWith(input.Prefix))
          .sort()
          .slice(0, input.MaxKeys)
          .map((Key) => ({ Key, LastModified: new Date() })),
      };
    throw new Error(type);
  }
}
async function setup(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "eurowatch-storage-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const client = new MemoryS3();
  const storage = createReleaseStorage({
    bucket: "private-test",
    client,
    partSize: 5 * 1024 * 1024,
  });
  const source = await createFixture(path.join(root, "source.db"));
  const directory = path.join(root, "release");
  await prepare({ source, output: directory, log: () => {} });
  return { root, directory, source, storage, client };
}
test("publish atomically selects a verified release and private checkpoint; stale publishers cannot replace it", async (t) => {
  const { root, directory, source, storage, client } = await setup(t);
  assert.equal(await storage.readCurrent(), null);
  const reference = await storage.uploadRelease(directory);
  const checkpoint = await storage.uploadCheckpoint(source, "run-1");
  const first = await storage.publishRelease(
    { ...reference, checkpoint },
    { expectedETag: null },
  );
  assert.deepEqual(await storage.readCurrent(), first);
  await assert.rejects(storage.publishRelease(reference), /expectedETag/);
  await assert.rejects(
    storage.publishRelease(reference, { expectedETag: null }),
    { code: "RELEASE_CONFLICT" },
  );
  await assert.rejects(
    storage.publishRelease(reference, { expectedETag: '"stale"' }),
    { code: "RELEASE_CONFLICT" },
  );
  assert.deepEqual(await storage.readCurrent(), first);
  const second = await storage.publishRelease(reference, {
    expectedETag: first.etag,
  });
  assert.equal(second.value.checkpoint, undefined);
  await storage.downloadRelease(reference, path.join(root, "download"));
  await validateDirectory(path.join(root, "download"), reference);
  await storage.downloadCheckpoint(checkpoint, path.join(root, "restored.db"));
  assert.deepEqual(
    await fs.readFile(path.join(root, "restored.db")),
    await fs.readFile(source),
  );
  // Repeating a release upload never overwrites existing immutable objects.
  assert.deepEqual(await storage.uploadRelease(directory), reference);
  const writes = client.calls.filter(
    (call) =>
      call.type === "PutObjectCommand" && call.Key.includes("/releases/"),
  );
  assert.ok(writes.every((call) => call.IfNoneMatch === "*"));
});
test("rejects missing files, wrong sizes, malformed manifest and mismatched remote bytes before publishing", async (t) => {
  const { root, directory, storage, client } = await setup(t);
  const manifestPath = path.join(directory, "manifest.json");
  const original = await fs.readFile(manifestPath);
  const manifest = JSON.parse(original);
  delete manifest.files["analytics.db"];
  await fs.writeFile(manifestPath, JSON.stringify(manifest));
  await assert.rejects(storage.uploadRelease(directory), /Incomplete/);
  await fs.writeFile(manifestPath, original);
  const reference = await storage.uploadRelease(directory);
  client.objects.get(`eurowatch/releases/${reference.id}/analytics.db`).body =
    Buffer.from("corrupt");
  await assert.rejects(
    storage.publishRelease(reference, { expectedETag: null }),
    /hash mismatch/,
  );
  assert.equal(await storage.readCurrent(), null);
  await assert.rejects(
    storage.downloadRelease(reference, path.join(root, "broken")),
    /hash mismatch/,
  );
  await assert.rejects(fs.stat(path.join(root, "broken")), /ENOENT/);
  manifest.files = JSON.parse(original).files;
  manifest.files["ep_data.db"].bytes++;
  await fs.writeFile(manifestPath, JSON.stringify(manifest));
  await assert.rejects(
    storage.uploadRelease(directory),
    /bytes or hash mismatch/,
  );
  await assert.rejects(
    storage.downloadRelease(
      { id: "../escape", manifestSha256: "a".repeat(64) },
      path.join(root, "escape"),
    ),
    /identifier/,
  );
});
test("multipart uploads bound each part, condition completion and abort failed uploads", async (t) => {
  const { root, storage, client } = await setup(t);
  const source = path.join(root, "large.db");
  await fs.writeFile(source, Buffer.alloc(11 * 1024 * 1024, 7));
  const ref = await storage.uploadCheckpoint(source, "multipart");
  assert.equal(ref.bytes, 11 * 1024 * 1024);
  const parts = client.calls.filter(
    (call) => call.type === "UploadPartCommand",
  );
  assert.deepEqual(
    parts.map((part) => part.Body),
    [5 * 1024 * 1024, 5 * 1024 * 1024, 1024 * 1024],
  );
  assert.equal(
    client.calls.find((call) => call.type === "CompleteMultipartUploadCommand")
      .IfNoneMatch,
    "*",
  );
  client.failPart = true;
  await assert.rejects(storage.uploadCheckpoint(source, "failed"), /S3 500/);
  assert.equal(client.uploads.size, 0);
  assert.equal(client.calls.at(-1).type, "AbortMultipartUploadCommand");
});
test("private reports require compare-and-swap and paginate separately from releases", async (t) => {
  const { storage } = await setup(t);
  assert.equal(await storage.readRunReport("run-1"), null);
  const first = await storage.writeRunReport(
    "run-1",
    { status: "running" },
    { expectedETag: null },
  );
  await assert.rejects(
    storage.writeRunReport("run-1", { status: "done" }, { expectedETag: null }),
    { code: "RELEASE_CONFLICT" },
  );
  const done = await storage.writeRunReport(
    "run-1",
    { status: "done" },
    { expectedETag: first.etag },
  );
  assert.deepEqual(await storage.readRunReport("run-1"), done);
  assert.equal((await storage.listRunReports()).items[0].runId, "run-1");
  await assert.rejects(
    storage.writeRunReport("../../public", {}, { expectedETag: null }),
    /identifier/,
  );
  await assert.rejects(
    storage.writeRunReport(
      "huge",
      { body: "x".repeat(1024 * 1024) },
      { expectedETag: null },
    ),
    /size limit/,
  );
});
test("shared update leases require CAS and report listing returns latest first with an explicit scan bound", async (t) => {
  const { storage, client } = await setup(t);
  assert.equal(await storage.readLease(), null);
  const first = await storage.writeLease(
    { runId: "first", expiresAt: "2026-11-08T01:00:00Z" },
    { expectedETag: null },
  );
  await assert.rejects(
    storage.writeLease(
      { runId: "second", expiresAt: "2026-11-08T02:00:00Z" },
      { expectedETag: null },
    ),
    { code: "RELEASE_CONFLICT" },
  );
  const second = await storage.writeLease(
    { runId: "second", expiresAt: "2026-11-08T02:00:00Z" },
    { expectedETag: first.etag },
  );
  assert.deepEqual(await storage.readLease(), second);
  await assert.rejects(
    storage.writeLease(
      { runId: "bad", expiresAt: "never" },
      { expectedETag: second.etag },
    ),
    /expiry/,
  );
  const original = client.send.bind(client);
  client.send = async (command) =>
    command.constructor.name === "ListObjectsV2Command"
      ? {
          Contents: [
            {
              Key: "eurowatch/private/runs/aaa.json",
              LastModified: new Date("2026-11-08"),
            },
            {
              Key: "eurowatch/private/runs/zzz.json",
              LastModified: new Date("2026-11-07"),
            },
          ],
        }
      : original(command);
  const page = await storage.listRunReports({ limit: 1 });
  assert.equal(page.items[0].runId, "aaa");
  assert.equal(
    (
      await storage.listRunReports({
        limit: 1,
        continuationToken: page.nextToken,
      })
    ).items[0].runId,
    "zzz",
  );
  client.send = async () => ({
    Contents: Array.from({ length: 1000 }, (_, i) => ({ Key: `${i}.json` })),
    NextContinuationToken: "more",
  });
  await assert.rejects(storage.listRunReports(), { code: "REPORT_SCAN_LIMIT" });
});
