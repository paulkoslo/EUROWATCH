const fs = require("node:fs/promises");
const { createWriteStream } = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { Transform } = require("node:stream");
const { pipeline } = require("node:stream/promises");
const s3 = require("@aws-sdk/client-s3");
const { loadDataset, hashFile } = require("./dataset");

const FILES = ["ep_data.db", "analytics.db"];
const JSON_LIMIT = 1024 * 1024;
const PART_SIZE = 16 * 1024 * 1024;
const sha256 = (bytes) =>
  crypto.createHash("sha256").update(bytes).digest("hex");
function identifier(value) {
  if (
    typeof value !== "string" ||
    !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(value)
  )
    throw new Error("Invalid storage identifier.");
  return value;
}
function fileRecord(value) {
  if (
    !value ||
    !Number.isSafeInteger(value.bytes) ||
    value.bytes <= 0 ||
    !/^[a-f0-9]{64}$/.test(value.sha256)
  )
    throw new Error("Invalid file size or SHA-256.");
  return value;
}
function validateManifest(manifest) {
  identifier(manifest?.id);
  if (
    manifest.schemaVersion !== 1 ||
    !manifest.files ||
    Object.keys(manifest.files).sort().join() !== [...FILES].sort().join()
  )
    throw new Error("Incomplete or unsupported release manifest.");
  for (const name of FILES) fileRecord(manifest.files[name]);
  return manifest;
}
function validateReference(ref) {
  identifier(ref?.id);
  if (!/^[a-f0-9]{64}$/.test(ref.manifestSha256))
    throw new Error("Invalid manifest digest.");
  return ref;
}
async function validateDirectory(directory, reference) {
  const bytes = await fs.readFile(path.join(directory, "manifest.json"));
  if (bytes.length > JSON_LIMIT)
    throw new Error("Manifest exceeds size limit.");
  const manifest = validateManifest(JSON.parse(bytes));
  if (
    reference &&
    (manifest.id !== reference.id || sha256(bytes) !== reference.manifestSha256)
  )
    throw new Error("Release manifest does not match its published reference.");
  for (const name of FILES) {
    const filename = path.join(directory, name);
    const stat = await fs.lstat(filename);
    if (
      !stat.isFile() ||
      stat.size !== manifest.files[name].bytes ||
      (await hashFile(filename)) !== manifest.files[name].sha256
    )
      throw new Error(`Release bytes or hash mismatch: ${name}`);
  }
  const dataset = await loadDataset(directory);
  try {
    if (!dataset.analytics) throw new Error("Matching analytics is required.");
    const release = { ...manifest };
    delete release.files;
    // Both databases must carry the complete same release metadata as the manifest.
    const { metadata } = require("./dataset");
    const { isDeepStrictEqual } = require("node:util");
    if (
      !isDeepStrictEqual(release, dataset.release) ||
      !isDeepStrictEqual(release, await metadata(dataset.analytics, "release"))
    )
      throw new Error("Release metadata does not match the manifest.");
  } finally {
    await dataset.close();
  }
  return { manifest, bytes };
}
function conflict(error) {
  return (
    [409, 412].includes(error?.$metadata?.httpStatusCode) ||
    ["PreconditionFailed", "ConditionalRequestConflict"].includes(error?.name)
  );
}
function missing(error) {
  return (
    error?.$metadata?.httpStatusCode === 404 || error?.name === "NoSuchKey"
  );
}
function conditional(expectedETag) {
  if (expectedETag === null) return { IfNoneMatch: "*" };
  if (typeof expectedETag !== "string" || !expectedETag)
    throw new Error(
      "An explicit expectedETag (or null for creation) is required.",
    );
  return { IfMatch: expectedETag };
}
async function readBody(body, limit = JSON_LIMIT) {
  const chunks = [];
  let size = 0;
  try {
    for await (const chunk of body) {
      size += chunk.length;
      if (size > limit) throw new Error("Remote JSON exceeds size limit.");
      chunks.push(Buffer.from(chunk));
    }
  } finally {
    body.destroy?.();
  }
  return Buffer.concat(chunks);
}
function createReleaseStorage({
  bucket,
  prefix = "eurowatch",
  client,
  partSize = PART_SIZE,
}) {
  if (!bucket || !client)
    throw new Error("Storage bucket and client are required.");
  if (!/^[a-zA-Z0-9][a-zA-Z0-9/_-]*$/.test(prefix) || prefix.includes("//"))
    throw new Error("Invalid storage prefix.");
  prefix = prefix.replace(/\/$/, "");
  if (
    !Number.isSafeInteger(partSize) ||
    partSize < 5 * 1024 * 1024 ||
    partSize > 128 * 1024 * 1024
  )
    throw new Error("Multipart size must be between 5 and 128 MiB.");
  const key = (suffix) => `${prefix}/${suffix}`;
  const send = (Command, input) =>
    client.send(new Command({ Bucket: bucket, ...input }));
  function checkpointReference(ref) {
    fileRecord(ref);
    if (
      typeof ref.key !== "string" ||
      !ref.key.startsWith(key("private/checkpoints/")) ||
      !/^[a-zA-Z0-9/._-]+$/.test(ref.key) ||
      ref.key.includes("..")
    )
      throw new Error("Invalid private checkpoint reference.");
    return ref;
  }
  async function getJson(objectKey) {
    try {
      const result = await send(s3.GetObjectCommand, { Key: objectKey });
      if (!result.ETag) throw new Error("Storage did not supply an ETag.");
      return {
        value: JSON.parse(await readBody(result.Body)),
        etag: result.ETag,
      };
    } catch (error) {
      if (missing(error)) return null;
      throw error;
    }
  }
  async function putJson(objectKey, value, expectedETag) {
    const body = Buffer.from(JSON.stringify(value) + "\n");
    if (body.length > JSON_LIMIT) throw new Error("JSON exceeds size limit.");
    try {
      const result = await send(s3.PutObjectCommand, {
        Key: objectKey,
        Body: body,
        ContentType: "application/json",
        ...conditional(expectedETag),
      });
      if (!result.ETag)
        throw new Error("Storage did not supply an ETag after writing.");
      return { value, etag: result.ETag };
    } catch (error) {
      if (conflict(error)) error.code = "RELEASE_CONFLICT";
      throw error;
    }
  }
  async function verifyRemote(objectKey, expected) {
    const result = await send(s3.GetObjectCommand, { Key: objectKey });
    const hash = crypto.createHash("sha256");
    let bytes = 0;
    try {
      for await (const chunk of result.Body) {
        bytes += chunk.length;
        if (bytes > expected.bytes)
          throw new Error("Remote object exceeds expected size.");
        hash.update(chunk);
      }
    } finally {
      result.Body.destroy?.();
    }
    if (bytes !== expected.bytes || hash.digest("hex") !== expected.sha256)
      throw new Error(`Remote object bytes or hash mismatch: ${objectKey}`);
  }
  async function uploadFile(objectKey, filename, expected) {
    fileRecord(expected);
    const stat = await fs.lstat(filename);
    if (!stat.isFile() || stat.size !== expected.bytes)
      throw new Error("Upload source size changed.");
    if (Math.ceil(expected.bytes / partSize) > 10000)
      throw new Error("File exceeds configured multipart capacity.");
    let uploadId;
    try {
      if (expected.bytes <= partSize) {
        const body = await fs.readFile(filename);
        if (body.length !== expected.bytes || sha256(body) !== expected.sha256)
          throw new Error("Upload source changed.");
        await send(s3.PutObjectCommand, {
          Key: objectKey,
          Body: body,
          IfNoneMatch: "*",
        });
      } else {
        const opened = await send(s3.CreateMultipartUploadCommand, {
          Key: objectKey,
        });
        uploadId = opened.UploadId;
        if (!uploadId)
          throw new Error("Storage did not supply a multipart upload ID.");
        const file = await fs.open(filename, "r");
        const hash = crypto.createHash("sha256");
        const parts = [];
        let position = 0;
        try {
          while (position < expected.bytes) {
            const body = Buffer.allocUnsafe(
              Math.min(partSize, expected.bytes - position),
            );
            let filled = 0;
            while (filled < body.length) {
              const { bytesRead } = await file.read(
                body,
                filled,
                body.length - filled,
                position + filled,
              );
              if (!bytesRead) throw new Error("Upload source truncated.");
              filled += bytesRead;
            }
            hash.update(body);
            const PartNumber = parts.length + 1;
            const result = await send(s3.UploadPartCommand, {
              Key: objectKey,
              UploadId: uploadId,
              PartNumber,
              Body: body,
              ContentLength: body.length,
            });
            if (!result.ETag)
              throw new Error("Storage did not supply a multipart ETag.");
            parts.push({ PartNumber, ETag: result.ETag });
            position += body.length;
          }
          if (
            (await file.stat()).size !== expected.bytes ||
            hash.digest("hex") !== expected.sha256
          )
            throw new Error("Upload source changed.");
        } finally {
          await file.close();
        }
        await send(s3.CompleteMultipartUploadCommand, {
          Key: objectKey,
          UploadId: uploadId,
          MultipartUpload: { Parts: parts },
          IfNoneMatch: "*",
        });
        uploadId = null;
      }
    } catch (error) {
      if (uploadId)
        await send(s3.AbortMultipartUploadCommand, {
          Key: objectKey,
          UploadId: uploadId,
        }).catch(() => {});
      // Retrying an immutable upload is safe only if the existing bytes match.
      if (conflict(error)) {
        await verifyRemote(objectKey, expected);
        return;
      }
      throw error;
    }
  }
  async function downloadFile(objectKey, destination, expected) {
    fileRecord(expected);
    const result = await send(s3.GetObjectCommand, { Key: objectKey });
    const hash = crypto.createHash("sha256");
    let bytes = 0;
    const checker = new Transform({
      transform(chunk, encoding, callback) {
        bytes += chunk.length;
        hash.update(chunk);
        callback(
          bytes > expected.bytes
            ? new Error("Download exceeds manifest size.")
            : null,
          chunk,
        );
      },
    });
    try {
      await pipeline(
        result.Body,
        checker,
        createWriteStream(destination, { flags: "wx", mode: 0o600 }),
      );
      if (bytes !== expected.bytes || hash.digest("hex") !== expected.sha256)
        throw new Error("Downloaded bytes or hash mismatch.");
    } catch (error) {
      await fs.rm(destination, { force: true });
      throw error;
    }
  }
  async function remoteManifest(reference) {
    validateReference(reference);
    const result = await send(s3.GetObjectCommand, {
      Key: key(`releases/${reference.id}/manifest.json`),
    });
    const bytes = await readBody(result.Body);
    if (sha256(bytes) !== reference.manifestSha256)
      throw new Error("Remote manifest hash mismatch.");
    const manifest = validateManifest(JSON.parse(bytes));
    if (manifest.id !== reference.id)
      throw new Error("Remote release ID mismatch.");
    return { bytes, manifest };
  }
  return {
    async readCurrent() {
      const current = await getJson(key("current.json"));
      if (current) {
        validateReference(current.value);
        if (current.value.checkpoint)
          checkpointReference(current.value.checkpoint);
      }
      return current;
    },
    async uploadRelease(directory) {
      const { manifest, bytes } = await validateDirectory(directory);
      for (const name of FILES)
        await uploadFile(
          key(`releases/${manifest.id}/${name}`),
          path.join(directory, name),
          manifest.files[name],
        );
      // The manifest is the completion marker and is uploaded only after both DBs.
      await uploadFile(
        key(`releases/${manifest.id}/manifest.json`),
        path.join(directory, "manifest.json"),
        { bytes: bytes.length, sha256: sha256(bytes) },
      );
      return { id: manifest.id, manifestSha256: sha256(bytes) };
    },
    async publishRelease(reference, { expectedETag } = {}) {
      conditional(expectedETag);
      const { manifest } = await remoteManifest(reference);
      for (const name of FILES)
        await verifyRemote(
          key(`releases/${reference.id}/${name}`),
          manifest.files[name],
        );
      const value = {
        id: reference.id,
        manifestSha256: reference.manifestSha256,
      };
      if (reference.checkpoint) {
        const checkpoint = checkpointReference(reference.checkpoint);
        await verifyRemote(checkpoint.key, checkpoint);
        value.checkpoint = {
          key: checkpoint.key,
          sha256: checkpoint.sha256,
          bytes: checkpoint.bytes,
        };
      }
      return putJson(key("current.json"), value, expectedETag);
    },
    async inspectRelease(reference) {
      const { manifest, bytes } = await remoteManifest(reference);
      return {
        manifest,
        totalBytes:
          bytes.length +
          FILES.reduce((total, name) => total + manifest.files[name].bytes, 0),
      };
    },
    async downloadRelease(reference, destination) {
      const { manifest, bytes } = await remoteManifest(reference);
      await fs.mkdir(destination, { recursive: false, mode: 0o700 });
      try {
        for (const name of FILES)
          await downloadFile(
            key(`releases/${reference.id}/${name}`),
            path.join(destination, name),
            manifest.files[name],
          );
        await fs.writeFile(path.join(destination, "manifest.json"), bytes, {
          flag: "wx",
          mode: 0o600,
        });
        await validateDirectory(destination, reference);
      } catch (error) {
        await fs.rm(destination, { recursive: true, force: true });
        throw error;
      }
      return destination;
    },
    async uploadCheckpoint(sourceFile, runId) {
      identifier(runId);
      const ref = {
        key: key(`private/checkpoints/${runId}/${crypto.randomUUID()}.db`),
        bytes: (await fs.stat(sourceFile)).size,
        sha256: await hashFile(sourceFile),
      };
      await uploadFile(ref.key, sourceFile, ref);
      return ref;
    },
    async downloadCheckpoint(reference, destination) {
      checkpointReference(reference);
      const temp = `${destination}.${crypto.randomUUID()}.tmp`;
      await fs.mkdir(path.dirname(destination), {
        recursive: true,
        mode: 0o700,
      });
      await downloadFile(reference.key, temp, reference);
      try {
        await fs.rename(temp, destination);
      } finally {
        await fs.rm(temp, { force: true });
      }
      return destination;
    },
    async writeRunReport(runId, report, { expectedETag } = {}) {
      identifier(runId);
      return putJson(key(`private/runs/${runId}.json`), report, expectedETag);
    },
    readRunReport(runId) {
      return getJson(key(`private/runs/${identifier(runId)}.json`));
    },
    readLease() {
      return getJson(key("private/update-lease.json"));
    },
    async writeLease(value, { expectedETag } = {}) {
      identifier(value?.runId);
      if (
        typeof value.expiresAt !== "string" ||
        !Number.isFinite(Date.parse(value.expiresAt))
      )
        throw new Error("Lease requires an expiry timestamp.");
      return putJson(
        key("private/update-lease.json"),
        { runId: value.runId, expiresAt: value.expiresAt },
        expectedETag,
      );
    },
    async listRunReports({ limit = 50, continuationToken } = {}) {
      if (!Number.isInteger(limit) || limit < 1 || limit > 100)
        throw new Error("Report list limit must be 1–100.");
      const offset =
        continuationToken === undefined ? 0 : Number(continuationToken);
      if (!Number.isSafeInteger(offset) || offset < 0 || offset > 1000)
        throw new Error("Invalid report cursor.");
      const objects = [];
      let token;
      do {
        const result = await send(s3.ListObjectsV2Command, {
          Prefix: key("private/runs/"),
          MaxKeys: 1000 - objects.length,
          ContinuationToken: token,
        });
        objects.push(...(result.Contents || []));
        token = result.NextContinuationToken;
        if (token && objects.length >= 1000) {
          const error = new Error(
            "Report history exceeds 1,000 entries; configure retention before listing.",
          );
          error.code = "REPORT_SCAN_LIMIT";
          throw error;
        }
      } while (token);
      const reports = objects
        .filter((item) => item.Key.endsWith(".json"))
        .sort(
          (a, b) =>
            new Date(b.LastModified) - new Date(a.LastModified) ||
            b.Key.localeCompare(a.Key),
        );
      return {
        items: reports
          .slice(offset, offset + limit)
          .map((item) => ({
            runId: path.posix.basename(item.Key, ".json"),
            lastModified: item.LastModified,
          })),
        nextToken:
          offset + limit < reports.length ? String(offset + limit) : null,
      };
    },
    close() {
      client.destroy?.();
    },
  };
}
function createReleaseStorageFromEnv(env = process.env) {
  if (!env.RELEASE_BUCKET) return null;
  if (
    env.RELEASE_ENDPOINT &&
    new URL(env.RELEASE_ENDPOINT).protocol !== "https:"
  )
    throw new Error("Release storage endpoint must use HTTPS.");
  const client = new s3.S3Client({
    region: env.RELEASE_REGION || "us-east-1",
    endpoint: env.RELEASE_ENDPOINT || undefined,
    forcePathStyle: env.RELEASE_FORCE_PATH_STYLE === "1",
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
  });
  return createReleaseStorage({
    bucket: env.RELEASE_BUCKET,
    prefix: env.RELEASE_PREFIX || "eurowatch",
    client,
  });
}
module.exports = {
  createReleaseStorage,
  createReleaseStorageFromEnv,
  validateDirectory,
  validateReference,
  validateManifest,
};
