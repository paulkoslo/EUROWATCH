const status = document.querySelector("#status");
const start = document.querySelector("#start");
const runs = document.querySelector("#runs");
let queued;
let submitting = false;

function element(tag, value) {
  const node = document.createElement(tag);
  node.textContent = value;
  return node;
}

async function launch(retryRunId) {
  submitting = true;
  start.disabled = true;
  try {
    const result = await fetch("/operator/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ retryRunId }),
    });
    const data = await result.json();
    if (!result.ok) throw new Error(data.error);
    queued = { id: data.runId, deadline: Date.now() + 120000 };
    status.textContent = "Queued on Render. Waiting for the job to start.";
  } catch (error) {
    status.textContent = error.message;
  } finally {
    submitting = false;
    await refresh();
  }
}

function renderRun(run, busy) {
  const block = document.createElement("details");
  block.open = run.status === "running" || run.status === "failed";
  block.append(
    element("summary", `${run.startedAt} · ${run.trigger} · ${run.status}`),
  );
  if (run.releaseId) block.append(element("p", `Release ${run.releaseId}`));
  if (
    (run.status === "failed" && run.start && run.end) ||
    run.failedDates.length ||
    run.pendingDates.length
  ) {
    const retry = element(
      "button",
      run.status === "failed" ? "Retry this run" : "Retry these sittings",
    );
    retry.disabled = busy;
    retry.addEventListener("click", () => launch(run.runId));
    block.append(retry);
  }
  const list = document.createElement("ol");
  for (const event of run.events) {
    const detail = event.item?.reason ? ` (${event.item.reason})` : "";
    list.append(element("li", `${event.time} · ${event.message}${detail}`));
  }
  block.append(list);
  return block;
}

async function refresh() {
  try {
    const result = await fetch("/operator/runs");
    if (!result.ok) throw new Error("Could not load update history.");
    const data = await result.json();
    if (queued && data.runs.some((run) => run.runId === queued.id)) {
      status.textContent = "The queued run appears in the history below.";
      queued = null;
    }
    if (queued && Date.now() >= queued.deadline) {
      status.textContent =
        "Render has not reported progress. Check the one-off job in Render.";
      queued = null;
    }
    const busy = Boolean(data.running || queued || submitting);
    start.disabled = busy;
    runs.replaceChildren(...data.runs.map((run) => renderRun(run, busy)));
    if (!data.runs.length) runs.textContent = "No runs recorded.";
  } catch (error) {
    status.textContent = error.message;
    start.disabled = Boolean(submitting || queued);
  }
}

start.addEventListener("click", () => launch());
refresh();
setInterval(refresh, 5000);
