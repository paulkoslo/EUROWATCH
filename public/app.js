const main = document.querySelector("#content");
const number = (value) => new Intl.NumberFormat("en-GB").format(value || 0);
const dateLabel = (date) =>
  date
    ? new Intl.DateTimeFormat("en-GB", {
        day: "numeric",
        month: "short",
        year: "numeric",
        timeZone: "UTC",
      }).format(new Date(`${date}T00:00:00Z`))
    : "Unknown";
let snapshot;
let activeRequest;

function node(tag, attrs = {}, ...children) {
  const element = document.createElement(tag);
  for (const [name, value] of Object.entries(attrs)) {
    if (value == null || value === false) continue;
    if (name.startsWith("on"))
      element.addEventListener(name.slice(2).toLowerCase(), value);
    else if (name === "className") element.className = value;
    else if (name === "checked" || name === "disabled" || name === "open")
      element[name] = value;
    else element.setAttribute(name, value);
  }
  for (const child of children.flat(Infinity))
    if (child != null && typeof child !== "boolean")
      element.append(
        child instanceof Node ? child : document.createTextNode(String(child)),
      );
  return element;
}
const para = (text, className = "", ...rest) =>
  className instanceof Node
    ? node("p", {}, text, className, ...rest)
    : node("p", { className }, text, ...rest);
const link = (text, href, className = "", internal = false) =>
  node("a", { href, className, ...(internal ? { "data-nav": "" } : {}) }, text);
const message = (text, className = "notice") =>
  node(
    "div",
    { className, role: className.includes("error") ? "alert" : "status" },
    text,
  );
const encoded = encodeURIComponent;

function url(changes, preserve = true) {
  const query = preserve
    ? new URLSearchParams(location.search)
    : new URLSearchParams();
  for (const [key, value] of Object.entries(changes)) {
    if (value == null || value === "") query.delete(key);
    else query.set(key, String(value));
  }
  return query.size ? `/?${query}` : "/";
}
function navigate(href) {
  if (location.pathname + location.search !== href)
    history.pushState(null, "", href);
  render(true);
}
async function api(path, signal) {
  const response = await fetch(path, { signal });
  const data = await response
    .json()
    .catch(() => ({ error: "The server returned an unreadable response." }));
  if (!response.ok)
    throw new Error(data.error || `Request failed (${response.status}).`);
  return data;
}
function heading(title, description, eyebrow = "Explore the corpus") {
  return node(
    "div",
    { className: "page-heading" },
    para(eyebrow, "eyebrow"),
    node("h1", {}, title),
    description && para(description),
  );
}
function field(label, name, value = "", type = "text") {
  const input = node("input", {
    type,
    name,
    value,
    ...(type === "search" ? { autocomplete: "off" } : {}),
  });
  return {
    wrapper: node(
      "label",
      { className: `field ${type === "search" ? "grow" : ""}` },
      label,
      input,
    ),
    input,
  };
}
function selectField(label, name, value, choices) {
  const select = node(
    "select",
    { name, "aria-label": label },
    choices.map(([id, text]) => node("option", { value: id }, text)),
  );
  select.value = value || "";
  return {
    wrapper: node("label", { className: "field" }, label, select),
    input: select,
  };
}
function form(filters, onSubmit, reset) {
  return node(
    "form",
    {
      className: "toolbar",
      onSubmit: (event) => {
        event.preventDefault();
        onSubmit();
      },
    },
    filters.map((item) => item.wrapper),
    node(
      "div",
      { className: "actions" },
      node("button", { type: "submit" }, "Apply filters"),
      link("Reset", reset, "button secondary", true),
    ),
  );
}
function pageNav(meta, key = "offset") {
  const start = meta.total ? meta.offset + 1 : 0;
  const end = Math.min(meta.offset + meta.limit, meta.total);
  const buttons = [];
  if (meta.offset > 0)
    buttons.push(
      link(
        "← Previous",
        url({ [key]: Math.max(0, meta.offset - meta.limit) }),
        "button secondary",
        true,
      ),
    );
  if (end < meta.total)
    buttons.push(
      link(
        "Next →",
        url({ [key]: meta.offset + meta.limit }),
        "button secondary",
        true,
      ),
    );
  return node(
    "div",
    { className: "pagination" },
    para(`${number(start)}–${number(end)} of ${number(meta.total)}`, "muted"),
    node("div", { className: "actions" }, buttons),
  );
}
function table(headers, rows, caption) {
  return node(
    "div",
    { className: "table-wrap" },
    node(
      "table",
      {},
      node("caption", { className: "sr-only" }, caption),
      node(
        "thead",
        {},
        node(
          "tr",
          {},
          headers.map((text) => node("th", { scope: "col" }, text)),
        ),
      ),
      node(
        "tbody",
        {},
        rows.map((row) =>
          node(
            "tr",
            {},
            row.map((value, index) =>
              node("td", { className: index === 0 ? "name" : "" }, value),
            ),
          ),
        ),
      ),
    ),
  );
}
function badge(source) {
  const label =
    source === "api"
      ? "API record"
      : source === "historic"
        ? "Inferred record"
        : "Unresolved identity";
  return node(
    "span",
    {
      className: `badge ${source === "api" ? "" : source === "historic" ? "inferred" : "unresolved"}`,
    },
    label,
  );
}
function home() {
  const stats = snapshot.stats;
  return node(
    "div",
    {},
    heading(
      "European Parliament speeches",
      "Browse the collected plenary records, explore contributions and download the dataset.",
      "EUROWATCH",
    ),
    node(
      "div",
      { className: "stats" },
      [
        [number(stats.speeches), "Recorded speeches"],
        [number(stats.sittings), "Parliament sittings"],
        [dateLabel(stats.toDate), "Latest stored sitting"],
      ].map(([value, label]) =>
        node(
          "div",
          { className: "stat" },
          node("strong", {}, value),
          node("span", {}, label),
        ),
      ),
    ),
    para(
      snapshot.coverage?.lastSuccessfulRefresh
        ? `Last successful refresh: ${dateLabel(snapshot.coverage.lastSuccessfulRefresh.slice(0, 10))}. Known gaps in the checked range: ${(snapshot.coverage.pendingDates || []).length} pending, ${(snapshot.coverage.failedDates || []).length} failed.`
        : "Refresh history and calendar gaps are not recorded for this snapshot.",
    ),
    snapshot.coverage?.pendingDates?.length > 0 &&
      para(
        `Pending transcripts: ${snapshot.coverage.pendingDates.join(", ")}.`,
      ),
    snapshot.coverage?.failedDates?.length > 0 &&
      para(
        `Failed sitting imports: ${snapshot.coverage.failedDates.join(", ")}.`,
      ),
    node(
      "section",
      { className: "panel" },
      node("h2", {}, "Explore the data"),
      para(
        "Use the tabs above or choose a view below. Filters, pagination and source links are available throughout the explorer.",
      ),
      node(
        "div",
        { className: "overview-actions" },
        link("Parliament Sittings", "/?view=sittings", "button", true),
        link("Speakers", "/?view=speakers", "button", true),
        link("Descriptive Analytics", "/?view=topics", "button", true),
        link("Parliament briefings", "/briefings", "button", true),
        link("Official Parliament records", "/parliament", "button", true),
        link("Full Data Download", "/?view=download", "button secondary", true),
      ),
    ),
    message(
      "This is a stored collection with known metadata gaps. Topic and language labels are derived, and some speaker identities are inferred.",
    ),
    para(link("Read the data and coverage notes", "/?view=data", "", true)),
  );
}
function fullDownloadPanel(info) {
  const gib = info.sizeBytes / 1024 ** 3;
  const size =
    gib >= 1
      ? `${gib.toFixed(2)} GiB`
      : `${(info.sizeBytes / 1024 ** 2).toFixed(2)} MiB`;
  return node(
    "section",
    { className: "panel full-download" },
    node("h2", {}, "Download the full database"),
    para(
      `Complete SQLite .db file · ${number(snapshot.stats.speeches)} speeches · ${size}`,
    ),
    info.available
      ? link(
          "Download full database (.db)",
          "/api/download/full-dataset",
          "button",
        )
      : message("The full download has been disabled on this host."),
    para(
      `Contains the full served snapshot, covering ${dateLabel(snapshot.stats.fromDate)} to ${dateLabel(snapshot.stats.toDate)}. No CSV row limit applies.`,
      "small muted",
    ),
  );
}
async function downloadView(query, signal) {
  const info = await api("/api/full-dataset/status", signal);
  return node(
    "div",
    {},
    heading(
      "Full Data Download",
      "Download the complete database for your own analysis.",
      "EUROWATCH",
    ),
    fullDownloadPanel(info),
    node(
      "section",
      { className: "panel prose" },
      node("h2", {}, "Using the database"),
      para(
        "The download is a SQLite database containing the complete served collection, including sittings, individual speeches, speaker metadata and the browsing projections. Open it in a SQLite viewer or use it directly in Python, R or other analysis tools.",
      ),
      para(
        "Raw speech text is preserved. Missing metadata and inferred identities remain part of the collection; the data notes explain these limitations.",
      ),
      para(link("Data notes and filtered CSV export", "/?view=data", "", true)),
      node(
        "details",
        {},
        node("summary", {}, "File verification"),
        para("SHA-256:"),
        node("code", {}, info.sha256),
      ),
    ),
  );
}
async function sittingView(query, signal) {
  const id = query.get("sitting");
  if (id) {
    const [sitting, result] = await Promise.all([
      api(`/api/sittings/${encoded(id)}`, signal),
      api(
        `/api/sittings/${encoded(id)}/speeches?limit=20&offset=${encoded(query.get("speechOffset") || "0")}`,
        signal,
      ),
    ]);
    return node(
      "div",
      {},
      link(
        "← Back to sittings",
        url({ sitting: null, speechOffset: null }),
        "back-link",
        true,
      ),
      heading(
        `Sitting of ${dateLabel(sitting.date)}`,
        `${number(result.meta.total)} stored contributions. Open a contribution to read its text.`,
        "Parliamentary sitting",
      ),
      link(
        "Open original Parliament record ↗",
        sitting.sourceUrl,
        "button secondary",
      ),
      node(
        "div",
        { className: "result-summary" },
        "Contributions in their recorded order",
      ),
      speechList(result),
      pageNav(result.meta, "speechOffset"),
    );
  }
  const start = field("From date", "startDate", query.get("startDate"), "date");
  const end = field("To date", "endDate", query.get("endDate"), "date");
  const filters = new URLSearchParams({
    limit: "25",
    offset: query.get("offset") || "0",
  });
  for (const name of ["startDate", "endDate"])
    if (query.get(name)) filters.set(name, query.get(name));
  const result = await api(`/api/sittings?${filters}`, signal);
  return node(
    "div",
    {},
    heading(
      "Parliamentary sittings",
      "Choose a date range and open a sitting to read its recorded contributions.",
    ),
    form(
      [start, end],
      () =>
        navigate(
          url(
            {
              view: "sittings",
              startDate: start.input.value,
              endDate: end.input.value,
            },
            false,
          ),
        ),
      "/?view=sittings",
    ),
    node(
      "div",
      { className: "result-summary" },
      `${number(result.meta.total)} sittings in this selection`,
      "Newest first",
    ),
    result.data.length
      ? table(
          ["Sitting date", "Recorded contributions", "Explore"],
          result.data.map((sitting) => [
            link(
              dateLabel(sitting.date),
              url({ sitting: sitting.id, speechOffset: null }),
              "",
              true,
            ),
            number(sitting.speechCount),
            link(
              "Read speeches →",
              url({ sitting: sitting.id, speechOffset: null }),
              "",
              true,
            ),
          ]),
          "Parliamentary sittings",
        )
      : message(
          "No sittings match this date range. Try another range or reset the filters.",
          "empty",
        ),
    pageNav(result.meta),
  );
}
function speechList(result) {
  if (!result.data.length)
    return message("No contributions match this selection.", "empty");
  return result.data.map((speech, index) =>
    node(
      "details",
      { className: "speech", id: `speech-${speech.id}`, open: index === 0 },
      node(
        "summary",
        {},
        node("strong", {}, speech.speakerName),
        node(
          "span",
          { className: "topic-preview" },
          `${dateLabel(speech.date)} · ${speech.topic || "Agenda topic unavailable"}`,
        ),
      ),
      node(
        "div",
        { className: "speech-body" },
        node(
          "div",
          { className: "speech-meta" },
          badge(speech.speakerSource),
          node(
            "span",
            {},
            `Detected language: ${speech.language || "Unknown"}`,
          ),
          node("span", {}, `Raw group: ${speech.rawGroup || "Unknown"}`),
          node("span", {}, `Derived group: ${speech.groupLabel || "Unknown"}`),
        ),
        para(
          speech.content || "Speech text is unavailable for this record.",
          "speech-text",
        ),
        node(
          "div",
          { className: "speech-links" },
          Number.isSafeInteger(speech.speakerId) && speech.speakerId > 0
            ? link(
                "View speaker contributions",
                url({ view: "speakers", speaker: speech.speakerId }, false),
                "",
                true,
              )
            : node(
                "span",
                { className: "muted" },
                "Speaker profile unavailable",
              ),
          link(
            "Open sitting",
            url({ view: "sittings", sitting: speech.sittingId }, false),
            "",
            true,
          ),
          link("Original Parliament record ↗", speech.sourceUrl),
        ),
      ),
    ),
  );
}
async function speakerView(query, signal) {
  const id = query.get("speaker");
  if (id) {
    const filters = new URLSearchParams({
      limit: "20",
      offset: query.get("speechOffset") || "0",
    });
    const [speaker, result, affiliations] = await Promise.all([
      api(`/api/speakers/${encoded(id)}`, signal),
      api(`/api/speakers/${encoded(id)}/speeches?${filters}`, signal),
      api(`/api/speakers/${encoded(id)}/affiliations`, signal),
    ]);
    return node(
      "div",
      {},
      link(
        "← Back to speakers",
        url({ speaker: null, speechOffset: null }),
        "back-link",
        true,
      ),
      heading(
        speaker.label,
        `${number(speaker.speechCount)} stored contributions · Country metadata: ${speaker.country || "Unknown"}`,
        "Speaker contributions",
      ),
      badge(speaker.source),
      message(
        speaker.source === "api"
          ? "This speaker record came from the Parliament API. Affiliation labels below belong to individual speech records; they are not a current membership profile."
          : "This identity is inferred or unresolved. Raw speech labels are preserved; this record is not a verified MEP profile.",
      ),
      affiliations.official?.memberships.length
        ? node(
            "details",
            { class: "panel" },
            node("summary", {}, "Official membership history"),
            node(
              "ul",
              {},
              affiliations.official.memberships.map((membership) =>
                node(
                  "li",
                  {},
                  `${membership.organizationId} · ${membership.start} to ${membership.end || "present"}`,
                ),
              ),
            ),
            link("Parliament source", affiliations.official.sourceUrl),
          )
        : null,
      speechList(result),
      pageNav(result.meta, "speechOffset"),
    );
  }
  const search = field("Speaker name", "q", query.get("q"), "search");
  const source = selectField("Record source", "source", query.get("source"), [
    ["", "All records"],
    ["api", "Parliament API"],
    ["historic", "Inferred historic records"],
    ["unresolved", "Unresolved identities"],
  ]);
  const filters = new URLSearchParams({
    limit: "25",
    offset: query.get("offset") || "0",
    q: query.get("q") || "",
    source: query.get("source") || "",
  });
  const result = await api(`/api/speakers?${filters}`, signal);
  return node(
    "div",
    {},
    heading(
      "Speakers & contributions",
      "Find a name in the corpus. API records, inferred historic records and unresolved identities are marked separately.",
    ),
    form(
      [search, source],
      () =>
        navigate(
          url(
            {
              view: "speakers",
              q: search.input.value,
              source: source.input.value,
            },
            false,
          ),
        ),
      "/?view=speakers",
    ),
    node(
      "div",
      { className: "result-summary" },
      `${number(result.meta.total)} speaker records`,
      "Most recorded contributions first",
    ),
    result.data.length
      ? table(
          ["Speaker", "Country metadata", "Record source", "Contributions"],
          result.data.map((speaker) => [
            link(
              speaker.label,
              url({ speaker: speaker.id, speechOffset: null }),
              "",
              true,
            ),
            speaker.country || "Unknown",
            badge(speaker.source),
            number(speaker.speechCount),
          ]),
          "Speaker records",
        )
      : message(
          "No speakers match this selection. Try another name or reset the filters.",
          "empty",
        ),
    pageNav(result.meta),
  );
}
function svgNode(tag, attrs = {}, text = null) {
  const element = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [name, value] of Object.entries(attrs))
    element.setAttribute(name, String(value));
  if (text != null) element.textContent = text;
  return element;
}
function chart(series) {
  const svg = svgNode("svg", {
    viewBox: "0 0 920 320",
    role: "img",
    "aria-labelledby": "chart-title chart-description",
  });
  svg.append(
    svgNode(
      "title",
      { id: "chart-title" },
      "Recorded speeches by topic over time",
    ),
    svgNode(
      "desc",
      { id: "chart-description" },
      "Line chart of the selected topic counts. Exact values and coverage are available in the data table below. Gaps indicate periods with no topic classifications.",
    ),
  );
  const max = Math.max(
    1,
    ...series.datasets.flatMap((dataset) =>
      dataset.data.filter((value) => value != null),
    ),
  );
  const x = (i) => 58 + (i / Math.max(1, series.labels.length - 1)) * 838;
  const y = (value) => 270 - (value / max) * 235;
  for (let i = 0; i <= 4; i++) {
    const value = (max * i) / 4;
    svg.append(
      svgNode("line", {
        x1: 58,
        y1: y(value),
        x2: 896,
        y2: y(value),
        class: "grid-line",
      }),
      svgNode(
        "text",
        { x: 48, y: y(value) + 4, "text-anchor": "end" },
        number(Math.round(value)),
      ),
    );
  }
  [
    ...new Set([
      0,
      Math.floor((series.labels.length - 1) / 2),
      series.labels.length - 1,
    ]),
  ].forEach((i) => {
    if (i >= 0)
      svg.append(
        svgNode(
          "text",
          {
            x: x(i),
            y: 302,
            "text-anchor":
              i === 0
                ? "start"
                : i === series.labels.length - 1
                  ? "end"
                  : "middle",
          },
          series.labels[i],
        ),
      );
  });
  series.datasets.forEach((dataset, index) => {
    let path = "",
      connected = false;
    dataset.data.forEach((value, i) => {
      if (value == null) {
        connected = false;
        return;
      }
      path += `${connected ? "L" : "M"}${x(i)},${y(value)} `;
      connected = true;
      if (series.labels.length <= 36) {
        const point = svgNode("circle", {
          cx: x(i),
          cy: y(value),
          r: 3,
          class: `series-line series-${index}`,
        });
        point.append(
          svgNode(
            "title",
            {},
            `${dataset.label} · ${series.labels[i]}: ${number(value)}`,
          ),
        );
        svg.append(point);
      }
    });
    svg.append(
      svgNode("path", { d: path, class: `series-line series-${index}` }),
    );
  });
  return node("div", { className: "chart" }, svg);
}
async function topicView(query, signal) {
  const choices = await api("/api/topics", signal);
  let selected;
  try {
    selected = query.has("topics")
      ? JSON.parse(query.get("topics"))
      : choices.data.slice(0, 3).map((row) => row.topic);
  } catch {
    throw new Error(
      "This link has an invalid topic selection. Reset it by choosing Topics in the navigation.",
    );
  }
  if (
    !Array.isArray(selected) ||
    selected.some((value) => typeof value !== "string")
  )
    throw new Error(
      "Invalid topic selection. Choose Topics in the navigation to reset it.",
    );
  const from = field(
    "From month",
    "from",
    query.get("from") || snapshot.stats.fromDate.slice(0, 7),
    "month",
  );
  const to = field(
    "To month",
    "to",
    query.get("to") || snapshot.stats.toDate.slice(0, 7),
    "month",
  );
  const interval = selectField(
    "Group by",
    "interval",
    query.get("interval") || "month",
    [
      ["month", "Month"],
      ["quarter", "Quarter"],
      ["year", "Year"],
    ],
  );
  const topicSearch = node("input", {
    type: "search",
    placeholder: "Find a topic…",
    "aria-label": "Find a topic in the list",
  });
  const selectionCount = para("", "selection-count");
  const checkboxes = [];
  const labels = choices.data.map((item) => {
    const input = node("input", {
      type: "checkbox",
      value: item.topic,
      checked: selected.includes(item.topic),
      name: "topic",
    });
    checkboxes.push(input);
    return node(
      "label",
      { className: "topic-option" },
      input,
      node("span", {}, item.topic),
      node("small", {}, number(item.count)),
    );
  });
  const updateCount = () => {
    const count = checkboxes.filter((input) => input.checked).length;
    selectionCount.textContent = `${count} of 8 topics selected`;
    checkboxes.forEach((input) => {
      input.disabled = !input.checked && count >= 8;
    });
  };
  checkboxes.forEach((input) => input.addEventListener("change", updateCount));
  updateCount();
  topicSearch.addEventListener("input", () =>
    labels.forEach((label) => {
      label.hidden = !label.textContent
        .toLowerCase()
        .includes(topicSearch.value.toLowerCase());
    }),
  );
  const filters = new URLSearchParams({
    topics: JSON.stringify(selected),
    from: from.input.value,
    to: to.input.value,
    interval: interval.input.value,
  });
  const series = await api(`/api/analytics/time-series?${filters}`, signal);
  const controls = node(
    "form",
    {
      className: "topic-controls",
      onSubmit: (event) => {
        event.preventDefault();
        navigate(
          url(
            {
              view: "topics",
              topics: JSON.stringify(
                checkboxes
                  .filter((input) => input.checked)
                  .map((input) => input.value),
              ),
              from: from.input.value,
              to: to.input.value,
              interval: interval.input.value,
            },
            false,
          ),
        );
      },
    },
    node(
      "div",
      {},
      node("h2", {}, "What would you like to explore?"),
      para(
        "Compare up to eight classified macro topics. Counts refer to classified speeches in this snapshot.",
        "muted small",
      ),
      node(
        "div",
        { className: "toolbar" },
        from.wrapper,
        to.wrapper,
        interval.wrapper,
      ),
      node(
        "div",
        { className: "actions" },
        node("button", { type: "submit" }, "Update chart"),
        " ",
        link("Reset", "/?view=topics", "button secondary", true),
      ),
    ),
    node(
      "fieldset",
      {},
      node("legend", {}, "Topic selection"),
      topicSearch,
      node("div", { className: "topic-list" }, labels),
      selectionCount,
    ),
  );
  const content = node(
    "div",
    {},
    heading(
      "Topics over time",
      "Explore the recorded discussion of a topic, with the underlying counts and coverage alongside it.",
    ),
    controls,
  );
  if (!series.labels.length) {
    content.append(
      message(
        "No data matches this selection. Choose at least one topic and a range within the snapshot.",
        "empty",
      ),
    );
    return content;
  }
  const dataTable = table(
    [
      "Period",
      ...series.datasets.map((dataset) => dataset.label),
      "Stored speeches (all topics)",
      "Months observed",
      "Classified speeches",
      "Classification coverage",
    ],
    series.labels.map((label, i) => [
      label,
      ...series.datasets.map((dataset) =>
        dataset.data[i] == null
          ? "No classified speeches"
          : number(dataset.data[i]),
      ),
      number(series.coverage[i].coverage),
      `${series.coverage[i].observedMonths}/${series.coverage[i].months}`,
      number(series.coverage[i].classified),
      series.coverage[i].classificationRate == null
        ? "No stored speeches"
        : `${(series.coverage[i].classificationRate * 100).toFixed(1)}%${series.coverage[i].classificationRate < 1 ? " — partial" : ""}`,
    ]),
    "Topic counts and stored corpus coverage",
  );
  if (!series.datasets.length) {
    content.append(
      message(
        "No topic classifications match this selection. Stored speech coverage is shown below.",
        "empty",
      ),
      dataTable,
    );
    return content;
  }
  content.append(
    node(
      "section",
      { className: "chart-panel" },
      node(
        "div",
        { className: "chart-top" },
        node(
          "div",
          {},
          node("h2", {}, "Recorded speeches by topic"),
          para(
            `${series.from} to ${series.to} · ${series.interval === "month" ? "Monthly" : series.interval === "quarter" ? "Quarterly" : "Yearly"} counts`,
            "muted small",
          ),
        ),
        link(
          "Download this series ↓",
          `/api/export/topics?${filters}`,
          "button secondary",
        ),
      ),
      chart(series),
      node(
        "ul",
        { className: "legend" },
        series.datasets.map((dataset, i) =>
          node(
            "li",
            {},
            node("span", {
              className: `swatch series-${i}`,
              "aria-hidden": "true",
            }),
            dataset.label,
          ),
        ),
      ),
      para(series.note, "chart-caption"),
      para(
        `${number(series.coverage.reduce((sum, period) => sum + period.classified, 0))} classified of ${number(series.coverage.reduce((sum, period) => sum + period.coverage, 0))} stored speeches in this range.`,
        "chart-caption",
      ),
      node(
        "details",
        { className: "series-table" },
        node(
          "summary",
          {},
          `View data table (${number(series.labels.length)} periods)`,
        ),
        dataTable,
      ),
    ),
  );
  return content;
}
async function dataView(query, signal) {
  const stats = snapshot.stats;
  const defaultStart = `${stats.toDate.slice(0, 7)}-01`;
  const start = field(
    "From date",
    "startDate",
    query.get("startDate") || defaultStart,
    "date",
  );
  const end = field(
    "To date",
    "endDate",
    query.get("endDate") || stats.toDate,
    "date",
  );
  const fields = [
    "id",
    "sitting_id",
    "date",
    "speaker_name",
    "political_group",
    "title",
    "speech_content",
    "language",
    "macro_topic",
    "specific_focus",
    "topic",
    "country",
    "mep_id",
  ];
  const chosen = query.has("fields") ? query.get("fields").split(",") : fields;
  const checks = fields.map((name) =>
    node("input", {
      type: "checkbox",
      name: "field",
      value: name,
      checked: chosen.includes(name),
    }),
  );
  const filters = new URLSearchParams({
    startDate: start.input.value,
    endDate: end.input.value,
    fields: chosen.join(","),
  });
  const [count, full] = await Promise.all([
    api(`/api/export/speeches?${filters}&countOnly=true`, signal),
    api("/api/full-dataset/status", signal),
  ]);
  const csvForm = node(
    "form",
    {
      className: "panel",
      onSubmit: (event) => {
        event.preventDefault();
        navigate(
          url(
            {
              view: "data",
              startDate: start.input.value,
              endDate: end.input.value,
              fields: checks
                .filter((input) => input.checked)
                .map((input) => input.value)
                .join(","),
            },
            false,
          ),
        );
      },
    },
    node(
      "div",
      { className: "toolbar" },
      start.wrapper,
      end.wrapper,
      node("button", { type: "submit" }, "Update export selection"),
    ),
    node(
      "details",
      {},
      node("summary", {}, "Choose CSV fields"),
      node(
        "div",
        { className: "field-options" },
        checks.map((input) => node("label", {}, input, input.value)),
      ),
    ),
    para(
      `${number(count.count)} speeches match these dates. The download limit is ${number(count.maxRows)} rows per selection.`,
      "small muted",
    ),
    count.count <= count.maxRows && count.count > 0
      ? link(
          "Download selected speeches ↓",
          `/api/export/speeches?${filters}`,
          "button",
        )
      : message(
          count.count
            ? "Choose a narrower date range to download this selection."
            : "No speeches match this date range.",
        ),
    full.available
      ? para(
          link(
            "Download the complete SQLite snapshot",
            "/api/download/full-dataset",
          ),
        )
      : para(
          "Full snapshot download is not enabled here. Filtered CSV downloads are available above.",
          "small muted",
        ),
  );
  return node(
    "div",
    {},
    heading(
      "Data & About",
      "What is in this snapshot, how it was processed, and how to take a closer look.",
    ),
    fullDownloadPanel(full),
    node(
      "div",
      { className: "prose" },
      node("h2", {}, "A corpus you can explore"),
      para(
        `EUROWATCH brings together ${number(stats.speeches)} stored plenary speeches from ${number(stats.sittings)} European Parliament sittings. This snapshot covers ${dateLabel(stats.fromDate)} to ${dateLabel(stats.toDate)}. It is a collected corpus, not a guarantee of complete parliamentary coverage.`,
      ),
      para(
        `Snapshot prepared ${dateLabel(snapshot.preparedAt.slice(0, 10))}. Its preparation date is separate from the date of the latest stored sitting.`,
      ),
      node(
        "div",
        { className: "data-grid" },
        node(
          "div",
          { className: "panel" },
          node("strong", {}, number(stats.unresolvedSpeakerSpeeches)),
          para(
            "Speeches without a matching speaker profile. Their raw speaker names and text remain available.",
          ),
        ),
        node(
          "div",
          { className: "panel" },
          node("strong", {}, number(stats.missingGroups)),
          para(
            "Speeches without a standardized group label. Unknown does not mean non-attached.",
          ),
        ),
      ),
      node("h2", {}, "How to read the records"),
      node(
        "ul",
        {},
        [
          "A sitting is a dated plenary record. A speech is a contribution extracted from that record. Speaker records are identity metadata, not the current composition of Parliament.",
          "API records come from the Parliament API. Inferred historic records were constructed from speaker labels and may contain parsing errors. Unresolved identities have no matching source profile.",
          `Country metadata is missing for ${number(stats.missingCountryRecords)} of ${number(stats.speakerRecords)} source speaker records. Comparisons by country would describe only the known subset.`,
          "Macro topics are AI-derived categories of agenda headings, assigned to the speeches under those headings. They are not independent classifications of every speech’s argument.",
          `${number(stats.missingTopics)} speeches have no macro-topic label; ${number(stats.lowConfidenceTopics)} have low-confidence topic labels. Existing labels do not reliably record their model or classification run.`,
          "Language labels are automatically detected. They do not establish whether a text is an original speech or a translation. Raw group text and derived group labels are kept separate; neither is a verified membership history.",
          "Topic charts count classified speeches. Blank periods have no topic classifications; zero means classified speeches exist but none match the selected topic. The table and CSV show stored and classified counts and partial coverage. Agenda headings alone are not topic classifications.",
        ].map((text) => node("li", {}, text)),
      ),
      node("h2", {}, "Download speech data"),
      para(
        "This CSV follows only the dates and fields selected here. It does not inherit filters from other views. Topic charts have a separate download of the exact displayed series. Spreadsheet formula-like text is prefixed with an apostrophe for safe opening.",
      ),
      csvForm,
      node("h2", {}, "Sources & project background"),
      para(
        node(
          "span",
          {},
          "Parliamentary source records are linked from each sitting and contribution. The source data is available through the ",
        ),
        link(
          "European Parliament open data portal",
          "https://data.europarl.europa.eu/",
        ),
        ".",
      ),
      para(
        "EUROWATCH is an independent project by Paul Gerrit Koslowsky. It began during his master’s research and now provides a way to explore the underlying collection.",
      ),
      para(
        link(
          "Read the master’s thesis: What they say vs how they say it",
          "https://ciencia.ucp.pt/en/studentTheses/what-they-say-vs-how-they-say-it/",
        ),
      ),
      para(
        link(
          "Portfolio / Contact Paul Koslowsky",
          "https://www.koslowsky.net/#contact",
        ),
      ),
      para(
        link(
          "Source code, setup instructions and issue reporting",
          "https://github.com/paulkoslo/EUROWATCH",
        ),
      ),
      node("h2", {}, "Snapshot details"),
      para(node("span", {}, "Dataset ID: "), node("code", {}, snapshot.id)),
      para(
        "Public browsing is read-only. Data refreshes and maintenance run separately, against offline copies. The explorer does not call an AI service when you browse.",
      ),
      !snapshot.analyticsAvailable &&
        message(
          "Matching topic analytics is unavailable. Sitting and speaker browsing still work.",
        ),
    ),
  );
}
async function render(focus = false) {
  activeRequest?.abort();
  activeRequest = new AbortController();
  const signal = activeRequest.signal;
  const query = new URLSearchParams(location.search);
  const view = [
    "home",
    "sittings",
    "speakers",
    "topics",
    "data",
    "download",
  ].includes(query.get("view"))
    ? query.get("view")
    : "home";
  document.querySelectorAll("[data-view]").forEach((link) => {
    if (link.dataset.view === view) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  });
  main.replaceChildren(message("Loading this selection…", "loading"));
  main.setAttribute("aria-busy", "true");
  try {
    if (!snapshot) snapshot = await api("/api/data-status", signal);
    document.querySelector("#snapshot-status").textContent =
      `${number(snapshot.stats.speeches)} stored speeches · ${dateLabel(snapshot.stats.fromDate)}–${dateLabel(snapshot.stats.toDate)}`;
    const content =
      view === "home"
        ? home()
        : await {
            sittings: sittingView,
            speakers: speakerView,
            topics: topicView,
            data: dataView,
            download: downloadView,
          }[view](query, signal);
    if (signal.aborted) return;
    main.replaceChildren(content);
    document.title = `${view === "home" ? "Parliament speech explorer" : { sittings: "Sittings", speakers: "Speakers", topics: "Topics over time", data: "Data & About", download: "Full Data Download" }[view]} · EUROWATCH`;
  } catch (error) {
    if (signal.aborted) return;
    main.replaceChildren(
      heading(
        "This selection could not be loaded",
        "Your data has not been changed.",
      ),
      message(error.message, "notice error"),
      node("button", { onClick: () => render(true) }, "Try again"),
      " ",
      link("Return to overview", "/", "button secondary", true),
    );
  } finally {
    if (!signal.aborted) {
      main.removeAttribute("aria-busy");
      if (focus) {
        main.focus();
        window.scrollTo({ top: 0 });
      }
    }
  }
}

document.addEventListener("click", (event) => {
  const anchor = event.target.closest("a[data-nav]");
  if (
    !anchor ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey
  )
    return;
  event.preventDefault();
  navigate(anchor.getAttribute("href"));
});
window.addEventListener("popstate", () => render(true));
render();

async function enableLocalRefresh() {
  if (!(await api("/api/localrun")).localrun) return;
  const control = document.querySelector("#local-refresh");
  const button = control.querySelector("button");
  const status = control.querySelector('[role="status"]');
  async function showProgress(job) {
    button.disabled = true;
    while (job.running) {
      status.textContent = job.message;
      await new Promise((resolve) => setTimeout(resolve, 1000));
      job = await api("/api/local-refresh");
    }
    status.textContent = job.message;
    if (job.datasetId && snapshot?.id !== job.datasetId) {
      snapshot = null;
      await render();
    }
  }
  button.addEventListener("click", async () => {
    button.disabled = true;
    status.textContent = "Starting refresh…";
    try {
      const response = await fetch("/api/local-refresh", {
        method: "POST",
        headers: { "X-EUROWATCH-Action": "refresh" },
      });
      const job = await response.json();
      if (!response.ok) throw new Error(job.error);
      await showProgress(job);
    } catch (error) {
      status.textContent = error.message;
    } finally {
      button.disabled = false;
    }
  });
  control.hidden = false;
  try {
    await showProgress(await api("/api/local-refresh"));
  } catch (error) {
    status.textContent = error.message;
  } finally {
    button.disabled = false;
  }
}

enableLocalRefresh().catch(() => {});
