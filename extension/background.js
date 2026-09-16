"use strict";

const BRIDGE_PROTOCOL_VERSION = 12;
const EXTENSION_VERSION = "0.6.5";
const RESPONSE_DOWNLOAD_CAPABILITIES = [
  "prepare_response_download",
  "poll_response_download",
  "cleanup_response_download",
  "download_diagnostics",
  "chrome_internal_diagnostics",
  "automatic_downloads_allow"
];

const MAX_RESPONSE_ARTIFACT_BYTES = 256 * 1024 * 1024;
const CHATGPT_PRIMARY_URL = "https://chatgpt.com/";
const CHATGPT_PRIMARY_PATTERN = "https://chatgpt.com/*";
const RESPONSE_DOWNLOAD_SESSIONS = new Map();
const CHROME_INTERNAL_EVENT_JOURNAL = [];
const MAX_CHROME_INTERNAL_EVENTS = 600;
let chromeInternalEventSequence = 0;

function nowIso() {
  return new Date().toISOString();
}

function appendChromeInternalEvent(type, details = {}) {
  const event = {
    sequence: ++chromeInternalEventSequence,
    recorded_at: nowIso(),
    timestamp_ms: Date.now(),
    type: String(type || "unknown"),
    ...details
  };
  CHROME_INTERNAL_EVENT_JOURNAL.push(event);
  if (CHROME_INTERNAL_EVENT_JOURNAL.length > MAX_CHROME_INTERNAL_EVENTS) {
    CHROME_INTERNAL_EVENT_JOURNAL.splice(
      0,
      CHROME_INTERNAL_EVENT_JOURNAL.length - MAX_CHROME_INTERNAL_EVENTS
    );
  }
  return event;
}

function sanitizedDiagnosticUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  try {
    const parsed = new URL(raw);
    const names = [...parsed.searchParams.keys()];
    parsed.search = "";
    parsed.hash = "";
    return `${parsed.origin}${parsed.pathname}${names.length ? `?${names.map((name) => `${encodeURIComponent(name)}=<redacted>`).join("&")}` : ""}`;
  } catch {
    return raw.slice(0, 1000);
  }
}

function safeHeaderSummary(headers) {
  const allowed = new Set([
    "content-type",
    "content-length",
    "content-disposition",
    "location",
    "cache-control",
    "x-content-type-options"
  ]);
  const result = {};
  for (const header of Array.isArray(headers) ? headers : []) {
    const name = String(header && header.name ? header.name : "").toLowerCase();
    if (!allowed.has(name)) {
      continue;
    }
    result[name] = String(header.value || "").slice(0, 1000);
  }
  return result;
}

function summarizeDownloadDelta(delta) {
  if (!delta) {
    return null;
  }
  const summary = { id: Number(delta.id) };
  for (const key of [
    "state",
    "filename",
    "url",
    "finalUrl",
    "bytesReceived",
    "totalBytes",
    "fileSize",
    "error",
    "danger",
    "paused",
    "canResume",
    "exists"
  ]) {
    const change = delta[key];
    if (!change) {
      continue;
    }
    const previous = change.previous === undefined ? null : change.previous;
    const current = change.current === undefined ? null : change.current;
    summary[key] = key === "url" || key === "finalUrl"
      ? {
          previous: previous === null ? null : sanitizedDiagnosticUrl(previous),
          current: current === null ? null : sanitizedDiagnosticUrl(current)
        }
      : { previous, current };
  }
  return summary;
}

function contentSettingGet(setting, details) {
  return new Promise((resolve) => {
    if (!setting || typeof setting.get !== "function") {
      resolve({ available: false, error: "Chrome content setting API is unavailable." });
      return;
    }
    setting.get(details, (result) => {
      const error = chrome.runtime.lastError;
      if (error) {
        resolve({ available: true, error: error.message });
      } else {
        resolve({ available: true, ...(result || {}) });
      }
    });
  });
}

function contentSettingSet(setting, details) {
  return new Promise((resolve, reject) => {
    if (!setting || typeof setting.set !== "function") {
      reject(new Error("Chrome content setting API is unavailable."));
      return;
    }
    setting.set(details, () => {
      const error = chrome.runtime.lastError;
      if (error) {
        reject(new Error(error.message));
      } else {
        resolve();
      }
    });
  });
}

function permissionsGetAll() {
  return new Promise((resolve) => {
    if (!chrome.permissions || typeof chrome.permissions.getAll !== "function") {
      resolve({ permissions: [], origins: [], error: "Chrome permissions API is unavailable." });
      return;
    }
    chrome.permissions.getAll((result) => {
      const error = chrome.runtime.lastError;
      if (error) {
        resolve({ permissions: [], origins: [], error: error.message });
      } else {
        resolve({
          permissions: Array.isArray(result && result.permissions) ? result.permissions : [],
          origins: Array.isArray(result && result.origins) ? result.origins : []
        });
      }
    });
  });
}

function runtimePlatformInfo() {
  return new Promise((resolve) => {
    chrome.runtime.getPlatformInfo((info) => {
      const error = chrome.runtime.lastError;
      resolve(error ? { error: error.message } : (info || {}));
    });
  });
}

async function automaticDownloadsSnapshot(primaryUrl = CHATGPT_PRIMARY_URL) {
  const api = chrome.contentSettings && chrome.contentSettings.automaticDownloads;
  const regular = await contentSettingGet(api, { primaryUrl, incognito: false });
  const incognito = await contentSettingGet(api, { primaryUrl, incognito: true });
  return { primary_url: primaryUrl, regular, incognito };
}

async function ensureAutomaticDownloadsAllowed(primaryUrl = CHATGPT_PRIMARY_URL) {
  const api = chrome.contentSettings && chrome.contentSettings.automaticDownloads;
  if (!api || typeof api.get !== "function" || typeof api.set !== "function") {
    throw new Error(
      "Chrome automatic-download settings are unavailable; reload the ChatGPT Tab Bridge extension."
    );
  }

  const before = await automaticDownloadsSnapshot(primaryUrl);
  await contentSettingSet(api, {
    primaryPattern: CHATGPT_PRIMARY_PATTERN,
    setting: "allow",
    scope: "regular"
  });
  const after = await automaticDownloadsSnapshot(primaryUrl);
  const effectiveSetting = String(after && after.regular && after.regular.setting || "");
  const levelOfControl = String(after && after.regular && after.regular.levelOfControl || "");
  const result = {
    primary_url: primaryUrl,
    primary_pattern: CHATGPT_PRIMARY_PATTERN,
    before,
    after,
    effective_setting: effectiveSetting,
    level_of_control: levelOfControl
  };
  appendChromeInternalEvent("automaticDownloads.ensure", result);

  if (effectiveSetting !== "allow") {
    throw new Error(
      `Chrome automatic downloads for ChatGPT remain ${effectiveSetting || "unknown"}` +
      `${levelOfControl ? ` (${levelOfControl})` : ""}; allow multiple downloads or reload the extension.`
    );
  }
  return result;
}

function chromeEventsForSession(session) {
  const events = CHROME_INTERNAL_EVENT_JOURNAL.filter(
    (event) => Number(event.sequence) > Number(session.eventSequenceStart || 0)
  );
  const relevant = events.filter((event) => {
    if (event.tab_id !== undefined && Number(event.tab_id) === Number(session.tabId)) {
      return true;
    }
    const text = JSON.stringify(event).toLowerCase();
    return Boolean(
      (session.lookupToken && text.includes(String(session.lookupToken).toLowerCase())) ||
      text.includes("oaiusercontent.com") ||
      text.includes("download") ||
      text.includes("artifact") ||
      text.includes("backend-api/files")
    );
  });
  return {
    total_since_session: events.length,
    relevant_since_session: relevant.length,
    last_sequence: chromeInternalEventSequence,
    events: relevant.slice(-120)
  };
}


function arrayBufferToBase64(buffer) {
  const bytes = new Uint8Array(buffer);
  const chunkSize = 0x8000;
  let binary = "";
  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(index, Math.min(bytes.length, index + chunkSize)));
  }
  return btoa(binary);
}

function downloadsSearch(query) {
  return new Promise((resolve, reject) => {
    chrome.downloads.search(query, (items) => {
      const error = chrome.runtime.lastError;
      if (error) {
        reject(new Error(error.message));
      } else {
        resolve(items || []);
      }
    });
  });
}

function downloadsDownload(options) {
  return new Promise((resolve, reject) => {
    chrome.downloads.download(options, (downloadId) => {
      const error = chrome.runtime.lastError;
      if (error || typeof downloadId !== "number") {
        reject(new Error(error ? error.message : "Chrome did not return a download ID."));
      } else {
        resolve(downloadId);
      }
    });
  });
}

function downloadsCancel(downloadId) {
  return new Promise((resolve) => {
    chrome.downloads.cancel(downloadId, () => {
      void chrome.runtime.lastError;
      resolve();
    });
  });
}

function downloadsRemoveFile(downloadId) {
  return new Promise((resolve) => {
    chrome.downloads.removeFile(downloadId, () => {
      void chrome.runtime.lastError;
      resolve();
    });
  });
}

function downloadsErase(downloadId) {
  return new Promise((resolve) => {
    chrome.downloads.erase({ id: downloadId }, () => {
      void chrome.runtime.lastError;
      resolve();
    });
  });
}

function normalizedFilename(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const clean = raw.split(/[?#]/, 1)[0];
  return clean.split(/[\\/]/).pop() || clean;
}

function artifactLookupToken(expectedFilename) {
  const expected = normalizedFilename(expectedFilename);
  const stem = expected.replace(/\.json$/i, "");
  const timestampSuffix = stem.match(/(\d{8}_\d{6}(?:_[A-Za-z0-9]+)*)$/);
  return timestampSuffix ? timestampSuffix[1] : stem;
}

function safeMaxBytes(value) {
  return Math.min(
    MAX_RESPONSE_ARTIFACT_BYTES,
    Math.max(1, Number(value) || MAX_RESPONSE_ARTIFACT_BYTES)
  );
}

function downloadStartedAt(item) {
  const parsed = Date.parse(String(item && item.startTime ? item.startTime : ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function summarizeDownload(item) {
  if (!item) {
    return null;
  }
  return {
    id: Number(item.id),
    state: String(item.state || ""),
    filename: String(item.filename || ""),
    actual_filename: normalizedFilename(item.filename),
    url: sanitizedDiagnosticUrl(item.url),
    final_url: sanitizedDiagnosticUrl(item.finalUrl),
    start_time: String(item.startTime || ""),
    end_time: String(item.endTime || ""),
    bytes_received: Number(item.bytesReceived || 0),
    total_bytes: Number(item.totalBytes || 0),
    file_size: Number(item.fileSize || 0),
    mime: String(item.mime || ""),
    error: String(item.error || ""),
    danger: String(item.danger || ""),
    paused: Boolean(item.paused),
    can_resume: Boolean(item.canResume),
    exists: item.exists === undefined ? null : Boolean(item.exists)
  };
}

async function responseDownloadDiagnostics(session) {
  const allItems = await downloadsSearch({});
  const newItems = allItems
    .filter((item) => {
      if (session.existingIds.has(Number(item.id))) {
        return false;
      }
      const startedAt = downloadStartedAt(item);
      return !startedAt || startedAt >= session.startedAt - 5000;
    })
    .sort((a, b) => downloadStartedAt(b) - downloadStartedAt(a));
  const matchingItems = newItems.filter((item) => downloadMatchesSession(item, session));
  const automaticDownloads = await automaticDownloadsSnapshot(session.primaryUrl);
  return {
    background_protocol_version: BRIDGE_PROTOCOL_VERSION,
    extension_version: EXTENSION_VERSION,
    session_started_at_ms: session.startedAt,
    session_age_ms: Math.max(0, Date.now() - session.startedAt),
    expected_filename: session.expectedFilename,
    lookup_token: session.lookupToken,
    tab_id: Number.isInteger(session.tabId) ? session.tabId : null,
    baseline_download_count: session.existingIds.size,
    new_download_count: newItems.length,
    matching_download_count: matchingItems.length,
    tracked_download_id: Number.isInteger(session.downloadId) ? session.downloadId : null,
    new_downloads: newItems.slice(0, 20).map(summarizeDownload),
    chrome_internal: {
      automatic_downloads: automaticDownloads,
      events: chromeEventsForSession(session)
    }
  };
}

function downloadMatchesSession(item, session) {
  if (!item || session.existingIds.has(Number(item.id))) {
    return false;
  }
  if (downloadStartedAt(item) && downloadStartedAt(item) < session.startedAt - 5000) {
    return false;
  }
  const searchable = [
    normalizedFilename(item.filename),
    item.filename,
    item.url,
    item.finalUrl
  ]
    .map((value) => String(value || ""))
    .join(" ");
  return Boolean(session.lookupToken && searchable.includes(session.lookupToken));
}

async function locateSessionDownload(session) {
  if (typeof session.downloadId === "number") {
    const items = await downloadsSearch({ id: session.downloadId });
    return items[0] || null;
  }

  const allItems = await downloadsSearch({});
  const candidates = allItems
    .filter((item) => downloadMatchesSession(item, session))
    .sort((a, b) => downloadStartedAt(b) - downloadStartedAt(a));

  if (candidates.length > 1) {
    const exact = candidates.filter(
      (item) => normalizedFilename(item.filename) === session.expectedFilename
    );
    if (exact.length === 1) {
      session.downloadId = Number(exact[0].id);
      return exact[0];
    }
    throw new Error(
      `Found ${candidates.length} new downloads containing response ID ${session.lookupToken}; exactly one is required.`
    );
  }

  const item = candidates[0] || null;
  if (item) {
    session.downloadId = Number(item.id);
  }
  return item;
}

async function cleanupDownloadSession(sessionToken, explicitDownloadId = null) {
  const session = RESPONSE_DOWNLOAD_SESSIONS.get(sessionToken) || null;
  const downloadId = Number.isInteger(explicitDownloadId)
    ? explicitDownloadId
    : session && Number.isInteger(session.downloadId)
      ? session.downloadId
      : null;

  if (Number.isInteger(downloadId)) {
    const items = await downloadsSearch({ id: downloadId });
    const item = items[0] || null;
    if (item && item.state === "in_progress") {
      await downloadsCancel(downloadId);
    }
    await downloadsRemoveFile(downloadId);
    await downloadsErase(downloadId);
  }
  if (sessionToken) {
    RESPONSE_DOWNLOAD_SESSIONS.delete(sessionToken);
  }
  return { download_id: downloadId };
}

async function waitForDownload(downloadId, maxBytes, timeoutMs = 30 * 60 * 1000) {
  const deadline = Date.now() + timeoutMs;
  let lastBytes = -1;
  let lastProgressAt = Date.now();
  while (Date.now() < deadline) {
    const items = await downloadsSearch({ id: downloadId });
    const item = items[0];
    if (item) {
      if (item.state === "interrupted") {
        throw new Error(`Response artifact download was interrupted: ${item.error || "unknown error"}.`);
      }
      const bytesReceived = Number(item.bytesReceived || 0);
      if (bytesReceived !== lastBytes) {
        lastBytes = bytesReceived;
        lastProgressAt = Date.now();
      }
      if (Date.now() - lastProgressAt > 5 * 60 * 1000) {
        throw new Error("Response artifact download made no progress for five minutes.");
      }
      if (item.state === "complete") {
        const size = Number(item.fileSize || item.totalBytes || bytesReceived || 0);
        if (size > maxBytes) {
          throw new Error(`Response artifact exceeds the ${maxBytes}-byte limit.`);
        }
        return {
          download_id: downloadId,
          local_path: String(item.filename || ""),
          actual_filename: normalizedFilename(item.filename),
          size_bytes: size,
          bytes_received: bytesReceived,
          total_bytes: Number(item.totalBytes || 0),
          mime_type: String(item.mime || "application/json"),
          state: "complete"
        };
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 750));
  }
  throw new Error("Timed out waiting for the response artifact download to complete.");
}

async function newestCompletedDownload(expectedFilename, maxBytes) {
  const token = artifactLookupToken(expectedFilename);
  const items = await downloadsSearch({});
  const completed = items
    .filter(
      (item) =>
        item.state === "complete" &&
        [item.filename, item.url, item.finalUrl]
          .map((value) => String(value || ""))
          .join(" ")
          .includes(token)
    )
    .sort((a, b) => downloadStartedAt(b) - downloadStartedAt(a));
  const item = completed[0];
  if (!item) {
    return null;
  }
  const size = Number(item.fileSize || item.totalBytes || item.bytesReceived || 0);
  if (size > maxBytes) {
    throw new Error(`Response artifact exceeds the ${maxBytes}-byte limit.`);
  }
  return {
    download_id: Number(item.id),
    local_path: String(item.filename || ""),
    actual_filename: normalizedFilename(item.filename),
    size_bytes: size,
    bytes_received: Number(item.bytesReceived || 0),
    total_bytes: Number(item.totalBytes || 0),
    mime_type: String(item.mime || "application/json"),
    state: "complete"
  };
}

if (chrome.downloads && chrome.downloads.onCreated) {
  chrome.downloads.onCreated.addListener((item) => {
    appendChromeInternalEvent("downloads.onCreated", { download: summarizeDownload(item) });
  });
}

if (chrome.downloads && chrome.downloads.onChanged) {
  chrome.downloads.onChanged.addListener((delta) => {
    appendChromeInternalEvent("downloads.onChanged", { delta: summarizeDownloadDelta(delta) });
  });
}

if (chrome.downloads && chrome.downloads.onErased) {
  chrome.downloads.onErased.addListener((downloadId) => {
    appendChromeInternalEvent("downloads.onErased", { download_id: Number(downloadId) });
  });
}

function recordWebRequestEvent(type, details, extra = {}) {
  appendChromeInternalEvent(type, {
    request_id: String(details.requestId || ""),
    tab_id: Number(details.tabId),
    frame_id: Number(details.frameId),
    parent_frame_id: details.parentFrameId === undefined ? null : Number(details.parentFrameId),
    method: String(details.method || ""),
    resource_type: String(details.type || ""),
    url: sanitizedDiagnosticUrl(details.url),
    initiator: sanitizedDiagnosticUrl(details.initiator || details.originUrl || ""),
    document_url: sanitizedDiagnosticUrl(details.documentUrl || ""),
    browser_timestamp_ms: Number(details.timeStamp || 0),
    ...extra
  });
}

if (chrome.webRequest) {
  const requestFilter = {
    urls: [
      "https://chatgpt.com/*",
      "https://*.oaiusercontent.com/*",
      "https://*.openai.com/*"
    ]
  };
  chrome.webRequest.onBeforeRequest.addListener(
    (details) => recordWebRequestEvent("webRequest.onBeforeRequest", details),
    requestFilter
  );
  chrome.webRequest.onHeadersReceived.addListener(
    (details) => recordWebRequestEvent("webRequest.onHeadersReceived", details, {
      status_code: Number(details.statusCode || 0),
      status_line: String(details.statusLine || ""),
      response_headers: safeHeaderSummary(details.responseHeaders)
    }),
    requestFilter,
    ["responseHeaders"]
  );
  chrome.webRequest.onBeforeRedirect.addListener(
    (details) => recordWebRequestEvent("webRequest.onBeforeRedirect", details, {
      status_code: Number(details.statusCode || 0),
      redirect_url: sanitizedDiagnosticUrl(details.redirectUrl),
      response_headers: safeHeaderSummary(details.responseHeaders)
    }),
    requestFilter,
    ["responseHeaders"]
  );
  chrome.webRequest.onCompleted.addListener(
    (details) => recordWebRequestEvent("webRequest.onCompleted", details, {
      status_code: Number(details.statusCode || 0),
      from_cache: Boolean(details.fromCache),
      response_headers: safeHeaderSummary(details.responseHeaders)
    }),
    requestFilter,
    ["responseHeaders"]
  );
  chrome.webRequest.onErrorOccurred.addListener(
    (details) => recordWebRequestEvent("webRequest.onErrorOccurred", details, {
      error: String(details.error || "")
    }),
    requestFilter
  );
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message && message.type === "get_bridge_tab_id") {
    if (!sender.tab || typeof sender.tab.id !== "number") {
      sendResponse({ ok: false, error: "Chrome tab ID is unavailable." });
      return false;
    }
    (async () => {
      try {
        const automaticDownloads = await ensureAutomaticDownloadsAllowed(
          sender.tab && sender.tab.url ? String(sender.tab.url) : CHATGPT_PRIMARY_URL
        );
        sendResponse({
          ok: true,
          bridge_tab_id: `tab_${sender.tab.id}`,
          protocol_version: BRIDGE_PROTOCOL_VERSION,
          extension_version: EXTENSION_VERSION,
          capabilities: RESPONSE_DOWNLOAD_CAPABILITIES,
          automatic_downloads: automaticDownloads
        });
      } catch (error) {
        appendChromeInternalEvent("automaticDownloads.registration_failed", {
          tab_id: Number(sender.tab.id),
          error: error instanceof Error ? error.message : String(error)
        });
        sendResponse({
          ok: false,
          error: error instanceof Error ? error.message : String(error),
          protocol_version: BRIDGE_PROTOCOL_VERSION,
          extension_version: EXTENSION_VERSION
        });
      }
    })();
    return true;
  }

  if (message && message.type === "fetch_response_artifact") {
    const url = String(message.url || "").trim();
    const maxBytes = safeMaxBytes(message.max_bytes);
    if (!url.startsWith("https://chatgpt.com/") && !/^https:\/\/[^/]+\.oaiusercontent\.com\//i.test(url)) {
      sendResponse({ ok: false, error: "Response artifact URL is outside the allowed ChatGPT domains." });
      return false;
    }
    (async () => {
      try {
        const response = await fetch(url, { credentials: "include", cache: "no-store" });
        if (!response.ok) {
          throw new Error(`Response artifact fetch returned HTTP ${response.status}.`);
        }
        const declaredLength = Number(response.headers.get("content-length") || 0);
        if (declaredLength > maxBytes) {
          throw new Error(`Response artifact exceeds the ${maxBytes}-byte limit.`);
        }
        const buffer = await response.arrayBuffer();
        if (!buffer.byteLength || buffer.byteLength > maxBytes) {
          throw new Error(`Response artifact size ${buffer.byteLength} is outside the allowed range.`);
        }
        sendResponse({
          ok: true,
          mime_type: String(response.headers.get("content-type") || "application/octet-stream"),
          size_bytes: buffer.byteLength,
          content_base64: arrayBufferToBase64(buffer)
        });
      } catch (error) {
        sendResponse({ ok: false, error: error instanceof Error ? error.message : String(error) });
      }
    })();
    return true;
  }

  if (message && message.type === "download_response_artifact") {
    const url = String(message.url || "").trim();
    const expectedFilename = String(message.filename || "").trim();
    const maxBytes = safeMaxBytes(message.max_bytes);
    (async () => {
      try {
        if (!url || !expectedFilename.toLowerCase().endsWith(".json")) {
          throw new Error("A response artifact URL and exact .json filename are required.");
        }
        const downloadId = await downloadsDownload({
          url,
          filename: `WebAI_Harmony/${expectedFilename}`,
          conflictAction: "overwrite",
          saveAs: false
        });
        const item = await waitForDownload(downloadId, maxBytes);
        sendResponse({ ok: true, ...item });
      } catch (error) {
        sendResponse({ ok: false, error: error instanceof Error ? error.message : String(error) });
      }
    })();
    return true;
  }

  if (message && message.type === "prepare_response_download") {
    const expectedFilename = String(message.filename || "").trim();
    const maxBytes = safeMaxBytes(message.max_bytes);
    (async () => {
      try {
        if (!expectedFilename.toLowerCase().endsWith(".json")) {
          throw new Error("An exact .json response filename is required.");
        }
        const primaryUrl = sender.tab && sender.tab.url ? String(sender.tab.url) : CHATGPT_PRIMARY_URL;
        const automaticDownloadsEnforcement = await ensureAutomaticDownloadsAllowed(primaryUrl);
        const existing = await downloadsSearch({});
        const sessionToken = `artifact_${Date.now()}_${Math.random().toString(16).slice(2)}`;
        const session = {
          expectedFilename,
          lookupToken: artifactLookupToken(expectedFilename),
          maxBytes,
          startedAt: Date.now(),
          existingIds: new Set(existing.map((item) => Number(item.id))),
          downloadId: null,
          tabId: sender.tab && Number.isInteger(sender.tab.id) ? Number(sender.tab.id) : null,
          primaryUrl,
          eventSequenceStart: chromeInternalEventSequence
        };
        RESPONSE_DOWNLOAD_SESSIONS.set(sessionToken, session);
        const [automaticDownloads, permissions, platform] = await Promise.all([
          automaticDownloadsSnapshot(session.primaryUrl),
          permissionsGetAll(),
          runtimePlatformInfo()
        ]);
        appendChromeInternalEvent("response_download_session.prepared", {
          session_token: sessionToken,
          tab_id: session.tabId,
          expected_filename: expectedFilename,
          lookup_token: session.lookupToken,
          automatic_downloads: automaticDownloads
        });
        sendResponse({
          ok: true,
          session_token: sessionToken,
          protocol_version: BRIDGE_PROTOCOL_VERSION,
          extension_version: EXTENSION_VERSION,
          diagnostics: {
            background_protocol_version: BRIDGE_PROTOCOL_VERSION,
            extension_version: EXTENSION_VERSION,
            expected_filename: expectedFilename,
            lookup_token: session.lookupToken,
            tab_id: session.tabId,
            baseline_download_count: existing.length,
            recent_baseline_downloads: existing
              .sort((a, b) => downloadStartedAt(b) - downloadStartedAt(a))
              .slice(0, 8)
              .map(summarizeDownload),
            chrome_internal: {
              automatic_downloads: automaticDownloads,
              automatic_downloads_enforcement: automaticDownloadsEnforcement,
              extension_permissions: permissions,
              platform,
              event_sequence_start: session.eventSequenceStart,
              event_sequence_after_prepare: chromeInternalEventSequence
            }
          }
        });
      } catch (error) {
        sendResponse({ ok: false, error: error instanceof Error ? error.message : String(error) });
      }
    })();
    return true;
  }

  if (message && message.type === "poll_response_download") {
    const sessionToken = String(message.session_token || "").trim();
    (async () => {
      try {
        const session = RESPONSE_DOWNLOAD_SESSIONS.get(sessionToken);
        if (!session) {
          throw new Error("Response download session is missing or expired.");
        }
        const item = await locateSessionDownload(session);
        const diagnostics = await responseDownloadDiagnostics(session);
        if (!item) {
          sendResponse({ ok: true, state: "waiting_start", diagnostics });
          return;
        }
        const downloadId = Number(item.id);
        if (item.state === "interrupted") {
          throw new Error(`Response artifact download was interrupted: ${item.error || "unknown error"}.`);
        }
        const bytesReceived = Number(item.bytesReceived || 0);
        const totalBytes = Number(item.totalBytes || 0);
        const size = Number(item.fileSize || totalBytes || bytesReceived || 0);
        if (size > session.maxBytes || bytesReceived > session.maxBytes) {
          await cleanupDownloadSession(sessionToken, downloadId);
          throw new Error(`Response artifact exceeds the ${session.maxBytes}-byte limit.`);
        }
        sendResponse({
          ok: true,
          state: String(item.state || "in_progress"),
          download_id: downloadId,
          local_path: String(item.filename || ""),
          actual_filename: normalizedFilename(item.filename),
          size_bytes: item.state === "complete" ? size : 0,
          bytes_received: bytesReceived,
          total_bytes: totalBytes,
          mime_type: String(item.mime || "application/json"),
          diagnostics
        });
      } catch (error) {
        sendResponse({ ok: false, error: error instanceof Error ? error.message : String(error) });
      }
    })();
    return true;
  }

  if (message && message.type === "cleanup_response_download") {
    const sessionToken = String(message.session_token || "").trim();
    const suppliedDownloadId = message.download_id;
    const rawDownloadId =
      suppliedDownloadId === null || suppliedDownloadId === undefined || suppliedDownloadId === ""
        ? null
        : Number(suppliedDownloadId);
    const downloadId = Number.isInteger(rawDownloadId) ? rawDownloadId : null;
    (async () => {
      try {
        const cleaned = await cleanupDownloadSession(sessionToken, downloadId);
        sendResponse({ ok: true, ...cleaned });
      } catch (error) {
        sendResponse({ ok: false, error: error instanceof Error ? error.message : String(error) });
      }
    })();
    return true;
  }

  if (message && message.type === "find_response_download") {
    const expectedFilename = String(message.filename || "").trim();
    const maxBytes = safeMaxBytes(message.max_bytes);
    (async () => {
      try {
        const item = await newestCompletedDownload(expectedFilename, maxBytes);
        sendResponse(item ? { ok: true, ...item } : { ok: false, error: "Download not found yet." });
      } catch (error) {
        sendResponse({ ok: false, error: error instanceof Error ? error.message : String(error) });
      }
    })();
    return true;
  }

  return false;
});
