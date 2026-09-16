(() => {
  "use strict";

  const BRIDGE_URL = "ws://127.0.0.1:8765";
  const BRIDGE_PROTOCOL_VERSION = 12;
  const POLL_INTERVAL_MS = 400;
  const STABLE_POLLS_REQUIRED = 4;
  const DEFAULT_INITIAL_RESPONSE_TIMEOUT_MS = 120000;
  const DEFAULT_STREAM_IDLE_TIMEOUT_MS = 45000;
  const DEFAULT_ABSOLUTE_TIMEOUT_MS = 300000;
  const MAX_ABSOLUTE_TIMEOUT_MS = 2147483000;
  const RECONNECT_DELAY_MS = 2000;
  const STREAM_LOG_INTERVAL_MS = 5000;
  const DEFAULT_TEXT_SUBMISSION_THRESHOLD_CHARS = 50000;
  const ATTACHMENT_UPLOAD_TIMEOUT_MS = 15000;
  const ATTACHMENT_UPLOAD_MAX_ATTEMPTS = 2;
  const DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES = 256 * 1024 * 1024;
  const RESPONSE_ARTIFACT_POLL_MS = 750;
  const RESPONSE_ARTIFACT_POST_RESPONSE_WAIT_MS = 60000;
  const RESPONSE_ARTIFACT_COMPLETION_TIMEOUT_MS = 30 * 60 * 1000;
  const RESPONSE_ARTIFACT_IDLE_TIMEOUT_MS = 5 * 60 * 1000;
  const RESPONSE_ARTIFACT_FALLBACK_CLEANUP_MS = 30 * 60 * 1000;

  let socket = null;
  let reconnectTimer = null;
  let shuttingDown = false;
  let bridgeTabId = null;
  let activeRequest = null;
  let backgroundRuntimeInfo = null;

  async function loadBridgeTabId() {
    const response = await chrome.runtime.sendMessage({ type: "get_bridge_tab_id" });
    if (!response || response.ok !== true || !response.bridge_tab_id) {
      throw new Error(
        response && response.error
          ? response.error
          : "Unable to obtain the Chrome tab ID."
      );
    }
    bridgeTabId = String(response.bridge_tab_id);
    backgroundRuntimeInfo = {
      protocol_version: Number(response.protocol_version || 0),
      extension_version: String(response.extension_version || ""),
      capabilities: Array.isArray(response.capabilities) ? response.capabilities.map(String) : [],
      automatic_downloads: response.automatic_downloads || null
    };
    if (backgroundRuntimeInfo.protocol_version !== BRIDGE_PROTOCOL_VERSION) {
      throw new Error(
        `ChatGPT Tab Bridge background protocol mismatch: background=${backgroundRuntimeInfo.protocol_version}, content=${BRIDGE_PROTOCOL_VERSION}; reload the extension.`
      );
    }
    if (!backgroundRuntimeInfo.capabilities.includes("automatic_downloads_allow")) {
      throw new Error("ChatGPT Tab Bridge background worker is stale; reload the extension.");
    }
    const automaticSetting = String(
      backgroundRuntimeInfo.automatic_downloads &&
      backgroundRuntimeInfo.automatic_downloads.after &&
      backgroundRuntimeInfo.automatic_downloads.after.regular &&
      backgroundRuntimeInfo.automatic_downloads.after.regular.setting ||
      ""
    );
    if (automaticSetting !== "allow") {
      throw new Error(
        `Chrome automatic downloads for ChatGPT are ${automaticSetting || "unknown"}; reload the extension or allow multiple downloads.`
      );
    }
  }

  function getBridgeTabId() {
    if (!bridgeTabId) {
      throw new Error("Bridge tab ID is not initialized.");
    }
    return bridgeTabId;
  }

  function getConversationId() {
    const match = location.pathname.match(/^\/c\/([^/?#]+)/);
    return match ? match[1] : null;
  }

  function send(message) {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      throw new Error("Bridge WebSocket is not connected.");
    }
    socket.send(JSON.stringify(message));
  }

  function safeSend(message) {
    try {
      send(message);
    } catch (error) {
      console.error("ChatGPT Tab Bridge could not send a bridge message.", error);
    }
  }

  function tabState(type) {
    return {
      type,
      bridge_tab_id: getBridgeTabId(),
      conversation_id: getConversationId(),
      url: location.href,
      title: document.title,
      status: activeRequest ? "busy" : "ready",
      protocol_version: BRIDGE_PROTOCOL_VERSION
    };
  }

  function requestEvent(eventName, requestId, details = {}) {
    safeSend({
      type: "prompt_event",
      request_id: String(requestId || ""),
      event_name: String(eventName || ""),
      bridge_tab_id: getBridgeTabId(),
      conversation_id: getConversationId(),
      url: location.href,
      title: document.title,
      details
    });
  }

  function clippedDiagnosticText(value, limit = 500) {
    const text = String(value || "").replace(/\s+/g, " ").trim();
    return text.length <= limit ? text : `${text.slice(0, limit)}…`;
  }

  function diagnosticElementName(element) {
    if (!(element instanceof Element)) {
      return "";
    }
    const tag = element.tagName.toLowerCase();
    const id = element.id ? `#${element.id}` : "";
    const testId = element.getAttribute("data-testid");
    const role = element.getAttribute("role");
    const aria = element.getAttribute("aria-label");
    return clippedDiagnosticText(
      [tag + id, testId ? `[data-testid=${testId}]` : "", role ? `[role=${role}]` : "", aria ? `[aria-label=${aria}]` : ""]
        .filter(Boolean)
        .join(""),
      240
    );
  }

  function diagnosticUserActivation() {
    const activation = navigator.userActivation;
    if (!activation) {
      return { available: false };
    }
    return {
      available: true,
      is_active: Boolean(activation.isActive),
      has_been_active: Boolean(activation.hasBeenActive)
    };
  }

  function diagnosticElementSnapshot(element) {
    if (!(element instanceof HTMLElement)) {
      return { present: false };
    }
    const rect = element.getBoundingClientRect();
    const style = getComputedStyle(element);
    const centerX = rect.width > 0 ? rect.left + rect.width / 2 : null;
    const centerY = rect.height > 0 ? rect.top + rect.height / 2 : null;
    const topmost =
      centerX !== null && centerY !== null &&
      centerX >= 0 && centerY >= 0 && centerX <= innerWidth && centerY <= innerHeight
        ? document.elementFromPoint(centerX, centerY)
        : null;
    const disabled =
      (element instanceof HTMLButtonElement && element.disabled) ||
      String(element.getAttribute("aria-disabled") || "").toLowerCase() === "true";
    const visible =
      element.isConnected &&
      rect.width > 0 &&
      rect.height > 0 &&
      style.display !== "none" &&
      style.visibility !== "hidden" &&
      Number(style.opacity || "1") > 0;
    return {
      present: true,
      name: diagnosticElementName(element),
      tag: element.tagName.toLowerCase(),
      connected: element.isConnected,
      disabled,
      visible,
      offset_parent_present: element.offsetParent !== null,
      display: style.display,
      visibility: style.visibility,
      opacity: style.opacity,
      pointer_events: style.pointerEvents,
      cursor: style.cursor,
      tab_index: element.tabIndex,
      active: document.activeElement === element,
      aria_label: clippedDiagnosticText(element.getAttribute("aria-label"), 240),
      title: clippedDiagnosticText(element.getAttribute("title"), 240),
      text: clippedDiagnosticText(element.innerText || element.textContent, 300),
      rect: {
        x: Math.round(rect.x * 100) / 100,
        y: Math.round(rect.y * 100) / 100,
        width: Math.round(rect.width * 100) / 100,
        height: Math.round(rect.height * 100) / 100
      },
      center: centerX === null ? null : {
        x: Math.round(centerX * 100) / 100,
        y: Math.round(centerY * 100) / 100
      },
      topmost: diagnosticElementName(topmost),
      topmost_is_target: Boolean(topmost && (topmost === element || element.contains(topmost))),
      outer_html: clippedDiagnosticText(element.outerHTML, 600)
    };
  }

  function diagnosticPageFeedback(root = document) {
    const scope = root instanceof Element || root instanceof Document ? root : document;
    const selectors = [
      '[role="alert"]',
      '[role="status"]',
      '[aria-live="assertive"]',
      '[aria-live="polite"]',
      '[data-testid*="toast"]',
      '[data-testid*="error"]'
    ];
    const messages = [];
    for (const element of scope.querySelectorAll(selectors.join(","))) {
      if (!(element instanceof HTMLElement)) {
        continue;
      }
      const text = clippedDiagnosticText(element.innerText || element.textContent, 400);
      if (text && !messages.includes(text)) {
        messages.push(text);
      }
      if (messages.length >= 8) {
        break;
      }
    }
    return messages;
  }

  function diagnosticClick(element) {
    const records = [];
    const userActivationBefore = diagnosticUserActivation();
    const registrations = [];
    const eventTypes = ["pointerdown", "mousedown", "pointerup", "mouseup", "click"];
    const recordEvent = (phase) => (event) => {
      const path = typeof event.composedPath === "function" ? event.composedPath() : [];
      if (event.target !== element && !element.contains(event.target) && !path.includes(element)) {
        return;
      }
      records.push({
        phase,
        type: event.type,
        is_trusted: event.isTrusted,
        default_prevented: event.defaultPrevented,
        cancel_bubble: event.cancelBubble,
        button: typeof event.button === "number" ? event.button : null,
        detail: typeof event.detail === "number" ? event.detail : null,
        target: diagnosticElementName(event.target)
      });
    };
    for (const type of eventTypes) {
      const capture = recordEvent("capture");
      const bubble = recordEvent("bubble");
      document.addEventListener(type, capture, true);
      document.addEventListener(type, bubble, false);
      registrations.push([type, capture, true], [type, bubble, false]);
    }
    let error = "";
    try {
      element.click();
    } catch (caught) {
      error = caught instanceof Error ? caught.message : String(caught);
    } finally {
      for (const [type, listener, capture] of registrations) {
        document.removeEventListener(type, listener, capture);
      }
    }
    return {
      error,
      user_activation_before: userActivationBefore,
      user_activation_after: diagnosticUserActivation(),
      events: records,
      after: diagnosticElementSnapshot(element),
      page_feedback: diagnosticPageFeedback(document)
    };
  }

  function diagnosticPreviewSnapshot(preview) {
    if (!(preview instanceof HTMLElement)) {
      return { present: false };
    }
    const buttons = [...preview.querySelectorAll('button, [role="button"]')]
      .filter((element) => element instanceof HTMLElement)
      .slice(0, 20)
      .map((element) => diagnosticElementSnapshot(element));
    return {
      present: true,
      panel: diagnosticElementSnapshot(preview),
      text_length: String(preview.innerText || preview.textContent || "").length,
      text_preview: clippedDiagnosticText(preview.innerText || preview.textContent, 800),
      interactive_controls: buttons,
      feedback: diagnosticPageFeedback(preview)
    };
  }

  function scheduleReconnect() {
    if (shuttingDown || reconnectTimer !== null) {
      return;
    }
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, RECONNECT_DELAY_MS);
  }

  function resetActiveRequest(reason) {
    const request = activeRequest;
    activeRequest = null;
    if (request) {
      requestEvent("state_reset", request.requestId, { reason: String(reason || "completed") });
    }
    if (socket && socket.readyState === WebSocket.OPEN) {
      safeSend(tabState("tab_update"));
    }
  }

  function connect() {
    if (
      socket &&
      (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)
    ) {
      return;
    }

    socket = new WebSocket(BRIDGE_URL);

    socket.addEventListener("open", () => {
      if (reconnectTimer !== null) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      send(tabState("register_tab"));
    });

    socket.addEventListener("message", async (event) => {
      let message;
      try {
        message = JSON.parse(event.data);
      } catch {
        return;
      }


      if (message.type === "tab_registered") {
        if (Number(message.protocol_version) !== BRIDGE_PROTOCOL_VERSION) {
          console.error(
            `ChatGPT Tab Bridge protocol mismatch: server=${message.protocol_version}, extension=${BRIDGE_PROTOCOL_VERSION}.`
          );
          socket.close();
        }
        return;
      }

      if (message.type === "error") {
        console.error(`ChatGPT Tab Bridge server error: ${String(message.error || "unknown error")}`);
        return;
      }

      if (message.type === "cancel_prompt") {
        if (
          activeRequest &&
          String(message.request_id || "") === activeRequest.requestId
        ) {
          requestEvent("cancelled", activeRequest.requestId, {
            reason: String(message.reason || "bridge_cancelled")
          });
          activeRequest.controller.abort(
            new Error(String(message.reason || "Bridge cancelled the active request."))
          );
        }
        return;
      }

      if (message.type === "cleanup_response_artifact") {
        const cleanupRequestId = String(message.request_id || "");
        try {
          const rawCleanupDownloadId = message.download_id;
          const parsedCleanupDownloadId =
            rawCleanupDownloadId === null ||
            rawCleanupDownloadId === undefined ||
            rawCleanupDownloadId === ""
              ? null
              : Number(rawCleanupDownloadId);
          const cleanup = await cleanupResponseDownload(
            String(message.cleanup_token || ""),
            Number.isInteger(parsedCleanupDownloadId) ? parsedCleanupDownloadId : null
          );
          safeSend({
            type: "artifact_cleanup_result",
            request_id: cleanupRequestId,
            download_id: cleanup.download_id,
            cleaned: true
          });
        } catch (error) {
          safeSend({
            type: "artifact_cleanup_error",
            request_id: cleanupRequestId,
            error: error instanceof Error ? error.message : String(error)
          });
        }
        return;
      }

      if (message.type !== "send_prompt") {
        return;
      }

      if (activeRequest) {
        safeSend({
          type: "prompt_error",
          request_id: message.request_id,
          error: "This ChatGPT tab is already processing another prompt."
        });
        return;
      }

      const requestId = String(message.request_id || "");
      const controller = new AbortController();
      activeRequest = {
        requestId,
        controller,
        tracker: null
      };
      const effectiveTimeout = timeoutConfig(message.timeout_config || {});
      const submissionConfig = {
        mode: String(message.submission_mode || "auto").trim().toLowerCase(),
        filename: String(message.prompt_filename || "complete_prompt.txt").trim() || "complete_prompt.txt",
        thresholdChars: Math.max(
          1,
          Number(message.text_submission_threshold_chars) || DEFAULT_TEXT_SUBMISSION_THRESHOLD_CHARS
        ),
        promptSha256: String(message.prompt_sha256 || "")
      };
      const responseConfig = {
        mode: String(message.response_mode || "text").trim().toLowerCase(),
        filename: String(message.response_filename || "").trim(),
        maxBytes: Math.min(
          DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES,
          Math.max(1, Number(message.response_max_bytes) || DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES)
        ),
        startTimeoutMs: Math.max(
          RESPONSE_ARTIFACT_POLL_MS,
          Number(message.response_artifact_start_timeout_ms) || RESPONSE_ARTIFACT_POST_RESPONSE_WAIT_MS
        ),
        completionTimeoutMs: Math.max(
          RESPONSE_ARTIFACT_POLL_MS,
          Number(message.response_artifact_completion_timeout_ms) || RESPONSE_ARTIFACT_COMPLETION_TIMEOUT_MS
        ),
        idleTimeoutMs: Math.max(
          RESPONSE_ARTIFACT_POLL_MS,
          Number(message.response_artifact_idle_timeout_ms) || RESPONSE_ARTIFACT_IDLE_TIMEOUT_MS
        )
      };
      if (!["text", "json_file"].includes(responseConfig.mode)) {
        throw new Error("response_mode must be text or json_file.");
      }
      if (responseConfig.mode === "json_file" && !responseConfig.filename.toLowerCase().endsWith(".json")) {
        throw new Error("response_filename must be an exact .json filename for json_file mode.");
      }
      requestEvent("request_created", requestId, {
        prompt_chars: String(message.prompt || "").length,
        prompt_sha256: submissionConfig.promptSha256,
        submission_mode_requested: submissionConfig.mode,
        prompt_filename: submissionConfig.filename,
        text_submission_threshold_chars: submissionConfig.thresholdChars,
        default_absolute_timeout_ms: effectiveTimeout.defaultAbsoluteMs,
        requested_absolute_timeout_ms: effectiveTimeout.requestedAbsoluteMs,
        effective_absolute_timeout_ms: effectiveTimeout.absoluteMs,
        response_mode: responseConfig.mode,
        response_filename: responseConfig.filename,
        response_max_bytes: responseConfig.maxBytes,
        response_artifact_start_timeout_ms: responseConfig.startTimeoutMs,
        response_artifact_completion_timeout_ms: responseConfig.completionTimeoutMs,
        response_artifact_idle_timeout_ms: responseConfig.idleTimeoutMs
      });

      try {
        safeSend(tabState("tab_update"));
        const execution = await executePrompt(
          String(message.prompt || ""),
          message.timeout_config || {},
          activeRequest,
          submissionConfig,
          responseConfig
        );
        const text = execution.text;
        requestEvent("response_complete", requestId, {
          response_chars: text.length,
          submission_mode: execution.submission.mode,
          response_mode: responseConfig.mode,
          response_artifact_filename: execution.responseArtifact ? execution.responseArtifact.filename : "",
          response_artifact_bytes: execution.responseArtifact ? execution.responseArtifact.size_bytes : 0
        });
        safeSend({
          type: "prompt_result",
          request_id: requestId,
          bridge_tab_id: getBridgeTabId(),
          conversation_id: getConversationId(),
          url: location.href,
          title: document.title,
          text,
          submission_mode: execution.submission.mode,
          prompt_filename: execution.submission.filename,
          prompt_sha256: execution.submission.promptSha256,
          response_artifact: execution.responseArtifact || null
        });
        resetActiveRequest("success");
      } catch (error) {
        const messageText = error instanceof Error ? error.message : String(error);
        const lower = messageText.toLowerCase();
        requestEvent(
          lower.includes("timed out") ? "timeout" : "request_error",
          requestId,
          { error: messageText }
        );
        safeSend({
          type: "prompt_error",
          request_id: requestId,
          error: messageText
        });
        resetActiveRequest("error");
      }
    });

    socket.addEventListener("close", () => {
      socket = null;
      if (activeRequest) {
        try {
          activeRequest.controller.abort(new Error("Bridge WebSocket disconnected."));
        } catch {
          // The request cleanup path will clear state.
        }
      }
      scheduleReconnect();
    });

    socket.addEventListener("error", () => {
      console.error(`ChatGPT Tab Bridge could not connect to ${BRIDGE_URL}.`);
      try {
        socket.close();
      } catch {
        socket = null;
        scheduleReconnect();
      }
    });
  }

  function getAssistantMessages() {
    return Array.from(document.querySelectorAll('[data-message-author-role="assistant"]'));
  }

  function getUserMessages() {
    return Array.from(document.querySelectorAll('[data-message-author-role="user"]'));
  }

  function normalizeMessageText(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }

  function getMessageKey(messageNode) {
    if (!(messageNode instanceof HTMLElement)) {
      return "";
    }

    const directMessageId = messageNode.getAttribute("data-message-id");
    if (directMessageId) {
      return `message:${directMessageId}`;
    }

    const messageContainer = messageNode.closest("[data-message-id]");
    if (messageContainer instanceof HTMLElement) {
      const messageId = messageContainer.getAttribute("data-message-id");
      if (messageId) {
        return `message:${messageId}`;
      }
    }

    const turnContainer = messageNode.closest(
      '[data-testid^="conversation-turn-"], article[data-testid], section[data-turn]'
    );
    if (turnContainer instanceof HTMLElement) {
      const testId = turnContainer.getAttribute("data-testid");
      const turn = turnContainer.getAttribute("data-turn");
      if (testId) {
        return `turn:${testId}`;
      }
      if (turn) {
        const ordinal = Array.from(document.querySelectorAll("section[data-turn]")).indexOf(turnContainer);
        return `section:${turn}:${ordinal}`;
      }
    }

    const directTestId = messageNode.getAttribute("data-testid");
    return directTestId ? `testid:${directTestId}` : "";
  }

  function snapshotMessages(nodes) {
    return nodes.map((node, index) => ({
      node,
      index,
      key: getMessageKey(node),
      text: normalizeMessageText(node.innerText)
    }));
  }

  function textLooksLikePrompt(userText, promptText) {
    const normalizedUser = normalizeMessageText(userText);
    const normalizedPrompt = normalizeMessageText(promptText);
    if (!normalizedUser || !normalizedPrompt) {
      return false;
    }
    if (normalizedUser === normalizedPrompt) {
      return true;
    }
    const prefixLength = Math.min(240, normalizedPrompt.length);
    const prefix = normalizedPrompt.slice(0, prefixLength);
    if (prefix.length >= 24 && normalizedUser.includes(prefix)) {
      return true;
    }
    return /pasted text|attached file|attachment/i.test(normalizedUser) && normalizedPrompt.length > 1200;
  }

  function nodeFollows(referenceNode, candidateNode) {
    if (!(referenceNode instanceof Node) || !(candidateNode instanceof Node)) {
      return false;
    }
    return Boolean(
      referenceNode.compareDocumentPosition(candidateNode) &
        Node.DOCUMENT_POSITION_FOLLOWING
    );
  }

  function textsAreCompatible(previousText, candidateText) {
    const previous = normalizeMessageText(previousText);
    const candidate = normalizeMessageText(candidateText);
    if (!previous || !candidate) {
      return true;
    }
    if (
      previous === candidate ||
      previous.startsWith(candidate) ||
      candidate.startsWith(previous)
    ) {
      return true;
    }
    const sharedPrefixLength = Math.min(160, previous.length, candidate.length);
    return (
      sharedPrefixLength >= 24 &&
      previous.slice(0, sharedPrefixLength) === candidate.slice(0, sharedPrefixLength)
    );
  }

  function boundedTimeout(value, fallback, minimum, maximum) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return fallback;
    }
    return Math.min(maximum, Math.max(minimum, Math.round(numeric)));
  }

  function timeoutConfig(raw) {
    const source = raw && typeof raw === "object" ? raw : {};
    const hasRequestedTimeout = Object.prototype.hasOwnProperty.call(
      source,
      "requested_absolute_timeout_ms"
    );
    const requestedRaw = hasRequestedTimeout
      ? source.requested_absolute_timeout_ms
      : source.absolute_timeout_ms;
    const effectiveRaw =
      source.effective_absolute_timeout_ms ?? source.absolute_timeout_ms;
    const absoluteMs = boundedTimeout(
      effectiveRaw,
      DEFAULT_ABSOLUTE_TIMEOUT_MS,
      30000,
      MAX_ABSOLUTE_TIMEOUT_MS
    );
    const requestedNumeric = Number(requestedRaw);
    const requestedAbsoluteMs =
      requestedRaw === null ||
      requestedRaw === undefined ||
      !Number.isFinite(requestedNumeric)
        ? null
        : Math.round(requestedNumeric);
    return {
      defaultAbsoluteMs: boundedTimeout(
        source.default_absolute_timeout_ms,
        DEFAULT_ABSOLUTE_TIMEOUT_MS,
        30000,
        MAX_ABSOLUTE_TIMEOUT_MS
      ),
      requestedAbsoluteMs,
      initialResponseMs: boundedTimeout(
        source.initial_response_timeout_ms,
        DEFAULT_INITIAL_RESPONSE_TIMEOUT_MS,
        15000,
        absoluteMs
      ),
      streamIdleMs:
        Number(source.stream_idle_timeout_ms) === 0
          ? 0
          : boundedTimeout(
              source.stream_idle_timeout_ms,
              DEFAULT_STREAM_IDLE_TIMEOUT_MS,
              10000,
              absoluteMs
            ),
      absoluteMs
    };
  }

  function createResponseTracker(prompt, requestId, rawTimeoutConfig) {
    const assistantBefore = snapshotMessages(getAssistantMessages());
    const userBefore = snapshotMessages(getUserMessages());
    const now = Date.now();
    return {
      requestId,
      prompt,
      submittedUrl: location.href,
      submittedConversationId: getConversationId(),
      submittedAt: now,
      assistantBaselineCount: assistantBefore.length,
      assistantBaselineKeys: new Set(
        assistantBefore.map((item) => item.key).filter(Boolean)
      ),
      userBaselineCount: userBefore.length,
      userBaselineKeys: new Set(userBefore.map((item) => item.key).filter(Boolean)),
      userKey: "",
      userNode: null,
      userIndex: -1,
      messageKey: "",
      messageNode: null,
      messageIndex: -1,
      lastText: "",
      lastTextChangedAt: now,
      lastMutationAt: now,
      lastActivityAt: now,
      lastStreamLogAt: 0,
      lastStreamLogChars: 0,
      userFoundLogged: false,
      assistantFoundLogged: false,
      responseStarted: false,
      timeout: timeoutConfig(rawTimeoutConfig),
      observer: null
    };
  }

  function routeChanged(tracker) {
    return (
      location.href !== tracker.submittedUrl ||
      getConversationId() !== tracker.submittedConversationId
    );
  }

  function locateSubmittedUserMessage(tracker) {
    const current = snapshotMessages(getUserMessages());

    if (tracker.userKey) {
      const keyed = current.find((item) => item.key === tracker.userKey);
      if (keyed) {
        tracker.userNode = keyed.node;
        tracker.userIndex = keyed.index;
        return keyed;
      }
    }

    if (tracker.userNode && tracker.userNode.isConnected) {
      const connected = current.find((item) => item.node === tracker.userNode);
      if (connected) {
        tracker.userKey = connected.key || tracker.userKey;
        tracker.userIndex = connected.index;
        return connected;
      }
    }

    const newItems = current.filter(
      (item) => !item.key || !tracker.userBaselineKeys.has(item.key)
    );
    const promptMatches = newItems.filter((item) =>
      textLooksLikePrompt(item.text, tracker.prompt)
    );
    let candidate = promptMatches.at(-1) || null;

    if (!candidate && newItems.length) {
      candidate = newItems.at(-1) || null;
    }
    if (!candidate && current.length > tracker.userBaselineCount) {
      candidate = current.at(-1) || null;
    }
    if (!candidate && routeChanged(tracker) && current.length) {
      candidate = current.at(-1) || null;
    }

    if (candidate) {
      tracker.userKey = candidate.key || tracker.userKey;
      tracker.userNode = candidate.node;
      tracker.userIndex = candidate.index;
      if (!tracker.userFoundLogged) {
        tracker.userFoundLogged = true;
        requestEvent("user_turn_found", tracker.requestId, {
          user_key: tracker.userKey,
          user_index: tracker.userIndex
        });
      }
    }
    return candidate;
  }

  function locateNewAssistantMessage(tracker) {
    const current = snapshotMessages(getAssistantMessages());
    const userMessage = locateSubmittedUserMessage(tracker);

    if (tracker.messageKey) {
      const keyed = current.find((item) => item.key === tracker.messageKey);
      if (keyed && (!userMessage || nodeFollows(userMessage.node, keyed.node))) {
        const reacquired = tracker.messageNode && tracker.messageNode !== keyed.node;
        tracker.messageNode = keyed.node;
        tracker.messageIndex = keyed.index;
        if (reacquired) {
          requestEvent("response_reacquired", tracker.requestId, {
            message_key: keyed.key,
            response_chars: keyed.text.length
          });
        }
        return keyed;
      }
    }

    if (tracker.messageNode && tracker.messageNode.isConnected) {
      const connected = current.find((item) => item.node === tracker.messageNode);
      if (connected && (!userMessage || nodeFollows(userMessage.node, connected.node))) {
        tracker.messageKey = connected.key || tracker.messageKey;
        tracker.messageIndex = connected.index;
        return connected;
      }
    }

    let candidates = current.filter(
      (item) => !item.key || !tracker.assistantBaselineKeys.has(item.key)
    );

    if (userMessage) {
      const afterUser = current.filter((item) => nodeFollows(userMessage.node, item.node));
      const afterUserNew = afterUser.filter(
        (item) => !item.key || !tracker.assistantBaselineKeys.has(item.key)
      );
      candidates = afterUserNew.length ? afterUserNew : afterUser;
    }

    if (!candidates.length && current.length > tracker.assistantBaselineCount) {
      candidates = current.slice(tracker.assistantBaselineCount);
      if (userMessage) {
        candidates = candidates.filter((item) => nodeFollows(userMessage.node, item.node));
      }
    }

    if (!candidates.length && tracker.lastText) {
      candidates = current.filter((item) => textsAreCompatible(tracker.lastText, item.text));
      if (userMessage) {
        candidates = candidates.filter((item) => nodeFollows(userMessage.node, item.node));
      }
    }

    if (!candidates.length && routeChanged(tracker) && userMessage && current.length) {
      const newest = current.at(-1);
      if (newest && nodeFollows(userMessage.node, newest.node)) {
        candidates = [newest];
      }
    }

    const candidate = candidates.at(-1) || null;
    if (!candidate) {
      return null;
    }

    const hadTrackedNode = Boolean(tracker.messageNode || tracker.messageKey);
    tracker.messageKey = candidate.key || tracker.messageKey;
    tracker.messageNode = candidate.node;
    tracker.messageIndex = candidate.index;
    if (!tracker.assistantFoundLogged) {
      tracker.assistantFoundLogged = true;
      requestEvent("assistant_turn_found", tracker.requestId, {
        message_key: tracker.messageKey,
        message_index: tracker.messageIndex
      });
    } else if (hadTrackedNode) {
      requestEvent("response_reacquired", tracker.requestId, {
        message_key: tracker.messageKey,
        response_chars: candidate.text.length
      });
    }
    return candidate;
  }


  function normalizedArtifactFilename(value) {
    const raw = String(value || "").trim();
    if (!raw) {
      return "";
    }
    try {
      return decodeURIComponent(raw.split(/[?#]/, 1)[0].split("/").pop() || raw).trim();
    } catch {
      return (raw.split(/[?#]/, 1)[0].split("/").pop() || raw).trim();
    }
  }

  function assistantTurnRoot(tracker) {
    const node = tracker && tracker.messageNode;
    if (!(node instanceof HTMLElement)) {
      return null;
    }
    return (
      node.closest('[data-testid^="conversation-turn-"], article[data-testid], section[data-turn]') ||
      node
    );
  }

  function artifactLookupToken(expectedFilename) {
    const expected = normalizedArtifactFilename(expectedFilename);
    const stem = expected.replace(/\.json$/i, "");
    const timestampSuffix = stem.match(/(\d{8}_\d{6}(?:_[A-Za-z0-9]+)*)$/);
    return timestampSuffix ? timestampSuffix[1] : stem;
  }

  function responseArtifactCandidates(tracker, expectedFilename) {
    const root = assistantTurnRoot(tracker);
    if (!(root instanceof HTMLElement)) {
      return [];
    }
    const expected = normalizedArtifactFilename(expectedFilename);
    const lookupToken = artifactLookupToken(expected);
    const matches = [];
    const clickableSelector = 'a[href], button, [role="button"]';
    for (const element of root.querySelectorAll(clickableSelector)) {
      if (!(element instanceof HTMLElement)) {
        continue;
      }
      const href =
        element instanceof HTMLAnchorElement
          ? String(element.href || element.getAttribute("href") || "").trim()
          : "";
      const labels = [
        element.getAttribute("download"),
        element.getAttribute("aria-label"),
        element.getAttribute("title"),
        element.innerText,
        element.textContent,
        href,
        normalizedArtifactFilename(href)
      ]
        .map((value) => String(value || "").trim())
        .filter(Boolean);
      const searchable = labels.join(" ");
      const exactFilenameMatch = labels
        .map(normalizedArtifactFilename)
        .some((value) => value === expected);
      const responseIdMatch = Boolean(lookupToken && searchable.includes(lookupToken));
      if (!exactFilenameMatch && !responseIdMatch) {
        continue;
      }
      matches.push({
        element,
        href,
        filename: expected,
        labels,
        kind: element instanceof HTMLAnchorElement ? "link" : "button",
        exactFilenameMatch
      });
    }

    const exactMatches = matches.filter((candidate) => candidate.exactFilenameMatch);
    return exactMatches.length ? exactMatches : matches;
  }

  function matchingArtifactPreview(expectedFilename) {
    const expected = normalizedArtifactFilename(expectedFilename);
    const lookupToken = artifactLookupToken(expected);
    const selectors = [
      'section[data-testid="screen-threadFlyOut"]',
      '[data-testid="screen-threadFlyOut"]'
    ];
    const matches = [];
    for (const element of document.querySelectorAll(selectors.join(','))) {
      if (!(element instanceof HTMLElement)) {
        continue;
      }
      const searchable = [
        element.getAttribute("aria-label"),
        element.getAttribute("title"),
        element.textContent
      ]
        .map((value) => String(value || ""))
        .join(" ");
      if (searchable.includes(expected) || (lookupToken && searchable.includes(lookupToken))) {
        matches.push(element);
      }
    }
    if (matches.length > 1) {
      throw new Error(
        `Found ${matches.length} artifact preview panels containing response ID ${lookupToken}; exactly one is required.`
      );
    }
    return matches[0] || null;
  }

  function previewDownloadButton(preview) {
    if (!(preview instanceof HTMLElement)) {
      return null;
    }
    const candidates = preview.querySelectorAll(
      'button[aria-label="Download"], [role="button"][aria-label="Download"], button[title="Download"]'
    );
    for (const element of candidates) {
      if (!(element instanceof HTMLElement)) {
        continue;
      }
      const disabled = element instanceof HTMLButtonElement && element.disabled;
      const visible = element.offsetParent !== null || element.getClientRects().length > 0;
      if (!disabled && visible) {
        return element;
      }
    }
    return null;
  }

  function closeArtifactPreview(preview) {
    if (!(preview instanceof HTMLElement) || !preview.isConnected) {
      return;
    }
    const selectors = [
      'button[aria-label="Close"]',
      'button[aria-label*="Close preview"]',
      '[role="button"][aria-label="Close"]'
    ];
    const button = preview.querySelector(selectors.join(','));
    if (button instanceof HTMLElement) {
      try {
        button.click();
      } catch {
        // Preview cleanup is best-effort and never changes artifact validity.
      }
    }
  }

  async function prepareResponseDownload(config) {
    const response = await chrome.runtime.sendMessage({
      type: "prepare_response_download",
      filename: config.filename,
      max_bytes: config.maxBytes
    });
    if (!response || response.ok !== true || !response.session_token) {
      throw new Error(
        response && response.error
          ? response.error
          : "Could not prepare exact response download tracking."
      );
    }
    return {
      sessionToken: String(response.session_token),
      diagnostics: response.diagnostics || null,
      background: {
        protocol_version: Number(response.protocol_version || 0),
        extension_version: String(response.extension_version || "")
      }
    };
  }

  async function pollResponseDownload(sessionToken) {
    const response = await chrome.runtime.sendMessage({
      type: "poll_response_download",
      session_token: sessionToken
    });
    if (!response || response.ok !== true) {
      throw new Error(
        response && response.error
          ? response.error
          : "Could not read the tracked response download state."
      );
    }
    return response;
  }

  async function cleanupResponseDownload(sessionToken, downloadId = null) {
    const response = await chrome.runtime.sendMessage({
      type: "cleanup_response_download",
      session_token: String(sessionToken || ""),
      download_id: Number.isInteger(downloadId) ? downloadId : null
    });
    if (!response || response.ok !== true) {
      throw new Error(
        response && response.error
          ? response.error
          : "Could not clean up the response download."
      );
    }
    return response;
  }

  async function clickAndTrackDownloadedArtifact(candidate, config, signal) {
    if (!(candidate.element instanceof HTMLElement) || !candidate.element.isConnected) {
      throw new Error("The response artifact control is no longer available.");
    }

    const preparedDownload = await prepareResponseDownload(config);
    const sessionToken = preparedDownload.sessionToken;
    let downloadId = null;
    let preview = null;
    let previewDownloadClicked = false;
    let completed = false;
    let lastBytes = -1;
    let lastProgressAt = Date.now();
    let lastProgressLogAt = 0;
    let previewLogged = false;
    let lastWaitingDiagnostic = "";
    let lastWaitingDiagnosticAt = 0;

    requestEvent("response_artifact_capture_diagnostic_started", activeRequest ? activeRequest.requestId : "", {
      filename: candidate.filename,
      kind: candidate.kind,
      href: candidate.href,
      candidate: diagnosticElementSnapshot(candidate.element),
      tracking_session: preparedDownload.diagnostics,
      background_runtime: backgroundRuntimeInfo || preparedDownload.background
    });

    try {
      throwIfAborted(signal);
      const artifactControlClick = diagnosticClick(candidate.element);
      requestEvent("response_artifact_control_click_diagnostic", activeRequest ? activeRequest.requestId : "", {
        filename: candidate.filename,
        kind: candidate.kind,
        click: artifactControlClick
      });
      if (artifactControlClick.error) {
        throw new Error(`Artifact control click failed: ${artifactControlClick.error}`);
      }
      requestEvent("response_artifact_control_clicked", activeRequest ? activeRequest.requestId : "", {
        filename: candidate.filename,
        kind: candidate.kind
      });

      const startTimeoutMs = Math.max(
        RESPONSE_ARTIFACT_POLL_MS,
        Number(config.startTimeoutMs) || RESPONSE_ARTIFACT_POST_RESPONSE_WAIT_MS
      );
      const startDeadline = Date.now() + startTimeoutMs;
      let state = null;

      while (Date.now() < startDeadline) {
        throwIfAborted(signal);
        state = await pollResponseDownload(sessionToken);
        if (state.state !== "waiting_start") {
          downloadId = Number(state.download_id);
          requestEvent("response_artifact_download_record_detected", activeRequest ? activeRequest.requestId : "", {
            filename: candidate.filename,
            state: String(state.state || ""),
            download_id: downloadId,
            diagnostics: state.diagnostics || null
          });
          break;
        }

        const downloadDiagnostics = state.diagnostics || {};
        const chromeInternal = downloadDiagnostics.chrome_internal || {};
        const chromeEvents = chromeInternal.events || {};
        const diagnosticJson = JSON.stringify({
          new_download_count: downloadDiagnostics.new_download_count || 0,
          matching_download_count: downloadDiagnostics.matching_download_count || 0,
          tracked_download_id: downloadDiagnostics.tracked_download_id || null,
          new_downloads: downloadDiagnostics.new_downloads || [],
          automatic_downloads: chromeInternal.automatic_downloads || null,
          chrome_internal_last_sequence: chromeEvents.last_sequence || 0,
          chrome_internal_relevant_count: chromeEvents.relevant_since_session || 0
        });
        const diagnosticChanged = diagnosticJson !== lastWaitingDiagnostic;
        const diagnosticAgeMs = Date.now() - lastWaitingDiagnosticAt;
        if (
          (diagnosticChanged && diagnosticAgeMs >= 1500) ||
          diagnosticAgeMs >= 5000
        ) {
          lastWaitingDiagnostic = diagnosticJson;
          lastWaitingDiagnosticAt = Date.now();
          requestEvent("response_artifact_download_start_probe", activeRequest ? activeRequest.requestId : "", {
            filename: candidate.filename,
            preview_download_clicked: previewDownloadClicked,
            diagnostics: state.diagnostics || null
          });
        }

        preview = matchingArtifactPreview(candidate.filename);
        if (preview && !previewLogged) {
          previewLogged = true;
          requestEvent("response_artifact_preview_diagnostic", activeRequest ? activeRequest.requestId : "", {
            filename: candidate.filename,
            preview: diagnosticPreviewSnapshot(preview)
          });
        }
        if (preview && !previewDownloadClicked) {
          const downloadButton = previewDownloadButton(preview);
          if (downloadButton) {
            const beforePreviewClick = {
              button: diagnosticElementSnapshot(downloadButton),
              preview: diagnosticPreviewSnapshot(preview),
              page_feedback: diagnosticPageFeedback(document)
            };
            const downloadClick = diagnosticClick(downloadButton);
            previewDownloadClicked = true;
            requestEvent("response_artifact_preview_download_click_diagnostic", activeRequest ? activeRequest.requestId : "", {
              filename: candidate.filename,
              before: beforePreviewClick,
              click: downloadClick
            });
            if (downloadClick.error) {
              throw new Error(`Preview Download click failed: ${downloadClick.error}`);
            }
            requestEvent("response_artifact_preview_download_clicked", activeRequest ? activeRequest.requestId : "", {
              filename: candidate.filename
            });
            setTimeout(() => {
              requestEvent("response_artifact_preview_post_click_diagnostic", activeRequest ? activeRequest.requestId : "", {
                filename: candidate.filename,
                preview: diagnosticPreviewSnapshot(preview),
                button: diagnosticElementSnapshot(downloadButton),
                page_feedback: diagnosticPageFeedback(document)
              });
            }, 500);
          }
        }
        await sleep(RESPONSE_ARTIFACT_POLL_MS, signal);
      }

      if (!state || state.state === "waiting_start") {
        requestEvent("response_artifact_download_start_timeout_diagnostic", activeRequest ? activeRequest.requestId : "", {
          filename: candidate.filename,
          preview_download_clicked: previewDownloadClicked,
          preview: diagnosticPreviewSnapshot(preview || matchingArtifactPreview(candidate.filename)),
          final_download_probe: state && state.diagnostics ? state.diagnostics : null,
          page_feedback: diagnosticPageFeedback(document)
        });
        throw new Error(
          `Timed out after ${Math.round(startTimeoutMs / 1000)} seconds waiting for the response artifact download to start.`
        );
      }

      const completionTimeoutMs = Math.max(
        RESPONSE_ARTIFACT_POLL_MS,
        Number(config.completionTimeoutMs) || 30 * 60 * 1000
      );
      const idleTimeoutMs = Math.max(
        RESPONSE_ARTIFACT_POLL_MS,
        Number(config.idleTimeoutMs) || 5 * 60 * 1000
      );
      const completionDeadline = Date.now() + completionTimeoutMs;

      while (Date.now() < completionDeadline) {
        throwIfAborted(signal);
        if (state.state === "complete") {
          completed = true;
          const completedDownloadId = Number(state.download_id);
          setTimeout(() => {
            cleanupResponseDownload(sessionToken, completedDownloadId).catch(() => {
              // Normal Tool acknowledgement performs immediate cleanup; this is crash protection.
            });
          }, RESPONSE_ARTIFACT_FALLBACK_CLEANUP_MS);
          return {
            filename: candidate.filename,
            actual_filename: String(state.actual_filename || ""),
            mime_type: String(state.mime_type || "application/json"),
            size_bytes: Number(state.size_bytes || state.bytes_received || 0),
            sha256: "",
            content_base64: "",
            local_path: String(state.local_path || ""),
            source_url: candidate.href || "",
            download_id: completedDownloadId,
            cleanup_token: sessionToken
          };
        }

        const bytesReceived = Number(state.bytes_received || 0);
        if (bytesReceived !== lastBytes) {
          lastBytes = bytesReceived;
          lastProgressAt = Date.now();
        }
        if (Date.now() - lastProgressAt > idleTimeoutMs) {
          throw new Error(
            `Response artifact download made no progress for ${Math.round(idleTimeoutMs / 1000)} seconds.`
          );
        }
        if (Date.now() - lastProgressLogAt >= 5000) {
          lastProgressLogAt = Date.now();
          requestEvent("response_artifact_download_progress", activeRequest ? activeRequest.requestId : "", {
            filename: candidate.filename,
            download_id: Number(state.download_id),
            bytes_received: bytesReceived,
            total_bytes: Number(state.total_bytes || 0),
            state: String(state.state || "in_progress")
          });
        }
        await sleep(RESPONSE_ARTIFACT_POLL_MS, signal);
        state = await pollResponseDownload(sessionToken);
        downloadId = Number(state.download_id || downloadId);
      }

      throw new Error(
        `Timed out after ${Math.round(completionTimeoutMs / 1000)} seconds waiting for the response artifact download to complete.`
      );
    } finally {
      try {
        closeArtifactPreview(preview || matchingArtifactPreview(candidate.filename));
      } catch {
        // Preview cleanup must not hide the original download result or error.
      }
      if (!completed) {
        try {
          await cleanupResponseDownload(sessionToken, Number.isInteger(downloadId) ? downloadId : null);
        } catch {
          // The Tool also performs local-path fallback cleanup after processing errors.
        }
      }
    }
  }

  async function fetchResponseArtifact(candidate, config, signal) {
    return await clickAndTrackDownloadedArtifact(candidate, config, signal);
  }

  async function waitForResponseArtifact(tracker, config, signal) {
    const discoveryTimeoutMs = Math.max(
      RESPONSE_ARTIFACT_POLL_MS,
      Number(config.startTimeoutMs) || RESPONSE_ARTIFACT_POST_RESPONSE_WAIT_MS
    );
    const deadline = Date.now() + discoveryTimeoutMs;
    let lastFetchError = "";
    while (Date.now() < deadline) {
      throwIfAborted(signal);
      locateNewAssistantMessage(tracker);
      const candidates = responseArtifactCandidates(tracker, config.filename);
      if (candidates.length > 1) {
        lastFetchError = `Found ${candidates.length} clickable artifact controls containing the expected response ID; exactly one is required.`;
      } else if (candidates.length === 1) {
        const candidate = candidates[0];
        requestEvent("response_artifact_found", tracker.requestId, {
          filename: candidate.filename,
          href: candidate.href,
          kind: candidate.kind
        });
        for (let captureAttempt = 1; captureAttempt <= 2; captureAttempt += 1) {
          try {
            const artifact = await fetchResponseArtifact(candidate, config, signal);
            requestEvent("response_artifact_ready", tracker.requestId, {
              filename: artifact.filename,
              actual_filename: artifact.actual_filename,
              download_id: artifact.download_id,
              size_bytes: artifact.size_bytes,
              mime_type: artifact.mime_type,
              capture_attempt: captureAttempt
            });
            return artifact;
          } catch (error) {
            lastFetchError = error instanceof Error ? error.message : String(error);
            requestEvent("response_artifact_capture_failed", tracker.requestId, {
              filename: candidate.filename,
              capture_attempt: captureAttempt,
              error: lastFetchError
            });
            if (captureAttempt < 2) {
              await sleep(1000, signal);
            }
          }
        }
        break;
      }
      await sleep(RESPONSE_ARTIFACT_POLL_MS, signal);
    }
    const detail = lastFetchError ? ` Last artifact error: ${lastFetchError}` : "";
    throw new Error(`Timed out after ${Math.round(discoveryTimeoutMs / 1000)} seconds waiting for response artifact ${config.filename}.${detail}`);
  }

  function findComposer() {
    const candidates = [
      document.querySelector("#prompt-textarea"),
      document.querySelector('textarea[data-testid="prompt-textarea"]'),
      document.querySelector('[data-testid="composer-input"]'),
      document.querySelector('textarea[placeholder*="Message"]'),
      document.querySelector('div[contenteditable="true"][data-virtualkeyboard]'),
      document.querySelector('form div[contenteditable="true"][role="textbox"]')
    ];
    return candidates.find((element) => element instanceof HTMLElement) || null;
  }

  function setComposerValue(composer, prompt) {
    composer.focus();

    if (composer instanceof HTMLTextAreaElement) {
      const descriptor = Object.getOwnPropertyDescriptor(
        HTMLTextAreaElement.prototype,
        "value"
      );
      if (descriptor && descriptor.set) {
        descriptor.set.call(composer, prompt);
      } else {
        composer.value = prompt;
      }
      composer.dispatchEvent(new Event("input", { bubbles: true }));
      composer.dispatchEvent(new Event("change", { bubbles: true }));
      return;
    }

    composer.textContent = prompt;
    composer.dispatchEvent(
      new InputEvent("input", {
        bubbles: true,
        inputType: "insertText",
        data: prompt
      })
    );
  }


  function getComposerValue(composer) {
    if (composer instanceof HTMLTextAreaElement) {
      return String(composer.value || "");
    }
    return String(composer.innerText || composer.textContent || "");
  }

  function composerContainsCompletePrompt(composer, prompt) {
    const expected = String(prompt || "").replace(/\r\n/g, "\n").trim();
    const actual = getComposerValue(composer).replace(/\r\n/g, "\n").trim();
    return actual === expected;
  }

  function clearComposerValue(composer) {
    setComposerValue(composer, "");
  }

  function sanitizePromptFilename(value) {
    let filename = String(value || "complete_prompt.txt").trim() || "complete_prompt.txt";
    filename = filename.replace(/[\\/:*?"<>|]+/g, "_");
    if (!filename.toLowerCase().endsWith(".txt")) {
      filename += ".txt";
    }
    return filename;
  }

  function findFileInput() {
    const inputs = Array.from(document.querySelectorAll('input[type="file"]'));
    return (
      inputs.find(
        (input) =>
          input instanceof HTMLInputElement &&
          !input.disabled &&
          (String(input.accept || "").includes("text") || input.multiple)
      ) ||
      inputs.find(
        (input) => input instanceof HTMLInputElement && !input.disabled
      ) ||
      null
    );
  }

  function findAttachmentButton() {
    const selectors = [
      'button[aria-label*="Attach files"]',
      'button[aria-label*="Attach"]',
      'button[aria-label*="Upload file"]',
      'button[aria-label*="Upload"]',
      'button[aria-label*="Add files"]',
      'button[aria-label*="Add photos"]',
      'button[data-testid="composer-plus-btn"]',
      'button[data-testid*="attach"]',
      'button[data-testid*="upload"]'
    ];
    for (const selector of selectors) {
      const button = document.querySelector(selector);
      if (
        button instanceof HTMLButtonElement &&
        !button.disabled &&
        button.offsetParent !== null
      ) {
        return button;
      }
    }
    return null;
  }

  function attachmentIsVisible(filename) {
    const target = String(filename || "").trim();
    if (!target) {
      return false;
    }
    const selectors = [
      '[data-testid*="attachment"]',
      '[data-testid*="file"]',
      '[aria-label*="attachment"]',
      '[aria-label*="file"]',
      'button[aria-label*="Remove"]'
    ];
    for (const selector of selectors) {
      for (const node of document.querySelectorAll(selector)) {
        const text = String(node.innerText || node.textContent || node.getAttribute("aria-label") || "");
        if (text.includes(target)) {
          return true;
        }
      }
    }
    return String(document.body && document.body.innerText || "").includes(target);
  }


  function removePromptAttachment(filename) {
    const target = String(filename || "").trim();
    if (!target) {
      return false;
    }
    const removeButtons = Array.from(
      document.querySelectorAll('button[aria-label*="Remove"], button[data-testid*="remove"]')
    );
    const button = removeButtons.find((candidate) => {
      const container = candidate.closest(
        '[data-testid*="attachment"], [data-testid*="file"], li, div'
      );
      const text = String(
        (container && (container.innerText || container.textContent)) ||
          candidate.getAttribute("aria-label") ||
          ""
      );
      return text.includes(target);
    });
    if (button instanceof HTMLButtonElement && !button.disabled) {
      button.click();
      return true;
    }
    return false;
  }

  function findUploadMenuItem() {
    const candidates = Array.from(
      document.querySelectorAll('[role="menuitem"], [role="option"], button')
    );
    return (
      candidates.find((element) => {
        if (!(element instanceof HTMLElement) || element.offsetParent === null) {
          return false;
        }
        const label = String(
          element.innerText ||
            element.textContent ||
            element.getAttribute("aria-label") ||
            ""
        ).trim();
        return /upload from computer|add photos? (?:&|and) files|upload files?|from computer/i.test(label);
      }) || null
    );
  }

  async function ensureFileInput(signal) {
    let input = findFileInput();
    if (input) {
      return input;
    }
    const attachmentButton = findAttachmentButton();
    if (attachmentButton) {
      attachmentButton.click();
      await sleep(300, signal);
    }
    input = findFileInput();
    if (input) {
      return input;
    }
    const uploadMenuItem = findUploadMenuItem();
    if (uploadMenuItem) {
      uploadMenuItem.click();
      await sleep(300, signal);
    }
    return await waitFor(
      findFileInput,
      30000,
      "the ChatGPT file attachment input",
      signal
    );
  }

  async function attachPromptTextFile(prompt, filename, request) {
    const safeFilename = sanitizePromptFilename(filename);
    const input = await ensureFileInput(request.controller.signal);
    const file = new File([String(prompt || "")], safeFilename, {
      type: "text/plain;charset=utf-8",
      lastModified: Date.now()
    });
    const transfer = new DataTransfer();
    transfer.items.add(file);
    input.files = transfer.files;
    requestEvent("prompt_attachment_started", request.requestId, {
      prompt_filename: safeFilename,
      prompt_chars: String(prompt || "").length,
      prompt_utf8_bytes: file.size
    });
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));

    await waitFor(
      () => attachmentIsVisible(safeFilename),
      ATTACHMENT_UPLOAD_TIMEOUT_MS,
      `the prompt attachment ${safeFilename}`,
      request.controller.signal
    );
    requestEvent("prompt_attachment_ready", request.requestId, {
      prompt_filename: safeFilename,
      prompt_utf8_bytes: file.size
    });
    return safeFilename;
  }

  function attachmentInstruction(filename, promptChars, promptSha256) {
    const hashLine = promptSha256 ? ` Its SHA-256 is ${promptSha256}.` : "";
    return (
      `Read the attached UTF-8 text file ${filename} as the complete prompt.` +
      ` It contains ${promptChars} characters.${hashLine}` +
      " Follow every instruction in that file exactly, inspect all supplied evidence, " +
      "and return the requested response directly in this chat."
    );
  }

  async function prepareTextSubmission(prompt, request) {
    const composer = await waitFor(
      findComposer,
      30000,
      "the ChatGPT prompt composer",
      request.controller.signal
    );
    setComposerValue(composer, prompt);
    await sleep(100, request.controller.signal);
    if (!composerContainsCompletePrompt(composer, prompt)) {
      throw new Error("ChatGPT composer did not retain the complete prompt text.");
    }
    return {
      mode: "text",
      filename: "",
      composer,
      trackingPrompt: prompt
    };
  }

  async function prepareFileSubmission(prompt, config, request) {
    let filename = "";
    let lastError = null;
    for (let attempt = 1; attempt <= ATTACHMENT_UPLOAD_MAX_ATTEMPTS; attempt += 1) {
      try {
        filename = await attachPromptTextFile(prompt, config.filename, request);
        break;
      } catch (error) {
        lastError = error;
        requestEvent("prompt_attachment_retry", request.requestId, {
          attempt,
          max_attempts: ATTACHMENT_UPLOAD_MAX_ATTEMPTS,
          prompt_filename: config.filename,
          error: error instanceof Error ? error.message : String(error)
        });
        removePromptAttachment(config.filename);
        const input = findFileInput();
        if (input instanceof HTMLInputElement) {
          input.value = "";
        }
        if (attempt < ATTACHMENT_UPLOAD_MAX_ATTEMPTS) {
          await sleep(750, request.controller.signal);
        }
      }
    }
    if (!filename) {
      const detail = lastError instanceof Error ? lastError.message : String(lastError || "unknown error");
      throw new Error(
        `Prompt attachment failed after ${ATTACHMENT_UPLOAD_MAX_ATTEMPTS} attempts: ${detail}`
      );
    }
    const composer = await waitFor(
      findComposer,
      30000,
      "the ChatGPT prompt composer",
      request.controller.signal
    );
    const instruction = attachmentInstruction(
      filename,
      prompt.length,
      config.promptSha256
    );
    setComposerValue(composer, instruction);
    await sleep(100, request.controller.signal);
    if (!composerContainsCompletePrompt(composer, instruction)) {
      throw new Error("ChatGPT composer did not retain the attachment instruction.");
    }
    return {
      mode: "file",
      filename,
      composer,
      trackingPrompt: instruction
    };
  }

  async function preparePromptSubmission(prompt, rawConfig, request) {
    const config = {
      mode: String(rawConfig && rawConfig.mode || "auto").trim().toLowerCase(),
      filename: sanitizePromptFilename(rawConfig && rawConfig.filename),
      thresholdChars: Math.max(
        1,
        Number(rawConfig && rawConfig.thresholdChars) || DEFAULT_TEXT_SUBMISSION_THRESHOLD_CHARS
      ),
      promptSha256: String(rawConfig && rawConfig.promptSha256 || "")
    };
    if (!["auto", "text", "file"].includes(config.mode)) {
      throw new Error("submission_mode must be auto, text, or file.");
    }

    if (config.mode === "text") {
      return { ...(await prepareTextSubmission(prompt, request)), promptSha256: config.promptSha256 };
    }
    if (config.mode === "file") {
      return { ...(await prepareFileSubmission(prompt, config, request)), promptSha256: config.promptSha256 };
    }

    const preferFile = prompt.length > config.thresholdChars;
    if (preferFile) {
      try {
        const prepared = await prepareFileSubmission(prompt, config, request);
        return { ...prepared, promptSha256: config.promptSha256 };
      } catch (error) {
        requestEvent("prompt_transport_fallback", request.requestId, {
          failed_mode: "file",
          next_mode: "none",
          error: error instanceof Error ? error.message : String(error)
        });
        removePromptAttachment(config.filename);
        const composer = findComposer();
        if (composer) {
          clearComposerValue(composer);
        }
        throw error;
      }
    }

    for (const mode of ["text", "file"]) {
      try {
        const prepared =
          mode === "file"
            ? await prepareFileSubmission(prompt, config, request)
            : await prepareTextSubmission(prompt, request);
        return { ...prepared, promptSha256: config.promptSha256 };
      } catch (error) {
        requestEvent("prompt_transport_fallback", request.requestId, {
          failed_mode: mode,
          next_mode: mode === "text" ? "file" : "none",
          error: error instanceof Error ? error.message : String(error)
        });
        if (mode === "file") {
          removePromptAttachment(config.filename);
        }
        const composer = findComposer();
        if (composer) {
          clearComposerValue(composer);
        }
        if (mode === "file") {
          throw error;
        }
      }
    }
    throw new Error("Unable to prepare the prompt for ChatGPT submission.");
  }

  function findSendButton() {
    const selectors = [
      '[data-testid="send-button"]',
      'button[aria-label*="Send"]',
      'button[data-testid*="send"]'
    ];

    for (const selector of selectors) {
      const button = document.querySelector(selector);
      if (
        button instanceof HTMLButtonElement &&
        !button.disabled &&
        button.offsetParent !== null
      ) {
        return button;
      }
    }
    return null;
  }

  function throwIfAborted(signal) {
    if (signal && signal.aborted) {
      const reason = signal.reason;
      throw reason instanceof Error ? reason : new Error(String(reason || "Request cancelled."));
    }
  }

  async function waitFor(condition, timeoutMs, description, signal) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      throwIfAborted(signal);
      const value = condition();
      if (value) {
        return value;
      }
      await sleep(POLL_INTERVAL_MS, signal);
    }
    throw new Error(`Timed out waiting for ${description}.`);
  }

  function observeResponseActivity(tracker) {
    if (!document.body || typeof MutationObserver === "undefined") {
      return;
    }
    tracker.observer = new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        const target = mutation.target instanceof Element ? mutation.target : mutation.target.parentElement;
        const messageRelated =
          (target && target.closest('[data-message-author-role], section[data-turn], [data-testid^="conversation-turn-"]')) ||
          Array.from(mutation.addedNodes || []).some(
            (node) =>
              node instanceof Element &&
              (node.matches('[data-message-author-role], section[data-turn], [data-testid^="conversation-turn-"]') ||
                node.querySelector('[data-message-author-role], section[data-turn], [data-testid^="conversation-turn-"]'))
          );
        if (messageRelated) {
          tracker.lastMutationAt = Date.now();
          tracker.lastActivityAt = tracker.lastMutationAt;
          break;
        }
      }
    });
    tracker.observer.observe(document.body, {
      subtree: true,
      childList: true,
      characterData: true
    });
  }

  function stopObserving(tracker) {
    if (tracker && tracker.observer) {
      try {
        tracker.observer.disconnect();
      } catch {
        // No cleanup action is needed.
      }
      tracker.observer = null;
    }
  }

  async function executePrompt(prompt, rawTimeoutConfig, request, rawSubmissionConfig, rawResponseConfig) {
    if (!prompt.trim()) {
      throw new Error("Prompt must not be empty.");
    }

    const submission = await preparePromptSubmission(
      prompt,
      rawSubmissionConfig || {},
      request
    );
    request.submission = submission;
    const tracker = createResponseTracker(
      submission.trackingPrompt,
      request.requestId,
      rawTimeoutConfig
    );
    request.tracker = tracker;
    observeResponseActivity(tracker);

    try {
      const sendButton = await waitFor(
        findSendButton,
        120000,
        "the ChatGPT send button after prompt preparation",
        request.controller.signal
      );
      sendButton.click();
      tracker.submittedAt = Date.now();
      tracker.lastActivityAt = tracker.submittedAt;
      requestEvent("prompt_submitted", tracker.requestId, {
        prompt_chars: prompt.length,
        submission_mode: submission.mode,
        prompt_filename: submission.filename,
        prompt_sha256: submission.promptSha256,
        default_absolute_timeout_ms: tracker.timeout.defaultAbsoluteMs,
        requested_absolute_timeout_ms: tracker.timeout.requestedAbsoluteMs,
        effective_absolute_timeout_ms: tracker.timeout.absoluteMs,
        absolute_timeout_ms: tracker.timeout.absoluteMs,
        initial_response_timeout_ms: tracker.timeout.initialResponseMs,
        stream_idle_timeout_ms: tracker.timeout.streamIdleMs
      });

      const newMessage = await waitForInitialAssistant(tracker, request.controller.signal);
      tracker.messageKey = newMessage.key || tracker.messageKey;
      tracker.messageNode = newMessage.node;
      tracker.messageIndex = newMessage.index;

      const text = await waitForStableResponse(tracker, request.controller.signal);
      const responseConfig = rawResponseConfig || { mode: "text", filename: "", maxBytes: DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES };
      const responseArtifact =
        responseConfig.mode === "json_file"
          ? await waitForResponseArtifact(tracker, responseConfig, request.controller.signal)
          : null;
      return {
        text,
        responseArtifact,
        submission: {
          mode: submission.mode,
          filename: submission.filename,
          promptSha256: submission.promptSha256
        }
      };
    } finally {
      stopObserving(tracker);
    }
  }

  async function waitForInitialAssistant(tracker, signal) {
    const deadline = tracker.submittedAt + tracker.timeout.initialResponseMs;
    const absoluteDeadline = tracker.submittedAt + tracker.timeout.absoluteMs;
    while (Date.now() < deadline && Date.now() < absoluteDeadline) {
      throwIfAborted(signal);
      locateSubmittedUserMessage(tracker);
      const message = locateNewAssistantMessage(tracker);
      if (message) {
        tracker.lastActivityAt = Date.now();
        return message;
      }
      await sleep(POLL_INTERVAL_MS, signal);
    }
    throw new Error("Timed out waiting for a new assistant response.");
  }

  function responseIsStreaming() {
    return (
      document.querySelector(
        '[data-testid="stop-button"], button[aria-label*="Stop"], button[data-testid*="stop"]'
      ) !== null
    );
  }

  async function waitForStableResponse(tracker, signal) {
    const absoluteDeadline = tracker.submittedAt + tracker.timeout.absoluteMs;
    let previousText = "";
    let stablePolls = 0;

    while (Date.now() < absoluteDeadline) {
      throwIfAborted(signal);
      const now = Date.now();
      const located = locateNewAssistantMessage(tracker);
      if (located) {
        const text = String(located.node.innerText || "").trim();
        if (text !== previousText) {
          tracker.lastTextChangedAt = now;
          tracker.lastActivityAt = now;
          tracker.lastText = text || tracker.lastText;
          stablePolls = 0;
        }

        if (text) {
          tracker.responseStarted = true;
        }

        const streaming = responseIsStreaming();
        if (text && text === previousText && !streaming) {
          stablePolls += 1;
        } else if (text !== previousText || streaming) {
          stablePolls = 0;
        }

        if (
          text &&
          (now - tracker.lastStreamLogAt >= STREAM_LOG_INTERVAL_MS ||
            text.length - tracker.lastStreamLogChars >= 2000)
        ) {
          tracker.lastStreamLogAt = now;
          tracker.lastStreamLogChars = text.length;
          requestEvent("stream_activity", tracker.requestId, {
            response_chars: text.length,
            streaming
          });
        }

        if (stablePolls >= STABLE_POLLS_REQUIRED) {
          return text;
        }
        previousText = text;
      }

      if (
        tracker.responseStarted &&
        tracker.timeout.streamIdleMs > 0 &&
        now - tracker.lastActivityAt >= tracker.timeout.streamIdleMs
      ) {
        throw new Error(
          `Timed out after ${Math.round(tracker.timeout.streamIdleMs / 1000)} seconds without assistant response activity.`
        );
      }

      await sleep(POLL_INTERVAL_MS, signal);
    }

    throw new Error(
      `Timed out after the absolute ${Math.round(tracker.timeout.absoluteMs / 1000)}-second response limit.`
    );
  }

  function sleep(milliseconds, signal) {
    return new Promise((resolve, reject) => {
      if (signal && signal.aborted) {
        reject(signal.reason instanceof Error ? signal.reason : new Error("Request cancelled."));
        return;
      }
      const timer = setTimeout(() => {
        if (signal) {
          signal.removeEventListener("abort", onAbort);
        }
        resolve();
      }, milliseconds);
      function onAbort() {
        clearTimeout(timer);
        signal.removeEventListener("abort", onAbort);
        reject(signal.reason instanceof Error ? signal.reason : new Error("Request cancelled."));
      }
      if (signal) {
        signal.addEventListener("abort", onAbort, { once: true });
      }
    });
  }

  let previousUrl = location.href;
  setInterval(() => {
    if (location.href !== previousUrl) {
      previousUrl = location.href;
      if (socket && socket.readyState === WebSocket.OPEN) {
        safeSend(tabState("tab_update"));
      }
    }
  }, 800);

  window.addEventListener("pagehide", () => {
    shuttingDown = true;
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (activeRequest) {
      try {
        activeRequest.controller.abort(new Error("ChatGPT page is closing."));
      } catch {
        // No further action is required.
      }
    }
    if (socket) {
      try {
        socket.close();
      } catch {
        // Page is closing; no further action is needed.
      }
    }
  });

  loadBridgeTabId()
    .then(connect)
    .catch((error) => {
      console.error("ChatGPT Tab Bridge initialization failed.", error);
    });
})();
