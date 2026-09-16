from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.util
import json
import logging
import math
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

HOST = "127.0.0.1"
PORT = 8765
BRIDGE_PROTOCOL_VERSION = 12
PLUGIN_FILENAME = "bridge_plugins.py"
DEFAULT_ABSOLUTE_TIMEOUT_SECONDS = 300.0
DEFAULT_INITIAL_RESPONSE_TIMEOUT_SECONDS = 120.0
DEFAULT_STREAM_IDLE_TIMEOUT_SECONDS = 45.0
SERVER_TRANSPORT_GRACE_SECONDS = 5.0
MIN_ABSOLUTE_TIMEOUT_SECONDS = 30.0
MAX_ABSOLUTE_TIMEOUT_SECONDS = 2147483.0
DEFAULT_TEXT_SUBMISSION_THRESHOLD_CHARS = 50000
DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES = 256 * 1024 * 1024
DEFAULT_RESPONSE_ARTIFACT_START_TIMEOUT_SECONDS = 60.0
DEFAULT_RESPONSE_ARTIFACT_COMPLETION_TIMEOUT_SECONDS = 1800.0
DEFAULT_RESPONSE_ARTIFACT_IDLE_TIMEOUT_SECONDS = 300.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("chatgpt-tab-bridge")


def _bounded_phase_timeout_seconds(
    value: Any,
    *,
    default: float,
    minimum: float,
    absolute_timeout_seconds: float,
    field_name: str,
    allow_zero: bool = False,
) -> float:
    raw = default if value in (None, "") else float(value)
    if not math.isfinite(raw) or raw < 0 or (raw == 0 and not allow_zero):
        qualifier = "greater than or equal to zero" if allow_zero else "greater than zero"
        raise ValueError(f"{field_name} must be a finite number {qualifier}.")
    if allow_zero and raw == 0:
        return 0.0
    return min(absolute_timeout_seconds, max(minimum, raw))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_plugins() -> list[Any]:
    """Load optional plugins from the caller's current working directory."""
    plugin_path = Path.cwd() / PLUGIN_FILENAME
    if not plugin_path.is_file():
        return []

    spec = importlib.util.spec_from_file_location("chatgpt_bridge_external_plugins", plugin_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load plugin module: {plugin_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        raise RuntimeError(f"Failed to load {PLUGIN_FILENAME}: {exc}") from exc

    plugins = getattr(module, "PLUGINS", [])
    if not isinstance(plugins, (list, tuple)):
        raise TypeError(f"{PLUGIN_FILENAME} must define PLUGINS as a list or tuple.")
    return list(plugins)


_PLUGINS = _load_plugins()


def _notify_plugins(event_name: str, event: dict[str, Any]) -> None:
    """Notify plugins without allowing optional plugin failures to break bridging."""
    for plugin in _PLUGINS:
        callback = getattr(plugin, event_name, None)
        if not callable(callback):
            continue
        try:
            callback(event)
        except Exception as exc:
            LOGGER.warning(
                "Plugin %s.%s failed: %s",
                type(plugin).__name__,
                event_name,
                exc,
            )


@dataclass
class TabConnection:
    bridge_tab_id: str
    websocket: Any
    conversation_id: str | None
    url: str
    title: str
    status: str = "ready"
    pending: dict[str, asyncio.Future[dict[str, Any]]] = field(default_factory=dict)
    pending_context: dict[str, dict[str, Any]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class ClientConnection:
    websocket: Any
    client_id: str
    working_directory: str
    program_name: str
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    tasks: set[asyncio.Task[Any]] = field(default_factory=set)


class BridgeServer:
    def __init__(self) -> None:
        self.tabs: dict[str, TabConnection] = {}
        self.clients: set[Any] = set()

    async def handler(self, websocket: Any) -> None:
        role: str | None = None
        tab_id: str | None = None
        client: ClientConnection | None = None

        try:
            first_raw = await asyncio.wait_for(websocket.recv(), timeout=10)
            first = self._parse_json(first_raw)

            if first.get("type") == "register_tab":
                self._require_protocol_version(first, role="tab")
                role = "tab"
                tab_id = await self._register_tab(websocket, first)
                await self._tab_loop(websocket, tab_id)
                return

            if first.get("type") == "register_client":
                self._require_protocol_version(first, role="client")
                role = "client"
                self.clients.add(websocket)
                client = ClientConnection(
                    websocket=websocket,
                    client_id=str(first.get("client_id") or f"client_{uuid.uuid4().hex[:12]}"),
                    working_directory=str(first.get("working_directory") or ""),
                    program_name=str(first.get("program_name") or ""),
                )
                await websocket.send(
                    json.dumps(
                        {
                            "type": "client_registered",
                            "client_id": client.client_id,
                            "protocol_version": BRIDGE_PROTOCOL_VERSION,
                        }
                    )
                )
                _notify_plugins("on_client_registered", self._client_snapshot(client))
                await self._client_loop(client)
                return

            await websocket.send(
                json.dumps(
                    {
                        "type": "error",
                        "error": "First message must register a tab or client.",
                    }
                )
            )
        except asyncio.TimeoutError:
            self._report_error("Connection timed out before registration.")
        except ConnectionClosed:
            pass
        except Exception as exc:
            self._report_error("Unhandled bridge connection error.", exc)
        finally:
            if client is not None:
                for task in list(client.tasks):
                    task.cancel()
                if client.tasks:
                    await asyncio.gather(*client.tasks, return_exceptions=True)
            if role == "client":
                self.clients.discard(websocket)
            if role == "tab" and tab_id:
                current = self.tabs.get(tab_id)
                if current and current.websocket is websocket:
                    self._fail_pending(current, "Tab disconnected.")
                    self.tabs.pop(tab_id, None)
                    LOGGER.info("Tab disconnected: %s", tab_id)

    @staticmethod
    def _require_protocol_version(message: dict[str, Any], *, role: str) -> None:
        try:
            received = int(message.get("protocol_version"))
        except (TypeError, ValueError):
            received = 0
        if received != BRIDGE_PROTOCOL_VERSION:
            raise ConnectionError(
                f"Incompatible {role} protocol version {received or 'missing'}; "
                f"expected {BRIDGE_PROTOCOL_VERSION}. Restart WebAI Harmony and reload the extension."
            )

    async def _register_tab(self, websocket: Any, message: dict[str, Any]) -> str:
        bridge_tab_id = str(
            message.get("bridge_tab_id") or f"tab_{uuid.uuid4().hex[:12]}"
        )
        previous = self.tabs.get(bridge_tab_id)
        if previous and previous.websocket is not websocket:
            self._fail_pending(previous, "Tab connection replaced.")
            try:
                await previous.websocket.close(code=4001, reason="Tab connection replaced")
            except Exception:
                pass

        tab = TabConnection(
            bridge_tab_id=bridge_tab_id,
            websocket=websocket,
            conversation_id=self._optional_str(message.get("conversation_id")),
            url=str(message.get("url") or ""),
            title=str(message.get("title") or ""),
            status=str(message.get("status") or "ready"),
        )
        self.tabs[bridge_tab_id] = tab

        await websocket.send(
            json.dumps(
                {
                    "type": "tab_registered",
                    "bridge_tab_id": bridge_tab_id,
                    "protocol_version": BRIDGE_PROTOCOL_VERSION,
                }
            )
        )
        LOGGER.info("Tab registered: %s", bridge_tab_id)
        _notify_plugins("on_tab_registered", self._tab_snapshot(tab))
        return bridge_tab_id

    async def _tab_loop(self, websocket: Any, bridge_tab_id: str) -> None:
        async for raw in websocket:
            message = self._parse_json(raw)
            tab = self.tabs.get(bridge_tab_id)
            if not tab or tab.websocket is not websocket:
                return

            msg_type = message.get("type")
            if msg_type == "tab_update":
                tab.conversation_id = self._optional_str(message.get("conversation_id"))
                tab.url = str(message.get("url") or tab.url)
                tab.title = str(message.get("title") or tab.title)
                tab.status = str(message.get("status") or tab.status)
                continue

            if msg_type == "prompt_event":
                request_id = str(message.get("request_id") or "")
                details = message.get("details") if isinstance(message.get("details"), dict) else {}
                tab.conversation_id = self._optional_str(message.get("conversation_id"))
                tab.url = str(message.get("url") or tab.url)
                tab.title = str(message.get("title") or tab.title)
                event_name = str(message.get("event_name") or "")
                _notify_plugins(
                    "on_prompt_event",
                    {
                        **tab.pending_context.get(request_id, {}),
                        **self._tab_snapshot(tab),
                        "request_id": request_id,
                        "event_name": event_name,
                        **{f"detail_{key}": value for key, value in details.items()},
                    },
                )
                if event_name == "state_reset":
                    tab.pending_context.pop(request_id, None)
                continue

            if msg_type == "prompt_result":
                request_id = str(message.get("request_id") or "")
                tab.conversation_id = self._optional_str(
                    message.get("conversation_id")
                )
                tab.url = str(message.get("url") or tab.url)
                tab.title = str(message.get("title") or tab.title)
                future = tab.pending.pop(request_id, None)
                tab.status = "ready"
                if future and not future.done():
                    future.set_result(message)
                continue

            if msg_type == "prompt_error":
                request_id = str(message.get("request_id") or "")
                future = tab.pending.pop(request_id, None)
                tab.status = "ready"
                error = str(message.get("error") or "Unknown tab error")
                if future and not future.done():
                    future.set_exception(RuntimeError(error))
                continue

            if msg_type == "artifact_cleanup_result":
                request_id = str(message.get("request_id") or "")
                future = tab.pending.pop(request_id, None)
                if future and not future.done():
                    future.set_result(message)
                continue

            if msg_type == "artifact_cleanup_error":
                request_id = str(message.get("request_id") or "")
                future = tab.pending.pop(request_id, None)
                error = str(message.get("error") or "Unknown artifact cleanup error")
                if future and not future.done():
                    future.set_exception(RuntimeError(error))
                continue

            self._report_error(
                f"Unsupported tab message type from {bridge_tab_id}: {msg_type!r}"
            )

    async def _client_loop(self, client: ClientConnection) -> None:
        async for raw in client.websocket:
            message = self._parse_json(raw)
            task = asyncio.create_task(self._handle_client_request(client, message))
            client.tasks.add(task)
            task.add_done_callback(client.tasks.discard)

    async def _handle_client_request(
        self, client: ClientConnection, message: dict[str, Any]
    ) -> None:
        request_id = str(message.get("request_id") or f"req_{uuid.uuid4().hex}")
        try:
            action = message.get("action")
            if action == "list_tabs":
                result = {"tabs": self._list_tabs()}
            elif action == "send_prompt":
                result = await self._send_prompt(message, client)
            elif action == "cleanup_response_artifact":
                result = await self._cleanup_response_artifact(message)
            else:
                raise ValueError(f"Unsupported action: {action!r}")

            response = {
                "type": "client_response",
                "request_id": request_id,
                "ok": True,
                "result": result,
            }
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._report_error(
                "Client request failed.",
                exc,
                request_id=request_id,
                action=message.get("action"),
                **self._client_snapshot(client),
            )
            response = {
                "type": "client_response",
                "request_id": request_id,
                "ok": False,
                "error": str(exc),
            }

        async with client.send_lock:
            await client.websocket.send(json.dumps(response))

    def _list_tabs(self) -> list[dict[str, Any]]:
        return [
            self._tab_snapshot(tab)
            for tab in sorted(self.tabs.values(), key=lambda item: item.bridge_tab_id)
        ]

    async def _cleanup_response_artifact(self, message: dict[str, Any]) -> dict[str, Any]:
        bridge_tab_id = str(message.get("bridge_tab_id") or "").strip()
        if not bridge_tab_id:
            raise ValueError("bridge_tab_id is required for artifact cleanup.")
        tab = self.tabs.get(bridge_tab_id)
        if not tab:
            raise RuntimeError(f"Bridge tab {bridge_tab_id!r} is not connected.")
        cleanup_token = str(message.get("cleanup_token") or "").strip()
        raw_download_id = message.get("download_id")
        download_id = int(raw_download_id) if raw_download_id not in (None, "") else None
        request_id = f"cleanup_{uuid.uuid4().hex}"
        future = asyncio.get_running_loop().create_future()
        tab.pending[request_id] = future
        try:
            await tab.websocket.send(
                json.dumps(
                    {
                        "type": "cleanup_response_artifact",
                        "request_id": request_id,
                        "cleanup_token": cleanup_token,
                        "download_id": download_id,
                    }
                )
            )
            result = await asyncio.wait_for(future, timeout=30.0)
            return {
                "cleaned": bool(result.get("cleaned", True)),
                "download_id": result.get("download_id", download_id),
            }
        finally:
            tab.pending.pop(request_id, None)

    async def _send_prompt(
        self,
        message: dict[str, Any],
        client: ClientConnection,
    ) -> dict[str, Any]:
        bridge_tab_id = str(message.get("bridge_tab_id") or "")
        prompt = str(message.get("prompt") or "")
        submission_mode = str(message.get("submission_mode") or "auto").strip().lower()
        if submission_mode not in {"auto", "text", "file"}:
            raise ValueError("submission_mode must be auto, text, or file.")
        prompt_filename = str(message.get("prompt_filename") or "complete_prompt.txt").strip() or "complete_prompt.txt"
        if not prompt_filename.lower().endswith(".txt"):
            prompt_filename += ".txt"
        prompt_filename = prompt_filename.replace("/", "_").replace("\\", "_")
        text_submission_threshold_chars = int(
            message.get("text_submission_threshold_chars")
            or DEFAULT_TEXT_SUBMISSION_THRESHOLD_CHARS
        )
        if text_submission_threshold_chars <= 0:
            raise ValueError("text_submission_threshold_chars must be greater than zero.")
        prompt_sha256 = str(message.get("prompt_sha256") or "").strip()
        response_mode = str(message.get("response_mode") or "text").strip().lower()
        if response_mode not in {"text", "json_file"}:
            raise ValueError("response_mode must be text or json_file.")
        response_filename = str(message.get("response_filename") or "").strip()
        if response_mode == "json_file":
            if not response_filename or not response_filename.lower().endswith(".json"):
                raise ValueError("response_filename must be an exact .json filename for json_file mode.")
            response_filename = response_filename.replace("/", "_").replace("\\", "_")
        response_max_bytes = int(message.get("response_max_bytes") or DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES)
        if response_max_bytes <= 0 or response_max_bytes > DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES:
            raise ValueError(
                f"response_max_bytes must be between 1 and {DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES}."
            )
        response_artifact_start_timeout_seconds = min(
            300.0,
            max(5.0, float(message.get("response_artifact_start_timeout_seconds") or DEFAULT_RESPONSE_ARTIFACT_START_TIMEOUT_SECONDS)),
        )
        response_artifact_completion_timeout_seconds = min(
            7200.0,
            max(30.0, float(message.get("response_artifact_completion_timeout_seconds") or DEFAULT_RESPONSE_ARTIFACT_COMPLETION_TIMEOUT_SECONDS)),
        )
        response_artifact_idle_timeout_seconds = min(
            response_artifact_completion_timeout_seconds,
            max(10.0, float(message.get("response_artifact_idle_timeout_seconds") or DEFAULT_RESPONSE_ARTIFACT_IDLE_TIMEOUT_SECONDS)),
        )
        declared_timeout_source = str(message.get("timeout_request_source") or "").strip()
        requested_timeout_value = message.get("absolute_timeout_seconds")
        requested_timeout_source = (
            declared_timeout_source or "absolute_timeout_seconds"
        )
        if requested_timeout_value in (None, ""):
            requested_timeout_value = message.get("timeout_seconds")
            requested_timeout_source = (
                declared_timeout_source
                or (
                    "timeout_seconds"
                    if requested_timeout_value not in (None, "")
                    else "bridge_default"
                )
            )
        raw_timeout_seconds = float(
            DEFAULT_ABSOLUTE_TIMEOUT_SECONDS
            if requested_timeout_value in (None, "")
            else requested_timeout_value
        )
        if not math.isfinite(raw_timeout_seconds) or raw_timeout_seconds <= 0:
            raise ValueError(
                "absolute_timeout_seconds must be a finite number greater than zero."
            )
        timeout_seconds = min(
            MAX_ABSOLUTE_TIMEOUT_SECONDS,
            max(MIN_ABSOLUTE_TIMEOUT_SECONDS, raw_timeout_seconds),
        )

        initial_response_timeout_seconds = _bounded_phase_timeout_seconds(
            message.get("initial_response_timeout_seconds"),
            default=DEFAULT_INITIAL_RESPONSE_TIMEOUT_SECONDS,
            minimum=15.0,
            absolute_timeout_seconds=timeout_seconds,
            field_name="initial_response_timeout_seconds",
        )
        stream_idle_timeout_seconds = _bounded_phase_timeout_seconds(
            message.get("stream_idle_timeout_seconds"),
            default=DEFAULT_STREAM_IDLE_TIMEOUT_SECONDS,
            minimum=10.0,
            absolute_timeout_seconds=timeout_seconds,
            field_name="stream_idle_timeout_seconds",
            allow_zero=True,
        )

        if not bridge_tab_id:
            raise ValueError("bridge_tab_id is required.")
        if not prompt.strip():
            raise ValueError("prompt must not be empty.")

        tab = self.tabs.get(bridge_tab_id)
        if not tab:
            raise ValueError(f"Unknown or disconnected tab: {bridge_tab_id}")

        async with tab.lock:
            if self.tabs.get(bridge_tab_id) is not tab:
                raise ConnectionError(f"Tab connection changed: {bridge_tab_id}")

            request_id = f"tabreq_{uuid.uuid4().hex}"
            future = asyncio.get_running_loop().create_future()
            tab.pending[request_id] = future
            requested_timeout_for_log = message.get(
                "requested_absolute_timeout_seconds"
            )
            if requested_timeout_for_log in (None, ""):
                requested_timeout_for_log = (
                    raw_timeout_seconds
                    if requested_timeout_source
                    not in {"bridge_default", "client_default"}
                    else None
                )
            timeout_metadata = {
                "default_absolute_timeout_seconds": DEFAULT_ABSOLUTE_TIMEOUT_SECONDS,
                "requested_absolute_timeout_seconds": requested_timeout_for_log,
                "effective_absolute_timeout_seconds": timeout_seconds,
                "timeout_request_source": requested_timeout_source,
                "initial_response_timeout_seconds": initial_response_timeout_seconds,
                "stream_idle_timeout_seconds": stream_idle_timeout_seconds,
                "submission_mode_requested": submission_mode,
                "prompt_filename": prompt_filename,
                "text_submission_threshold_chars": text_submission_threshold_chars,
                "prompt_sha256": prompt_sha256,
                "response_mode": response_mode,
                "response_filename": response_filename,
                "response_max_bytes": response_max_bytes,
                "response_artifact_start_timeout_seconds": response_artifact_start_timeout_seconds,
                "response_artifact_completion_timeout_seconds": response_artifact_completion_timeout_seconds,
                "response_artifact_idle_timeout_seconds": response_artifact_idle_timeout_seconds,
            }
            tab.pending_context[request_id] = {
                **self._client_snapshot(client),
                **timeout_metadata,
            }
            tab.status = "busy"

            _notify_plugins(
                "on_prompt_sent",
                {
                    **self._client_snapshot(client),
                    **self._tab_snapshot(tab),
                    "request_id": request_id,
                    "prompt_chars": len(prompt),
                    **timeout_metadata,
                },
            )

            try:
                await tab.websocket.send(
                    json.dumps(
                        {
                            "type": "send_prompt",
                            "request_id": request_id,
                            "prompt": prompt,
                            "submission_mode": submission_mode,
                            "prompt_filename": prompt_filename,
                            "text_submission_threshold_chars": text_submission_threshold_chars,
                            "prompt_sha256": prompt_sha256,
                            "response_mode": response_mode,
                            "response_filename": response_filename,
                            "response_max_bytes": response_max_bytes,
                            "response_artifact_start_timeout_ms": int(response_artifact_start_timeout_seconds * 1000),
                            "response_artifact_completion_timeout_ms": int(response_artifact_completion_timeout_seconds * 1000),
                            "response_artifact_idle_timeout_ms": int(response_artifact_idle_timeout_seconds * 1000),
                            "timeout_config": {
                                "default_absolute_timeout_ms": int(DEFAULT_ABSOLUTE_TIMEOUT_SECONDS * 1000),
                                "requested_absolute_timeout_ms": (
                                    int(float(requested_timeout_for_log) * 1000)
                                    if requested_timeout_for_log not in (None, "")
                                    else None
                                ),
                                "effective_absolute_timeout_ms": int(timeout_seconds * 1000),
                                "absolute_timeout_ms": int(timeout_seconds * 1000),
                                "initial_response_timeout_ms": int(initial_response_timeout_seconds * 1000),
                                "stream_idle_timeout_ms": int(stream_idle_timeout_seconds * 1000),
                            },
                        }
                    )
                )
                artifact_transport_seconds = (
                    response_artifact_start_timeout_seconds
                    + response_artifact_completion_timeout_seconds
                    if response_mode == "json_file"
                    else 0.0
                )
                result = await asyncio.wait_for(
                    future,
                    timeout=timeout_seconds + artifact_transport_seconds + SERVER_TRANSPORT_GRACE_SECONDS,
                )
            except asyncio.TimeoutError:
                tab.pending.pop(request_id, None)
                tab.status = "ready"
                try:
                    await tab.websocket.send(
                        json.dumps(
                            {
                                "type": "cancel_prompt",
                                "request_id": request_id,
                                "reason": f"Bridge server reached the generation and response-artifact timeout window ({int(timeout_seconds)} seconds for generation).",
                            }
                        )
                    )
                except Exception:
                    pass
                raise TimeoutError(
                    f"Bridge server reached the generation and response-artifact timeout window ({int(timeout_seconds)} seconds for generation)."
                )
            except asyncio.CancelledError:
                tab.pending.pop(request_id, None)
                tab.status = "ready"
                try:
                    await tab.websocket.send(
                        json.dumps(
                            {
                                "type": "cancel_prompt",
                                "request_id": request_id,
                                "reason": "Bridge client cancelled the request.",
                            }
                        )
                    )
                except Exception:
                    pass
                raise
            except Exception:
                tab.pending.pop(request_id, None)
                tab.status = "ready"
                raise

        text = str(result.get("text") or "")
        response_artifact = result.get("response_artifact") if isinstance(result.get("response_artifact"), dict) else None
        if response_mode == "json_file":
            if not response_artifact:
                raise RuntimeError("ChatGPT response did not include the required JSON artifact.")
            artifact_filename = str(response_artifact.get("filename") or "")
            if artifact_filename != response_filename:
                raise RuntimeError(
                    f"ChatGPT returned response artifact {artifact_filename or '[blank]'}; expected {response_filename}."
                )
            artifact_b64 = str(response_artifact.get("content_base64") or "")
            local_path = str(response_artifact.get("local_path") or "").strip()
            declared_size = int(response_artifact.get("size_bytes") or 0)
            declared_sha256 = str(response_artifact.get("sha256") or "").lower()
            if artifact_b64:
                try:
                    artifact_bytes = base64.b64decode(artifact_b64, validate=True)
                except Exception as exc:
                    raise RuntimeError("ChatGPT response artifact was not valid base64.") from exc
                if not artifact_bytes or len(artifact_bytes) > response_max_bytes:
                    raise RuntimeError(
                        f"ChatGPT response artifact size {len(artifact_bytes)} is outside the allowed range 1..{response_max_bytes}."
                    )
                actual_size = len(artifact_bytes)
                actual_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
                normalized_base64 = base64.b64encode(artifact_bytes).decode("ascii")
            elif local_path:
                artifact_path = Path(local_path).expanduser().resolve()
                try:
                    actual_size = artifact_path.stat().st_size
                except OSError as exc:
                    raise RuntimeError(f"Could not inspect downloaded response artifact {artifact_path}: {exc}") from exc
                if actual_size <= 0 or actual_size > response_max_bytes:
                    raise RuntimeError(
                        f"ChatGPT response artifact size {actual_size} is outside the allowed range 1..{response_max_bytes}."
                    )
                try:
                    actual_sha256 = _file_sha256(artifact_path)
                except OSError as exc:
                    raise RuntimeError(f"Could not hash downloaded response artifact {artifact_path}: {exc}") from exc
                normalized_base64 = ""
            else:
                raise RuntimeError("ChatGPT response artifact contained neither file bytes nor a downloaded local path.")
            if declared_size and declared_size != actual_size:
                raise RuntimeError(
                    f"ChatGPT response artifact size mismatch: declared {declared_size}, received {actual_size}."
                )
            if declared_sha256 and declared_sha256 != actual_sha256:
                raise RuntimeError("ChatGPT response artifact SHA-256 mismatch.")
            response_artifact = {
                **response_artifact,
                "size_bytes": actual_size,
                "sha256": actual_sha256,
                "content_base64": normalized_base64,
                "local_path": local_path,
            }
        _notify_plugins(
            "on_response_received",
            {
                **self._client_snapshot(client),
                **self._tab_snapshot(tab),
                "request_id": request_id,
                "response_chars": len(text),
                "submission_mode_effective": str(result.get("submission_mode") or submission_mode),
                "prompt_filename_effective": str(result.get("prompt_filename") or prompt_filename),
                **timeout_metadata,
            },
        )
        return {
            "bridge_tab_id": bridge_tab_id,
            "conversation_id": tab.conversation_id,
            "url": tab.url,
            "title": tab.title,
            "text": text,
            "submission_mode": str(result.get("submission_mode") or submission_mode),
            "prompt_filename": str(result.get("prompt_filename") or prompt_filename),
            "prompt_sha256": str(result.get("prompt_sha256") or prompt_sha256),
            "response_artifact": response_artifact,
            **timeout_metadata,
        }

    @staticmethod
    def _parse_json(raw: Any) -> dict[str, Any]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Protocol message must be a JSON object.")
        return data

    @staticmethod
    def _optional_str(value: Any) -> str | None:
        return None if value in (None, "") else str(value)


    @staticmethod
    def _client_snapshot(client: ClientConnection) -> dict[str, Any]:
        return {
            "client_id": client.client_id,
            "client_working_directory": client.working_directory,
            "program_name": client.program_name,
        }

    @staticmethod
    def _tab_snapshot(tab: TabConnection) -> dict[str, Any]:
        return {
            "bridge_tab_id": tab.bridge_tab_id,
            "conversation_id": tab.conversation_id,
            "url": tab.url,
            "title": tab.title,
            "status": tab.status,
        }

    @staticmethod
    def _fail_pending(tab: TabConnection, reason: str) -> None:
        for future in tab.pending.values():
            if not future.done():
                future.set_exception(ConnectionError(reason))
        tab.pending.clear()
        tab.pending_context.clear()
        tab.status = "disconnected"

    @staticmethod
    def _report_error(
        message: str,
        exception: Exception | None = None,
        **context: Any,
    ) -> None:
        if exception is None:
            LOGGER.error(message)
        else:
            LOGGER.error("%s %s", message, exception)
        _notify_plugins(
            "on_error",
            {
                "message": message,
                "error": str(exception) if exception else None,
                **context,
            },
        )


async def main() -> None:
    bridge = BridgeServer()
    LOGGER.info("Starting ChatGPT Tab Bridge on ws://%s:%s", HOST, PORT)
    _notify_plugins("on_bridge_started", {"host": HOST, "port": PORT})

    async with websockets.serve(
        bridge.handler,
        HOST,
        PORT,
        max_size=None,
        ping_interval=20,
        ping_timeout=20,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("Bridge server stopped.")
