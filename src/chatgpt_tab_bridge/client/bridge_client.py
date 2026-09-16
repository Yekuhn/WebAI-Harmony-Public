from __future__ import annotations

import asyncio
import hashlib
import json
import math
import uuid
from pathlib import Path
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed


DEFAULT_ABSOLUTE_TIMEOUT_SECONDS = 300.0
DEFAULT_INITIAL_RESPONSE_TIMEOUT_SECONDS = 120.0
DEFAULT_STREAM_IDLE_TIMEOUT_SECONDS = 45.0
CLIENT_TRANSPORT_BUFFER_SECONDS = 30.0
DEFAULT_TEXT_SUBMISSION_THRESHOLD_CHARS = 50000
DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES = 256 * 1024 * 1024
DEFAULT_RESPONSE_ARTIFACT_START_TIMEOUT_SECONDS = 60.0
DEFAULT_RESPONSE_ARTIFACT_COMPLETION_TIMEOUT_SECONDS = 1800.0
DEFAULT_RESPONSE_ARTIFACT_IDLE_TIMEOUT_SECONDS = 300.0
MIN_ABSOLUTE_TIMEOUT_SECONDS = 30.0
MAX_ABSOLUTE_TIMEOUT_SECONDS = 2147483.0
BRIDGE_PROTOCOL_VERSION = 12


def _positive_timeout(value: float | int | None, *, default: float, field_name: str) -> float:
    resolved = default if value is None else float(value)
    if not math.isfinite(resolved) or resolved <= 0:
        raise ValueError(f"{field_name} must be a finite number greater than zero.")
    return resolved


def _stream_idle_timeout(value: float | int | None) -> float:
    resolved = DEFAULT_STREAM_IDLE_TIMEOUT_SECONDS if value is None else float(value)
    if not math.isfinite(resolved) or resolved < 0:
        raise ValueError("stream_idle_timeout_seconds must be a finite number greater than or equal to zero.")
    return resolved


class BridgeClient:
    def __init__(
        self,
        uri: str = "ws://127.0.0.1:8765",
        *,
        working_directory: str | Path | None = None,
        program_name: str | None = None,
    ) -> None:
        self.uri = uri
        self.client_id = f"client_{uuid.uuid4().hex[:12]}"
        self.working_directory = str(
            Path(working_directory).expanduser().resolve()
            if working_directory is not None
            else Path.cwd().resolve()
        )
        self.program_name = program_name or Path(self.working_directory).name
        self.websocket: Any | None = None
        self._connect_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._receiver_task: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}

    async def __aenter__(self) -> "BridgeClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def connect(self) -> None:
        async with self._connect_lock:
            if self.websocket is not None:
                return

            websocket = await websockets.connect(
                self.uri,
                max_size=None,
                ping_interval=20,
                ping_timeout=20,
            )
            try:
                await websocket.send(
                    json.dumps(
                        {
                            "type": "register_client",
                            "client_id": self.client_id,
                            "working_directory": self.working_directory,
                            "program_name": self.program_name,
                            "protocol_version": BRIDGE_PROTOCOL_VERSION,
                        }
                    )
                )
                response = self._parse_json(await websocket.recv())
                if response.get("type") != "client_registered":
                    raise ConnectionError("Bridge server rejected client registration.")
                if response.get("protocol_version") != BRIDGE_PROTOCOL_VERSION:
                    raise ConnectionError(
                        "The running ChatGPT bridge is outdated or incompatible. "
                        "Restart WebAI Harmony."
                    )
            except Exception:
                await websocket.close()
                raise

            self.websocket = websocket
            self._receiver_task = asyncio.create_task(self._receive_loop(websocket))

    async def close(self) -> None:
        async with self._connect_lock:
            websocket = self.websocket
            receiver = self._receiver_task
            self.websocket = None
            self._receiver_task = None

            if websocket is not None:
                await websocket.close()
            if receiver is not None and receiver is not asyncio.current_task():
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)

            self._fail_pending(ConnectionError("Bridge client closed."))

    async def list_tabs(self) -> list[dict[str, Any]]:
        result = await self._request("list_tabs")
        tabs = result.get("tabs", [])
        if not isinstance(tabs, list):
            raise RuntimeError("Bridge returned an invalid tabs payload.")
        return tabs

    async def send_prompt(
        self,
        bridge_tab_id: str,
        prompt: str,
        timeout_seconds: float | None = None,
        *,
        absolute_timeout_seconds: float | None = None,
        initial_response_timeout_seconds: float = DEFAULT_INITIAL_RESPONSE_TIMEOUT_SECONDS,
        stream_idle_timeout_seconds: float = DEFAULT_STREAM_IDLE_TIMEOUT_SECONDS,
        submission_mode: str = "auto",
        prompt_filename: str | None = None,
        text_submission_threshold_chars: int = DEFAULT_TEXT_SUBMISSION_THRESHOLD_CHARS,
        response_mode: str = "text",
        response_filename: str = "",
        response_max_bytes: int = DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES,
        response_artifact_start_timeout_seconds: float = DEFAULT_RESPONSE_ARTIFACT_START_TIMEOUT_SECONDS,
        response_artifact_completion_timeout_seconds: float = DEFAULT_RESPONSE_ARTIFACT_COMPLETION_TIMEOUT_SECONDS,
        response_artifact_idle_timeout_seconds: float = DEFAULT_RESPONSE_ARTIFACT_IDLE_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        """Send one prompt with program-selected timeout and transport settings.

        ``submission_mode`` is ``text``, ``file``, or ``auto``.  Auto mode uses
        normal composer text for smaller prompts and an in-browser UTF-8 text
        attachment for larger prompts or composer insertion failure.  The
        threshold selects transport only; it never rejects or truncates content.
        A stream-idle value of zero disables that watchdog.
        """
        if not bridge_tab_id:
            raise ValueError("bridge_tab_id is required.")
        prompt_text = str(prompt or "")
        if not prompt_text.strip():
            raise ValueError("prompt must not be empty.")

        mode = str(submission_mode or "auto").strip().lower()
        if mode not in {"auto", "text", "file"}:
            raise ValueError("submission_mode must be auto, text, or file.")
        threshold = int(text_submission_threshold_chars)
        if threshold <= 0:
            raise ValueError("text_submission_threshold_chars must be greater than zero.")
        filename = str(prompt_filename or "complete_prompt.txt").strip() or "complete_prompt.txt"
        if not filename.lower().endswith(".txt"):
            filename += ".txt"
        filename = filename.replace("/", "_").replace("\\", "_")
        prompt_sha256 = hashlib.sha256(prompt_text.encode("utf-8")).hexdigest()
        resolved_response_mode = str(response_mode or "text").strip().lower()
        if resolved_response_mode not in {"text", "json_file"}:
            raise ValueError("response_mode must be text or json_file.")
        resolved_response_filename = str(response_filename or "").strip()
        if resolved_response_mode == "json_file":
            if not resolved_response_filename.lower().endswith(".json"):
                raise ValueError("response_filename must be an exact .json filename in json_file mode.")
            resolved_response_filename = resolved_response_filename.replace("/", "_").replace("\\", "_")
        resolved_response_max_bytes = int(response_max_bytes)
        if resolved_response_max_bytes <= 0 or resolved_response_max_bytes > DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES:
            raise ValueError(
                f"response_max_bytes must be between 1 and {DEFAULT_RESPONSE_ARTIFACT_MAX_BYTES}."
            )
        artifact_start_timeout = min(
            300.0,
            max(5.0, _positive_timeout(
                response_artifact_start_timeout_seconds,
                default=DEFAULT_RESPONSE_ARTIFACT_START_TIMEOUT_SECONDS,
                field_name="response_artifact_start_timeout_seconds",
            )),
        )
        artifact_completion_timeout = min(
            7200.0,
            max(30.0, _positive_timeout(
                response_artifact_completion_timeout_seconds,
                default=DEFAULT_RESPONSE_ARTIFACT_COMPLETION_TIMEOUT_SECONDS,
                field_name="response_artifact_completion_timeout_seconds",
            )),
        )
        artifact_idle_timeout = min(
            artifact_completion_timeout,
            max(10.0, _positive_timeout(
                response_artifact_idle_timeout_seconds,
                default=DEFAULT_RESPONSE_ARTIFACT_IDLE_TIMEOUT_SECONDS,
                field_name="response_artifact_idle_timeout_seconds",
            )),
        )

        if absolute_timeout_seconds is not None:
            requested_absolute: float | None = float(absolute_timeout_seconds)
            timeout_request_source = "absolute_timeout_seconds"
        elif timeout_seconds is not None:
            requested_absolute = float(timeout_seconds)
            timeout_request_source = "timeout_seconds"
        else:
            requested_absolute = None
            timeout_request_source = "client_default"

        resolved_absolute = (
            DEFAULT_ABSOLUTE_TIMEOUT_SECONDS
            if requested_absolute is None
            else requested_absolute
        )
        effective_absolute = min(
            MAX_ABSOLUTE_TIMEOUT_SECONDS,
            max(
                MIN_ABSOLUTE_TIMEOUT_SECONDS,
                _positive_timeout(
                    resolved_absolute,
                    default=DEFAULT_ABSOLUTE_TIMEOUT_SECONDS,
                    field_name="absolute_timeout_seconds",
                ),
            ),
        )
        effective_initial = min(
            effective_absolute,
            _positive_timeout(
                initial_response_timeout_seconds,
                default=DEFAULT_INITIAL_RESPONSE_TIMEOUT_SECONDS,
                field_name="initial_response_timeout_seconds",
            ),
        )
        requested_idle = _stream_idle_timeout(stream_idle_timeout_seconds)
        effective_idle = 0.0 if requested_idle == 0 else min(effective_absolute, requested_idle)

        artifact_transport_timeout = (
            artifact_start_timeout + artifact_completion_timeout
            if resolved_response_mode == "json_file"
            else 0.0
        )
        return await self._request(
            "send_prompt",
            client_wait_timeout_seconds=effective_absolute + artifact_transport_timeout + CLIENT_TRANSPORT_BUFFER_SECONDS,
            bridge_tab_id=bridge_tab_id,
            prompt=prompt_text,
            prompt_chars=len(prompt_text),
            prompt_utf8_bytes=len(prompt_text.encode("utf-8")),
            prompt_sha256=prompt_sha256,
            submission_mode=mode,
            prompt_filename=filename,
            text_submission_threshold_chars=threshold,
            absolute_timeout_seconds=effective_absolute,
            requested_absolute_timeout_seconds=requested_absolute,
            timeout_request_source=timeout_request_source,
            # Retain the legacy field so v0.4.x servers can still execute it.
            timeout_seconds=effective_absolute,
            initial_response_timeout_seconds=effective_initial,
            stream_idle_timeout_seconds=effective_idle,
            response_mode=resolved_response_mode,
            response_filename=resolved_response_filename,
            response_max_bytes=resolved_response_max_bytes,
            response_artifact_start_timeout_seconds=artifact_start_timeout,
            response_artifact_completion_timeout_seconds=artifact_completion_timeout,
            response_artifact_idle_timeout_seconds=artifact_idle_timeout,
        )

    async def cleanup_response_artifact(
        self,
        bridge_tab_id: str,
        *,
        cleanup_token: str = "",
        download_id: int | None = None,
    ) -> dict[str, Any]:
        if not bridge_tab_id:
            raise ValueError("bridge_tab_id is required for artifact cleanup.")
        return await self._request(
            "cleanup_response_artifact",
            client_wait_timeout_seconds=35.0,
            bridge_tab_id=str(bridge_tab_id),
            cleanup_token=str(cleanup_token or ""),
            download_id=download_id,
        )

    async def pipe(
        self,
        source_tab_id: str,
        source_prompt: str,
        target_tab_id: str,
        target_prompt_template: str,
        timeout_seconds: float | None = None,
        *,
        absolute_timeout_seconds: float | None = None,
        initial_response_timeout_seconds: float = DEFAULT_INITIAL_RESPONSE_TIMEOUT_SECONDS,
        stream_idle_timeout_seconds: float = DEFAULT_STREAM_IDLE_TIMEOUT_SECONDS,
        submission_mode: str = "auto",
        text_submission_threshold_chars: int = DEFAULT_TEXT_SUBMISSION_THRESHOLD_CHARS,
    ) -> dict[str, Any]:
        source = await self.send_prompt(
            source_tab_id,
            source_prompt,
            timeout_seconds,
            absolute_timeout_seconds=absolute_timeout_seconds,
            initial_response_timeout_seconds=initial_response_timeout_seconds,
            stream_idle_timeout_seconds=stream_idle_timeout_seconds,
            submission_mode=submission_mode,
            text_submission_threshold_chars=text_submission_threshold_chars,
        )
        target_prompt = target_prompt_template.replace("{{output}}", source["text"])
        target = await self.send_prompt(
            target_tab_id,
            target_prompt,
            timeout_seconds,
            absolute_timeout_seconds=absolute_timeout_seconds,
            initial_response_timeout_seconds=initial_response_timeout_seconds,
            stream_idle_timeout_seconds=stream_idle_timeout_seconds,
            submission_mode=submission_mode,
            text_submission_threshold_chars=text_submission_threshold_chars,
        )
        return {"source": source, "target": target}

    async def _request(
        self,
        action: str,
        *,
        client_wait_timeout_seconds: float | None = None,
        **payload: Any,
    ) -> dict[str, Any]:
        await self.connect()
        websocket = self.websocket
        if websocket is None:
            raise ConnectionError("Bridge client is not connected.")

        request_id = f"req_{uuid.uuid4().hex}"
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future

        try:
            async with self._send_lock:
                await websocket.send(
                    json.dumps(
                        {
                            "type": "client_request",
                            "request_id": request_id,
                            "action": action,
                            **payload,
                        }
                    )
                )
            if client_wait_timeout_seconds is None:
                response = await future
            else:
                response = await asyncio.wait_for(
                    asyncio.shield(future),
                    timeout=float(client_wait_timeout_seconds),
                )
        except asyncio.CancelledError:
            pending = self._pending.pop(request_id, None)
            if pending is not None and not pending.done():
                pending.cancel()
            raise
        except Exception:
            pending = self._pending.pop(request_id, None)
            if pending is not None and not pending.done():
                pending.cancel()
            raise

        if not response.get("ok"):
            raise RuntimeError(str(response.get("error") or "Bridge request failed."))

        result = response.get("result", {})
        if not isinstance(result, dict):
            raise RuntimeError("Bridge returned an invalid result payload.")
        return result

    async def _receive_loop(self, websocket: Any) -> None:
        try:
            async for raw in websocket:
                response = self._parse_json(raw)
                if response.get("type") != "client_response":
                    continue
                request_id = str(response.get("request_id") or "")
                future = self._pending.pop(request_id, None)
                if future and not future.done():
                    future.set_result(response)
        except ConnectionClosed as exc:
            self._fail_pending(ConnectionError(f"Bridge connection closed: {exc}"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail_pending(ConnectionError(f"Bridge receive loop failed: {exc}"))
        finally:
            if self.websocket is websocket:
                self.websocket = None
                self._receiver_task = None

    def _fail_pending(self, error: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    @staticmethod
    def _parse_json(raw: Any) -> dict[str, Any]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("Protocol message must be a JSON object.")
        return data
