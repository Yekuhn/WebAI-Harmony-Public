from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from threading import Lock
from typing import Any


class BridgePlugin:
    def on_bridge_started(self, event: dict[str, Any]) -> None:
        pass

    def on_client_registered(self, event: dict[str, Any]) -> None:
        pass

    def on_tab_registered(self, event: dict[str, Any]) -> None:
        pass

    def on_prompt_sent(self, event: dict[str, Any]) -> None:
        pass

    def on_response_received(self, event: dict[str, Any]) -> None:
        pass

    def on_prompt_event(self, event: dict[str, Any]) -> None:
        pass

    def on_error(self, event: dict[str, Any]) -> None:
        pass


class LoggerPlugin(BridgePlugin):
    def __init__(self, filename: str = "bridge.log") -> None:
        self.filename = filename
        self._lock = Lock()
        self._initialized_paths: set[Path] = set()

    def on_client_registered(self, event: dict[str, Any]) -> None:
        self._write("CLIENT_REGISTERED", event)

    def on_prompt_sent(self, event: dict[str, Any]) -> None:
        self._write("PROMPT_SENT", event)

    def on_response_received(self, event: dict[str, Any]) -> None:
        self._write("RESPONSE_RECEIVED", event)

    def on_prompt_event(self, event: dict[str, Any]) -> None:
        event_name = str(event.get("event_name") or "PROMPT_EVENT").strip().upper()
        self._write(event_name or "PROMPT_EVENT", event)

    def on_error(self, event: dict[str, Any]) -> None:
        self._write("ERROR", event)

    def _resolve_path(self, event: dict[str, Any]) -> Path | None:
        raw_directory = event.get("client_working_directory")
        if not raw_directory:
            return None
        directory = Path(str(raw_directory)).expanduser()
        if not directory.is_dir():
            return None
        return directory / self.filename

    def _write(self, event_name: str, event: dict[str, Any]) -> None:
        path = self._resolve_path(event)
        if path is None:
            return

        timestamp = datetime.now().astimezone().isoformat(timespec="milliseconds")
        fields = " ".join(
            f"{key}={self._clean(value)}"
            for key, value in sorted(event.items())
            if key != "client_working_directory" and value not in (None, "")
        )
        line = f"{timestamp} {event_name}"
        if fields:
            line = f"{line} {fields}"

        with self._lock:
            if path not in self._initialized_paths:
                path.write_text("", encoding="utf-8")
                self._initialized_paths.add(path)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    @staticmethod
    def _clean(value: Any) -> str:
        if isinstance(value, (dict, list, tuple, bool, int, float)):
            try:
                rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
            except (TypeError, ValueError):
                rendered = str(value)
        else:
            rendered = str(value)
        rendered = rendered.replace("\r", " ").replace("\n", " ")
        return rendered if len(rendered) <= 20000 else rendered[:20000] + "…[truncated]"


PLUGINS = [LoggerPlugin()]
