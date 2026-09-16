# ChatGPT Tab Bridge

A small local bridge that lets Python programs send prompts to open ChatGPT tabs and receive either normal text or a downloadable JSON response artifact.

## Features

- Registers each open Chrome tab with a unique `bridge_tab_id`
- Tracks the ChatGPT `conversation_id` when available
- Processes one prompt at a time within each tab while allowing different tabs to work concurrently
- Supports UTF-8 prompt-file attachment transport for large requests
- Finds the unique clickable response artifact by its request-specific response ID, whether ChatGPT renders it as a link or a button
- Supports downloadable JSON response artifacts up to 256 MB
- Fetches linked artifacts directly when possible and clicks button-rendered artifacts for Chrome download capture
- Limits artifact discovery and download start to 60 seconds, then tracks the exact Chrome download to completion for up to 30 minutes
- Validates response filename, size, and SHA-256 before returning file bytes to the caller
- Supports optional external plugins through one `bridge_plugins.py` entry point

## Setup

1. From this folder, start the current bridge implementation:

   ```bash
   ./run_bridge.sh
   ```

   The launcher creates or repairs `.venv`, installs the package in editable mode, stops any stale process on port 8765, and starts the current server.

2. Launch the isolated Chrome profile when it is not already open:

   ```bash
   ./launch_chrome_power_mode.sh
   ```

3. Open `chrome://extensions`, enable **Developer mode**, and load this project's complete `extension` folder as an unpacked extension.

4. After replacing or updating this folder, click **Reload** for the extension and refresh every ChatGPT tab.

## Python client

```python
import asyncio
from chatgpt_tab_bridge import BridgeClient


async def main() -> None:
    async with BridgeClient() as bridge:
        tabs = await bridge.list_tabs()
        result = await bridge.send_prompt(
            bridge_tab_id=tabs[0]["bridge_tab_id"],
            prompt="Create the requested canonical JSON response artifact.",
            response_mode="json_file",
            response_filename="example_response_abc123.json",
        )
        artifact = result["response_artifact"]
        print(artifact["filename"], artifact["size_bytes"], artifact["sha256"])


asyncio.run(main())
```

## Optional plugins

The server looks only for `bridge_plugins.py` in the directory where the bridge server is started.

- When the file exists, the objects in `PLUGINS` receive bridge events.
- When the file does not exist, no plugins are loaded and no log file is generated.
- Plugin failures are reported to the console but do not change prompt or response-file transport behavior.

The included `bridge_plugins.py` contains one `LoggerPlugin`. It overwrites `bridge.log` when the server starts, so only the current run is retained. Remove `bridge_plugins.py` to disable logging completely.

Artifact-download diagnostics record the exact response control and preview Download button state, synthetic click event sequence and `isTrusted` values, user-activation state, topmost-element checks, preview feedback, the Chrome `automaticDownloads` setting, extension permissions, Chrome download lifecycle events, and sanitized Chrome network-request events for ChatGPT and artifact hosts. These diagnostics intentionally do not change the click method; they are designed to identify whether failure occurs in ChatGPT, Chrome permission handling, network initiation, or the download manager.


## Response artifact lifecycle

JSON response artifacts are opened through the ChatGPT preview, downloaded by the preview Download button, tracked by Chrome download ID until complete, validated by the Tool, and then deleted together with the Chrome download-history entry.
