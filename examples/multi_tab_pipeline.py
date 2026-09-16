from __future__ import annotations

import asyncio
from chatgpt_tab_bridge import BridgeClient


async def main() -> None:
    async with BridgeClient() as bridge:
        tabs = await bridge.list_tabs()
        if len(tabs) < 2:
            raise RuntimeError(
                "Open at least two ChatGPT conversation tabs and refresh them."
            )

        print("Available tabs:")
        for index, tab in enumerate(tabs, start=1):
            print(
                f"{index}. {tab['bridge_tab_id']} | "
                f"{tab.get('conversation_id')} | {tab.get('title')}"
            )

        result = await bridge.pipe(
            source_tab_id=tabs[0]["bridge_tab_id"],
            source_prompt=(
                "Explain one important risk of browser automation in three sentences."
            ),
            target_tab_id=tabs[1]["bridge_tab_id"],
            target_prompt_template=(
                "Critique and improve this answer:\n\n{{output}}"
            ),
            absolute_timeout_seconds=600,
        )

        print("\nSource output:\n")
        print(result["source"]["text"])
        print("\nTarget output:\n")
        print(result["target"]["text"])


if __name__ == "__main__":
    asyncio.run(main())
