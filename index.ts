import type { Plugin, Hooks, Config } from "@opencode-ai/plugin"
import { createCodexProvider } from "./src/codexProvider"
import { fileURLToPath } from "node:url"

const PACKAGE_NAME = "opencode-codex-provider"

export { createCodexProvider }

export const CodexProviderPlugin: Plugin = async () => {
  const providerNpm = PACKAGE_NAME
  const defaultModels = {
    "gpt-5.5": {
      name: "GPT-5.5",
      reasoning: true,
      limit: { context: 258400, output: 128000 },
    },
    "gpt-5.4": {
      name: "GPT-5.4",
      reasoning: true,
      limit: { context: 128000, output: 16384 },
    },
    "gpt-5.2": {
      name: "GPT-5.2",
      reasoning: true,
      limit: { context: 128000, output: 16384 },
    },
  } satisfies Record<string, Record<string, unknown>>

  return {
    async config(input: Config) {
      const config = input as Record<string, unknown>
      const providers = (config.provider ?? {}) as Record<string, Record<string, unknown>>
      config.provider = providers
      const existing = (providers["codex"] ?? {}) as Record<string, unknown>
      const options = {
        ...((existing.options as Record<string, unknown>) ?? {}),
      } as Record<string, unknown>
      if (!options.providerFactory) {
        const isFileProtocol = import.meta.url.startsWith("file://")
        if (isFileProtocol) {
          options.providerFactory = fileURLToPath(new URL("./provider.ts", import.meta.url))
        } else {
          options.providerFactory = "opencode-codex-provider/provider"
        }
      }
      const existingModels = {
        ...defaultModels,
        ...(existing.models ?? {}),
      }
      providers["codex"] = {
        ...existing,
        npm: existing.npm ?? providerNpm,
        name: existing.name ?? "Codex CLI",
        models: existingModels,
        options,
      }
    },
    "chat.params": async (_input: { sessionID: string; agent: string; model: unknown; provider: { source: string; info: { id: string }; options: Record<string, unknown> }; message: unknown }, output: { temperature: number; topP: number; topK: number; maxOutputTokens: number | undefined; options: Record<string, unknown> }) => {
      if (_input.provider?.info?.id === "codex") {
        output.maxOutputTokens = undefined
      }
    },
  } satisfies Hooks
}
