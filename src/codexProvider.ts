import type {
  LanguageModelV3,
  LanguageModelV3CallOptions,
  LanguageModelV3Content,
  LanguageModelV3StreamPart,
  LanguageModelV3GenerateResult,
  LanguageModelV3StreamResult,
  LanguageModelV3Usage,
  ProviderV3,
} from "@ai-sdk/provider"
import { CodexMCPClient } from "./codexClient"
import { codexLog } from "./logger"
import { StreamState } from "./stream-state"
import type { CodexProviderOptions, JsonValue } from "./types"
import {
  buildConversationPayload,
  decodeExecChunk,
  DEFAULT_REASONING,
  extractTextFromResult,
  mapApprovalPolicy,
  mapSandboxMode,
  sharedPrefixLength,
} from "./utils"

class CodexLanguageModel implements LanguageModelV3 {
  readonly specificationVersion = "v3" as const
  readonly provider = "codex"
  readonly supportedUrls: Record<string, RegExp[]> = { "*/*": [] }

  constructor(public readonly modelId: string) {}

  get modelIdForLogging() {
    return this.modelId
  }

  get modelIdLabel() {
    return this.modelId
  }

  get modelIdValue() {
    return this.modelId
  }

  async doGenerate(options: LanguageModelV3CallOptions): Promise<LanguageModelV3GenerateResult> {
    const { stream } = await this.doStream(options)
    const reader = stream.getReader()
    let text = ""
    let usage: LanguageModelV3Usage = {
      inputTokens: { total: undefined, noCache: undefined, cacheRead: undefined, cacheWrite: undefined },
      outputTokens: { total: undefined, text: undefined, reasoning: undefined },
    }
    let providerMetadata: import("@ai-sdk/provider").SharedV3ProviderMetadata | undefined

    while (true) {
      const { value, done } = await reader.read()
      if (done) break
      switch (value.type) {
        case "text-delta":
          text += value.delta
          break
        case "error":
          throw value.error instanceof Error ? value.error : new Error(String(value.error))
        case "finish":
          usage = value.usage
          providerMetadata = value.providerMetadata
          break
      }
    }

    const content: LanguageModelV3Content[] = text
      ? [{ type: "text", text }]
      : []

    return {
      content,
      finishReason: { unified: "stop", raw: undefined },
      usage,
      providerMetadata,
      warnings: [],
    }
  }

  async doStream(options: LanguageModelV3CallOptions): Promise<LanguageModelV3StreamResult> {
    const providerOptions = this.extractProviderOptions(options)
    const { baseInstructions, userText, assistantText } = buildConversationPayload(options.prompt)
    let prompt = userText || "Please respond to the request."
    if (baseInstructions) {
      prompt = `${baseInstructions}\n\n${prompt}`
    }
    if (assistantText) {
      prompt = `${prompt}\n\nAssistant context:\n${assistantText}`
    }
    const reasoningEffort = providerOptions.reasoningEffort ?? DEFAULT_REASONING
    const cwd = providerOptions.cwd ?? process.cwd()
    const approvalPolicy = mapApprovalPolicy(providerOptions.approvalPolicy)
    const sandbox = mapSandboxMode(providerOptions.sandboxMode)

    const toolArgs: Record<string, JsonValue> = {
      prompt,
      model: providerOptions.model ?? this.modelId,
      cwd,
      "approval-policy": approvalPolicy,
      sandbox,
      "include-plan-tool": false,
    }
    if (reasoningEffort) {
      toolArgs["config"] = { model_reasoning_effort: reasoningEffort }
    }

    const client = new CodexMCPClient(
      providerOptions.binary,
      providerOptions.args,
      {
        cwd: providerOptions.spawnCwd,
        env: providerOptions.env,
      },
      {
        onSend: (payload) => codexLog("rpc.send", { payload }),
        onReceive: (payload) => codexLog("rpc.receive", { payload }),
      },
    )

    const stream = new ReadableStream<LanguageModelV3StreamPart>({
      start: async (controller) => {
        const streamState = new StreamState(controller, client, providerOptions.streamReasoning ?? true)
        let finishedViaNotification = false
        let abortCleanup = () => {}
        let callRequestId: number | undefined
        let lastAgentMessage = ""
        let lastReasoningMessage = ""
        const includeCommandOutput = providerOptions.streamCommandOutput ?? false
        const includeReasoning = providerOptions.streamReasoning ?? true

        const notificationCleanup = client.onNotification((notification) => {
          if (!notification.method.startsWith("codex/event")) return
          const params = (notification.params ?? {}) as Record<string, any>
          const meta = params["_meta"] as Record<string, any> | undefined
          if (callRequestId !== undefined && meta && Object.prototype.hasOwnProperty.call(meta, "requestId")) {
            const rawMetaRequestId = meta.requestId
            const metaRequestId =
              typeof rawMetaRequestId === "number"
                ? rawMetaRequestId
                : typeof rawMetaRequestId === "string"
                ? Number.parseInt(rawMetaRequestId, 10)
                : undefined
            if (Number.isFinite(metaRequestId) && metaRequestId !== callRequestId) {
              return
            }
          }
          const msg = params["msg"] ?? {}
          const type = typeof msg.type === "string" ? msg.type : notification.method.split("/").at(-1) ?? ""

          if (type === "agent_message_delta" && typeof msg.delta === "string" && msg.delta) {
            streamState.pushDelta("text", msg.delta)
            lastAgentMessage = `${lastAgentMessage}${msg.delta}`
            return
          }

          if (type === "agent_message" && typeof msg.message === "string") {
            const message = msg.message
            if (!message) {
              lastAgentMessage = message
              return
            }
            streamState.ensureStreamStart("text")
            const prefixLength = sharedPrefixLength(lastAgentMessage, message)
            const delta = message.slice(prefixLength)
            if (delta) {
              streamState.pushDelta("text", delta)
            }
            lastAgentMessage = message
            return
          }

          if (includeReasoning && type === "agent_reasoning_delta" && typeof msg.delta === "string" && msg.delta) {
            streamState.pushReasoning(msg.delta)
            lastReasoningMessage = `${lastReasoningMessage}${msg.delta}`
            return
          }

          if (includeReasoning && type === "agent_reasoning" && typeof msg.text === "string") {
            const text = msg.text
            if (!text) {
              lastReasoningMessage = text
              return
            }
            if (!streamState.reasoningDeltaSeen) {
              streamState.pushReasoning(text)
            }
            lastReasoningMessage = text
            return
          }

          if (includeReasoning && type === "agent_reasoning_section_break") {
            streamState.pushReasoning("\n")
            lastReasoningMessage = `${lastReasoningMessage}\n`
            return
          }

          if (includeCommandOutput && type === "exec_command_output_delta" && typeof msg.chunk === "string") {
            const decoded = decodeExecChunk(msg.chunk)
            if (decoded) {
              streamState.pushDelta("exec", decoded)
            }
            return
          }

          if (type === "token_count") {
            codexLog("token_count", { hasUsage: !!msg.info?.total_token_usage, inputTokens: msg.info?.total_token_usage?.input_tokens, outputTokens: msg.info?.total_token_usage?.output_tokens })
            streamState.updateTokenUsage(msg.info ?? null, msg.rate_limits ?? null)
            return
          }

          if (type === "session_configured" && msg.model) {
            codexLog("session_configured", { model: msg.model })
            streamState.emitResponseMetadata({ modelId: String(msg.model) })
            return
          }

          if (type === "task_complete") {
            codexLog("task_complete", { duration_ms: msg.duration_ms, time_to_first_token_ms: msg.time_to_first_token_ms })
            if (msg.duration_ms != null || msg.time_to_first_token_ms != null) {
              streamState.updateTaskTiming({
                duration_ms: typeof msg.duration_ms === "number" ? msg.duration_ms : undefined,
                time_to_first_token_ms: typeof msg.time_to_first_token_ms === "number" ? msg.time_to_first_token_ms : undefined,
              })
            }
            finishedViaNotification = true
            streamState.finish("stop")
            return
          }

          if (type === "stream_error" || type === "error") {
            finishedViaNotification = true
            const message = typeof msg.message === "string" ? msg.message : `Codex ${type}`
            streamState.finish("error", new Error(message))
            return
          }

          codexLog("notification.ignored_event", { type })
        })

        const errorCleanup = client.onError((error) => {
          streamState.finish("error", error)
        })

        const exitCleanup = client.onExit((code, signal) => {
          const message = `codex mcp-server exited unexpectedly (${code ?? "null"}${signal ? `, ${signal}` : ""})`
          streamState.finish("error", new Error(message))
        })

        const cleanupAll = () => {
          notificationCleanup()
          errorCleanup()
          exitCleanup()
          abortCleanup()
          abortCleanup = () => {}
          client.close()
        }

        const abortHandler = () => {
          streamState.finish("error", new DOMException("Aborted", "AbortError"))
        }

        if (options.abortSignal) {
          if (options.abortSignal.aborted) {
            abortHandler()
            cleanupAll()
            return
          }
          options.abortSignal.addEventListener("abort", abortHandler, { once: true })
          abortCleanup = () => options.abortSignal?.removeEventListener("abort", abortHandler)
        }

        try {
          await client.initialize(providerOptions.clientInfo)
          const { result } = await client.callCodex(toolArgs, {
            abortSignal: options.abortSignal,
            onNotification: (notification) => codexLog("notification", { notification }),
            onRequestId: (id) => {
              callRequestId = id
            },
          })
          if (finishedViaNotification) {
            return
          }
          if (!result || typeof result !== "object") {
            throw new Error("Codex MCP tool returned an invalid result")
          }
          const text = extractTextFromResult(result)
          const isError =
            !!(result && typeof result === "object" && "isError" in result && (result as Record<string, any>)["isError"])
          if (isError) {
            throw new Error(text || "Codex MCP tool invocation failed")
          }
          if (text) {
            const prefixLength = sharedPrefixLength(lastAgentMessage, text)
            const delta = text.slice(prefixLength)
            if (delta) {
              streamState.pushDelta("text", delta)
            }
          }
          streamState.finish("stop")
        } catch (error) {
          if (finishedViaNotification) {
            codexLog("callCodex.finished_after_notification", {
              error:
                error instanceof Error
                  ? { message: error.message, name: error.name }
                  : { message: String(error) },
            })
          } else {
            streamState.finish("error", error instanceof Error ? error : new Error(String(error)))
          }
        } finally {
          cleanupAll()
        }
      },
      cancel: async () => {
        await client.close()
      },
    })

    return { stream }
  }

  private extractProviderOptions(options: LanguageModelV3CallOptions): CodexProviderOptions {
    const providerSpecific =
      ((options.providerOptions ?? {}) as Record<string, CodexProviderOptions | undefined>)[this.provider] ?? {}
    return providerSpecific
  }
}

export function createCodexProvider(): ProviderV3 {
  return {
    specificationVersion: "v3",
    languageModel: (modelId: string) => new CodexLanguageModel(modelId),
    embeddingModel: (modelId: string) => {
      throw new Error(`Codex provider does not support embedding models (requested model: ${modelId})`)
    },
    imageModel: (modelId: string) => {
      throw new Error(`Codex provider does not support image models (requested model: ${modelId})`)
    },
  }
}
