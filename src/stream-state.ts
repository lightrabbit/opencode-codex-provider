import type { LanguageModelV3StreamPart, LanguageModelV3FinishReason, LanguageModelV3Usage, SharedV3ProviderMetadata } from "@ai-sdk/provider";
import { CodexMCPClient } from "./codexClient";

type StreamType = "text" | "exec" | "reasoning";

interface CodexTokenUsage {
  input_tokens: number;
  cached_input_tokens: number;
  output_tokens: number;
  reasoning_output_tokens: number;
  total_tokens: number;
}

interface CodexRateLimits {
  plan_type?: string;
  primary?: { used_percent: number; window_minutes: number; resets_at: number };
  secondary?: { used_percent: number; window_minutes: number; resets_at: number };
}

export class StreamState {
  private finished = false;
  private streams: Map<StreamType, boolean> = new Map();
  private lastReasoningChunk = "";
  private lastReasoningNormalized = "";
  public reasoningDeltaSeen = false;

  private tokenUsage: CodexTokenUsage | undefined;
  private rateLimits: CodexRateLimits | undefined;
  private taskTiming: { duration_ms?: number; time_to_first_token_ms?: number } = {};

  constructor(
    private readonly controller: ReadableStreamDefaultController<LanguageModelV3StreamPart>,
    private readonly client: CodexMCPClient,
    private readonly includeReasoning: boolean,
  ) {}

  public updateTokenUsage(info: { total_token_usage?: CodexTokenUsage; last_token_usage?: CodexTokenUsage } | null, rateLimits?: CodexRateLimits | null) {
    if (info) {
      const usage = info.total_token_usage ?? info.last_token_usage;
      if (usage) {
        this.tokenUsage = usage;
      }
    }
    if (rateLimits) {
      this.rateLimits = rateLimits;
    }
  }

  public updateTaskTiming(timing: { duration_ms?: number; time_to_first_token_ms?: number }) {
    this.taskTiming = { ...this.taskTiming, ...timing };
  }

  private buildUsage(): LanguageModelV3Usage {
    const u = this.tokenUsage;
    if (!u) {
      return {
        inputTokens: { total: undefined, noCache: undefined, cacheRead: undefined, cacheWrite: undefined },
        outputTokens: { total: undefined, text: undefined, reasoning: undefined },
      };
    }
    const cached = u.cached_input_tokens ?? 0;
    const nonCached = Math.max(0, u.input_tokens - cached);
    return {
      inputTokens: { total: u.input_tokens, noCache: nonCached, cacheRead: Math.min(u.input_tokens, cached), cacheWrite: undefined },
      outputTokens: { total: u.output_tokens, text: undefined, reasoning: u.reasoning_output_tokens ?? undefined },
    };
  }

  private buildProviderMetadata(): SharedV3ProviderMetadata | undefined {
    const meta: Record<string, unknown> = {};
    if (this.taskTiming.duration_ms !== undefined || this.taskTiming.time_to_first_token_ms !== undefined) {
      meta.timing = { ...this.taskTiming };
    }
    if (this.rateLimits) {
      meta.rateLimits = JSON.parse(JSON.stringify(this.rateLimits));
    }
    if (Object.keys(meta).length === 0) return undefined;
    return { codex: JSON.parse(JSON.stringify(meta)) } as SharedV3ProviderMetadata;
  }

  public finish(reason: LanguageModelV3FinishReason["unified"], error?: Error) {
    if (this.finished) return;
    this.finished = true;

    if (error) {
      this.controller.enqueue({ type: "error", error });
    }

    this.streams.forEach((_started, type) => {
      this.endStream(type);
    });

    this.controller.enqueue({
      type: "finish",
      finishReason: { unified: reason, raw: undefined },
      usage: this.buildUsage(),
      providerMetadata: this.buildProviderMetadata(),
    });

    this.controller.close();
    this.client.close();
  }

  public emitResponseMetadata(meta: { id?: string; modelId?: string }) {
    const event: LanguageModelV3StreamPart = { type: "response-metadata" };
    // LanguageModelV3ResponseMetadata fields are mixed in directly
    const enriched = { ...event, ...meta };
    this.controller.enqueue(enriched as LanguageModelV3StreamPart);
  }

  public ensureStreamStart(type: StreamType) {
    if (!this.streams.has(type)) {
      this.streams.set(type, true);
      this.controller.enqueue({ type: "text-start", id: `codex-${type}` });
    }
  }

  private endStream(type: StreamType) {
    if (this.streams.has(type)) {
      this.controller.enqueue({ type: "text-end", id: `codex-${type}` });
      this.streams.delete(type);
    }
  }

  private normalizeReasoning(value: string) {
    return value.trim().replace(/\*/g, "").replace(/\s+/g, " ");
  }

  public pushReasoning(chunk: string) {
    if (!chunk || !this.includeReasoning) return;
    const normalized = this.normalizeReasoning(chunk);
    if (!normalized && chunk === "\n" && this.lastReasoningChunk === "\n") {
      return;
    }
    if (normalized && normalized === this.lastReasoningNormalized) {
      return;
    }
    if (!normalized && this.lastReasoningChunk === chunk) {
      return;
    }
    this.reasoningDeltaSeen = true;
    this.pushDelta("reasoning", chunk);
    this.lastReasoningChunk = chunk;
    if (normalized) {
      this.lastReasoningNormalized = normalized;
    }
  }

  public pushDelta(type: StreamType, delta: string) {
    if (!delta) return;
    if (type === "reasoning" && !this.includeReasoning) return;

    this.ensureStreamStart(type);
    this.controller.enqueue({ type: "text-delta", id: `codex-${type}`, delta });
  }
}
