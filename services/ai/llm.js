const trimSlash = (value) => String(value || "").replace(/\/+$/, "");

const env = process.env;

export const getPlannerLlmConfig = () => {
  const apiKey = String(env.LLM_API_KEY || "").trim();
  const model = String(env.LLM_MODEL || "").trim();
  const baseUrl = trimSlash(env.LLM_API_BASE_URL || "https://api.openai.com/v1");
  const provider = String(env.LLM_PROVIDER || "openai-compatible").trim() || "openai-compatible";
  const styleProfile = String(env.LLM_STYLE_PROFILE || "journeypro_trip_planner_v1").trim();
  const temperature = Number(env.LLM_TEMPERATURE || 0.45);
  const maxTokens = Math.max(300, Math.min(Number(env.LLM_MAX_TOKENS || 520), 1800));
  return {
    configured: !!apiKey && !!model,
    provider,
    apiKey,
    model,
    baseUrl,
    styleProfile,
    temperature: Number.isFinite(temperature) ? temperature : 0.45,
    maxTokens,
  };
};

const buildSystemPrompt = () =>
  [
    "You are JourneyPro AI, a London-only travel planning assistant.",
    "Your job is to explain and organize route-aware London recommendations using the provided candidate POIs and community evidence.",
    "Never invent cities, POIs, community opinions, or operational facts that are not supported by the supplied context.",
    "If the supplied context is thin, say so plainly.",
    "Keep the answer practical, direct, and product-grade.",
    "Use short paragraphs and short bullet lists when useful.",
    "Reference community evidence naturally, for example: community posts suggest, comments repeatedly mention, users often note.",
    "Do not output JSON. Output plain text only.",
  ].join(" ");

const buildUserPrompt = ({
  prompt,
  itinerary,
  promptContext,
  interestWeight,
  exploreWeight,
}) => {
  const itineraryLines = Array.isArray(itinerary?.segments)
    ? itinerary.segments.flatMap((segment) => {
        const lines = [`${segment.label}: ${segment.summary}`];
        (segment.stops || []).forEach((stop) => {
          lines.push(`- ${stop.order}. ${stop.name} (${stop.category || "poi"}) detour ${Math.max(1, Math.round(Number(stop.detour_duration_s || 0) / 60))} min`);
        });
        return lines;
      })
    : [];

  return [
    `User request: ${String(prompt || "").trim() || "N/A"}`,
    `Ranking controls: interest ${Math.round((Number(interestWeight) || 0) * 100)}%, distance ${100 - Math.round((Number(interestWeight) || 0) * 100)}%, explore ${Math.round((Number(exploreWeight) || 0) * 100)}%.`,
    "",
    "Structured itinerary draft:",
    ...(itineraryLines.length ? itineraryLines : ["No itinerary segments available."]),
    "",
    "Retrieval context:",
    promptContext || "No retrieval context available.",
    "",
    "Write a useful London itinerary answer for the user. Mention the strongest route anchors and 2-4 community-backed reasons.",
  ].join("\n");
};

const parseSseBlocks = (buffer, onEvent) => {
  let normalized = String(buffer || "");
  let boundary = normalized.indexOf("\n\n");
  while (boundary >= 0) {
    const block = normalized.slice(0, boundary).trim();
    normalized = normalized.slice(boundary + 2);
    if (block) onEvent(block);
    boundary = normalized.indexOf("\n\n");
  }
  return normalized;
};

const extractContentFromBlock = (block) => {
  const lines = String(block || "").split("\n");
  const dataLines = lines.filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trimStart());
  if (!dataLines.length) return { done: false, token: "" };
  const raw = dataLines.join("\n");
  if (raw === "[DONE]") return { done: true, token: "" };
  try {
    const parsed = JSON.parse(raw);
    const token = String(parsed?.choices?.[0]?.delta?.content || "");
    return { done: false, token };
  } catch {
    return { done: false, token: "" };
  }
};

export const streamPlannerNarrativeFromLlm = async ({
  prompt,
  itinerary,
  promptContext,
  interestWeight,
  exploreWeight,
  onToken,
}) => {
  const cfg = getPlannerLlmConfig();
  if (!cfg.configured) {
    return {
      ok: false,
      mode: "fallback",
      provider: cfg.provider,
      model: cfg.model || "",
      styleProfile: cfg.styleProfile,
      reason: "missing_llm_config",
      text: "",
    };
  }

  const response = await fetch(`${cfg.baseUrl}/chat/completions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${cfg.apiKey}`,
    },
    body: JSON.stringify({
      model: cfg.model,
      temperature: cfg.temperature,
      max_tokens: cfg.maxTokens,
      stream: true,
      messages: [
        { role: "system", content: buildSystemPrompt() },
        {
          role: "user",
          content: buildUserPrompt({
            prompt,
            itinerary,
            promptContext,
            interestWeight,
            exploreWeight,
          }),
        },
      ],
    }),
  });

  if (!response.ok || !response.body) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `LLM request failed: ${response.status}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  let fullText = "";

  while (true) {
    // eslint-disable-next-line no-await-in-loop
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    buffer = parseSseBlocks(buffer, (block) => {
      const parsed = extractContentFromBlock(block);
      if (parsed.done || !parsed.token) return;
      fullText += parsed.token;
      onToken(parsed.token);
    });
  }

  return {
    ok: true,
    mode: "external",
    provider: cfg.provider,
    model: cfg.model,
    styleProfile: cfg.styleProfile,
    text: fullText,
  };
};
