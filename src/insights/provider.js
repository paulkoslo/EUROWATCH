// Network is opt-in. Never read environment variables until this adapter is selected.
function openAIProvider({
  model,
  apiKey,
  inputUsdPerMillion,
  outputUsdPerMillion,
  maxOutputTokens = 1200,
  fetchImpl = fetch,
}) {
  if (
    !model ||
    !apiKey ||
    ![inputUsdPerMillion, outputUsdPerMillion].every(
      (x) => Number.isFinite(x) && x > 0,
    ) ||
    !Number.isInteger(maxOutputTokens) ||
    maxOutputTokens < 1
  )
    throw new Error(
      "Explicit model, key, positive current prices and output cap required",
    );
  function reserve(request) {
    // UTF-8 byte count plus generous protocol overhead is conservative for text input.
    const maxInputTokens =
      Buffer.byteLength(JSON.stringify(request.messages)) + 4096;
    return (
      (maxInputTokens * inputUsdPerMillion +
        maxOutputTokens * outputUsdPerMillion) /
      1e6
    );
  }
  return {
    kind: "openai",
    reserve,
    async generate(request) {
      const response = await fetchImpl(
        "https://api.openai.com/v1/chat/completions",
        {
          method: "POST",
          signal: AbortSignal.timeout(120000),
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            model,
            messages: request.messages,
            response_format: { type: "json_object" },
            max_completion_tokens: maxOutputTokens,
            store: false,
          }),
        },
      );
      if (!response.ok) throw new Error(`OpenAI HTTP ${response.status}`);
      const body = await response.json();
      const choice = body.choices?.[0];
      const usage = body.usage;
      if (
        !usage ||
        !Number.isInteger(usage.prompt_tokens) ||
        !Number.isInteger(usage.completion_tokens) ||
        usage.prompt_tokens < 0 ||
        usage.completion_tokens < 0
      )
        throw new Error("Missing provider usage");
      const accounting = {
        inputTokens: usage.prompt_tokens,
        outputTokens: usage.completion_tokens,
        costUsd:
          (usage.prompt_tokens * inputUsdPerMillion +
            usage.completion_tokens * outputUsdPerMillion) /
          1e6,
        providerResponseId: body.id,
        actualModel: body.model,
      };
      try {
        if (choice?.finish_reason !== "stop" || choice.message?.refusal)
          throw new Error("Provider refused or did not finish");
        return {
          candidate: JSON.parse(choice.message.content),
          usage: accounting,
        };
      } catch (error) {
        throw Object.assign(
          new Error("Provider output could not be used", { cause: error }),
          { usage: accounting },
        );
      }
    },
  };
}

function offlineProvider(responses = {}) {
  return {
    kind: "offline",
    reserve: () => 0,
    async generate(request) {
      if (!Object.hasOwn(responses, request.key))
        throw new Error("No offline response for input/version key");
      return {
        candidate: responses[request.key],
        usage: {
          inputTokens: 0,
          outputTokens: 0,
          costUsd: 0,
          provider: "offline",
        },
      };
    },
  };
}
module.exports = { openAIProvider, offlineProvider };
