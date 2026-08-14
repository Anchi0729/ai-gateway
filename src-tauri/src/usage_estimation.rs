use serde_json::Value;
use tiktoken_rs::{bpe_for_model, cl100k_base_singleton, o200k_base_singleton};

pub const MAX_ESTIMATION_TEXT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct RequestTokenEstimate {
    pub input_tokens: u64,
    pub method: String,
}

#[derive(Default)]
pub struct OutputTextCapture {
    text: String,
    overflowed: bool,
    saw_output: bool,
}

impl OutputTextCapture {
    pub fn observe_event(&mut self, value: &Value) {
        let event_type = value.get("type").and_then(Value::as_str);
        if matches!(
            event_type,
            Some(
                "response.output_text.delta"
                    | "response.refusal.delta"
                    | "response.function_call_arguments.delta"
                    | "response.reasoning_summary_text.delta"
                    | "response.reasoning_text.delta"
            )
        ) {
            self.append(value.get("delta").and_then(Value::as_str));
        }

        // Chat Completions 的流式内容位于 choices[].delta，不能只依赖 Responses 事件类型。
        if let Some(choices) = value.get("choices").and_then(Value::as_array) {
            for choice in choices {
                let Some(delta) = choice.get("delta") else {
                    continue;
                };
                self.append(delta.get("content").and_then(Value::as_str));
                self.append(delta.get("refusal").and_then(Value::as_str));
                if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                    for call in tool_calls {
                        self.append(call.pointer("/function/name").and_then(Value::as_str));
                        self.append(call.pointer("/function/arguments").and_then(Value::as_str));
                    }
                }
            }
        }
    }

    pub fn observe_json_response(&mut self, value: &Value) {
        let root = value.get("response").unwrap_or(value);
        if let Some(output) = root.get("output") {
            collect_content_text(output, self);
        }
        if let Some(choices) = root.get("choices").and_then(Value::as_array) {
            for choice in choices {
                if let Some(message) = choice.get("message") {
                    collect_content_text(message, self);
                }
            }
        }
    }

    pub fn estimated_tokens(&self, model: Option<&str>) -> Option<u64> {
        if self.overflowed {
            return None;
        }
        Some(count_tokens(&self.text, model) as u64)
    }

    pub fn saw_output(&self) -> bool {
        self.saw_output
    }

    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    fn append(&mut self, value: Option<&str>) {
        let Some(value) = value.filter(|value| !value.is_empty()) else {
            return;
        };
        self.saw_output = true;
        if self.overflowed {
            return;
        }
        let separator = usize::from(!self.text.is_empty());
        if self
            .text
            .len()
            .saturating_add(separator)
            .saturating_add(value.len())
            > MAX_ESTIMATION_TEXT_BYTES
        {
            self.text.clear();
            self.overflowed = true;
            return;
        }
        if separator != 0 {
            self.text.push('\n');
        }
        self.text.push_str(value);
    }
}

pub fn estimate_request(
    body: &[u8],
    request_path: &str,
    model: Option<&str>,
) -> Option<RequestTokenEstimate> {
    if !is_generation_path(request_path) || body.len() > MAX_ESTIMATION_TEXT_BYTES {
        return None;
    }
    let root = serde_json::from_slice::<Value>(body).ok()?;
    let mut capture = OutputTextCapture::default();

    capture.append(root.get("instructions").and_then(Value::as_str));
    if let Some(input) = root.get("input") {
        collect_content_text(input, &mut capture);
    }
    if let Some(messages) = root.get("messages") {
        collect_content_text(messages, &mut capture);
    }
    if let Some(tools) = root.get("tools") {
        collect_tool_definitions(tools, &mut capture);
    }
    if let Some(format) = root
        .pointer("/text/format")
        .or_else(|| root.get("response_format"))
    {
        append_json(format, &mut capture);
    }
    if capture.overflowed() {
        return None;
    }

    Some(RequestTokenEstimate {
        input_tokens: capture.estimated_tokens(model)?,
        method: encoding_name(model).to_string(),
    })
}

fn is_generation_path(path: &str) -> bool {
    let normalized = path.trim_end_matches('/').to_ascii_lowercase();
    normalized.ends_with("/responses") || normalized.ends_with("/chat/completions")
}

fn collect_tool_definitions(value: &Value, capture: &mut OutputTextCapture) {
    let Some(tools) = value.as_array() else {
        return;
    };
    for tool in tools {
        let function = tool.get("function").unwrap_or(tool);
        capture.append(function.get("name").and_then(Value::as_str));
        capture.append(function.get("description").and_then(Value::as_str));
        if let Some(parameters) = function.get("parameters") {
            append_json(parameters, capture);
        }
    }
}

fn collect_content_text(value: &Value, capture: &mut OutputTextCapture) {
    match value {
        Value::String(text) => capture.append(Some(text)),
        Value::Array(items) => {
            for item in items {
                collect_content_text(item, capture);
            }
        }
        Value::Object(object) => {
            for (key, child) in object {
                // 图片、音频和 URL 无法用文本 tokenizer 得到可靠计费值，必须显式跳过。
                if matches!(
                    key.as_str(),
                    "image_url" | "input_image" | "audio" | "input_audio" | "file_data"
                ) {
                    continue;
                }
                if matches!(key.as_str(), "parameters" | "schema") {
                    append_json(child, capture);
                } else if matches!(
                    key.as_str(),
                    "text"
                        | "content"
                        | "name"
                        | "description"
                        | "arguments"
                        | "output"
                        | "refusal"
                        | "summary"
                        | "role"
                        | "tool_calls"
                        | "function_call"
                ) {
                    collect_content_text(child, capture);
                }
            }
        }
        _ => {}
    }
}

fn append_json(value: &Value, capture: &mut OutputTextCapture) {
    if let Ok(text) = serde_json::to_string(value) {
        capture.append(Some(&text));
    }
}

fn encoding_name(model: Option<&str>) -> &'static str {
    let normalized = model.unwrap_or_default().trim().to_ascii_lowercase();
    if normalized.starts_with("gpt-5")
        || normalized.starts_with("gpt-4.1")
        || normalized.starts_with("gpt-4o")
        || normalized.starts_with("o1")
        || normalized.starts_with("o3")
        || normalized.starts_with("o4")
    {
        "tiktoken:o200k_base"
    } else {
        "tiktoken:cl100k_base"
    }
}

fn count_tokens(text: &str, model: Option<&str>) -> usize {
    if text.is_empty() {
        return 0;
    }
    if let Some(model) = model {
        if let Ok(encoding) = bpe_for_model(model) {
            return encoding.encode_ordinary(text).len();
        }
    }
    if encoding_name(model) == "tiktoken:o200k_base" {
        o200k_base_singleton().encode_ordinary(text).len()
    } else {
        cl100k_base_singleton().encode_ordinary(text).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimates_responses_request_and_stream_output() {
        let body = br#"{"model":"gpt-5","instructions":"be concise","input":[{"role":"user","content":[{"type":"input_text","text":"hello"}]}],"tools":[{"type":"function","name":"lookup","description":"find item","parameters":{"type":"object"}}]}"#;
        let estimate = estimate_request(body, "/v1/responses", Some("gpt-5")).unwrap();
        assert!(estimate.input_tokens > 0);
        assert_eq!(estimate.method, "tiktoken:o200k_base");

        let mut output = OutputTextCapture::default();
        output.observe_event(&serde_json::json!({
            "type": "response.output_text.delta",
            "delta": "hello world"
        }));
        assert!(output.saw_output());
        assert!(output.estimated_tokens(Some("gpt-5")).unwrap() > 0);
    }

    #[test]
    fn ignores_non_generation_and_multimodal_payloads() {
        assert!(estimate_request(b"{}", "/v1/models", None).is_none());
        let body = br#"{"model":"gpt-5","input":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}"#;
        let estimate = estimate_request(body, "/v1/responses", Some("gpt-5")).unwrap();
        assert_eq!(estimate.input_tokens, 0);
    }
}
