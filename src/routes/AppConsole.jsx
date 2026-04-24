import React, { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { apiFetch, uploadFile, chat, chatStream, transcribeAudio, requestFounderHandoff, getRealtimeClientSecret, startRealtimeSession, startSummitSession, postRealtimeEventsBatch, endRealtimeSession, getRealtimeSession, getSummitSessionScore, submitSummitSessionReview, downloadRealtimeAta as downloadRealtimeAtaFile, guardRealtimeTranscript, getOrionSquadHealth, getOrionSquadPreview, getAgentCapabilities } from "../ui/api.js";
import { clearSession, getTenant, getToken, getUser, isAdmin, isApproved, setSession, logout } from "../lib/auth.js";
import { ORKIO_VOICES, coerceVoiceId } from "../lib/voices.js";
import TermsModal from "../ui/TermsModal.jsx";
import PWAInstallPrompt from "../components/PWAInstallPrompt.jsx";
import OnboardingModal from "../components/OnboardingModal.jsx";
import { startSessionHeartbeat } from "../lib/sessionHeartbeat.js";

const ORKIO_ENV = (typeof window !== "undefined" && window.__ORKIO_ENV__) ? window.__ORKIO_ENV__ : {};
const SUMMIT_VOICE_MODE = ((ORKIO_ENV.VITE_SUMMIT_VOICE_MODE || import.meta.env.VITE_SUMMIT_VOICE_MODE || "realtime").trim().toLowerCase() === "stt_tts")
  ? "stt_tts"
  : "realtime";
const SPEECH_RECOGNITION_LANG = ((ORKIO_ENV.VITE_SPEECH_RECOGNITION_LANG || import.meta.env.VITE_SPEECH_RECOGNITION_LANG || "pt-BR").trim() || "pt-BR");


const ORKIO_CHAT_STREAM_PRIMARY = ((ORKIO_ENV.VITE_CHAT_STREAM_PRIMARY || import.meta.env.VITE_CHAT_STREAM_PRIMARY || "true").toString().trim().toLowerCase() !== "false");

const WALLET_UI_ENABLED = false;

class StreamSemanticError extends Error {
  constructor(payload = {}) {
    super(payload?.message || payload?.error || "Stream semantic error");
    this.name = "StreamSemanticError";
    this.payload = payload || {};
    this.status = payload?.code || "STREAM_ERROR";
  }
}



async function consumeChatStream(
  response,
  {
    onStatus,
    onError,
    onDone,
    onChunk,
    onAgentDone,
    onKeepalive,
    onExecution,
  } = {}
) {
  const reader = response?.body?.getReader?.();
  if (!reader) return { thread_id: null, trace_id: null, event_count: 0, used_stream: false };
  const decoder = new TextDecoder();
  let buf = "";
  let lastThreadId = null;
  let lastTraceId = null;
  let eventCount = 0;
  let donePayload = null;
  let draftText = "";

  const flushBlock = (block) => {
    const lines = String(block || "").split(/\r?\n/).filter(Boolean);
    if (!lines.length) return;
    let ev = "message";
    const dataLines = [];
    for (const line of lines) {
      if (line.startsWith("event:")) ev = line.slice(6).trim();
      else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
    }
    let payload = {};
    if (dataLines.length) {
      try { payload = JSON.parse(dataLines.join("\n")); } catch { payload = { raw: dataLines.join("\n") }; }
    }
    if (payload?.thread_id) lastThreadId = payload.thread_id;
    if (payload?.trace_id) lastTraceId = payload.trace_id;
    eventCount += 1;
    if (ev === "status") onStatus?.(payload);
    if (ev === "execution") onExecution?.(payload);
    if (ev === "chunk") {
      const delta = String(payload?.delta ?? payload?.content ?? "");
      if (delta) draftText += delta;
      onChunk?.(payload, draftText);
    }
    if (ev === "agent_done") onAgentDone?.(payload, draftText);
    if (ev === "keepalive") onKeepalive?.(payload);
    if (ev === "error") {
      onError?.(payload);
      const agentScopedRecoverableError = !!payload?.agent_id && payload?.code !== "SERVER_BUSY";
      if (!agentScopedRecoverableError) {
        throw new StreamSemanticError(payload);
      }
    }
    if (ev === "done") {
      donePayload = payload || {};
      onDone?.(payload);
    }
  };

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    const parts = buf.split(/\r?\n\r?\n/);
    buf = parts.pop() || "";
    for (const part of parts) flushBlock(part);
  }
  if (buf.trim()) flushBlock(buf);
  return {
    thread_id: donePayload?.thread_id || lastThreadId,
    trace_id: donePayload?.trace_id || lastTraceId,
    event_count: eventCount,
    used_stream: true,
    runtime_hints: donePayload?.runtime_hints || null,
    execution_cursor: donePayload?.runtime_hints?.routing?.execution_cursor || null,
    execution_lifecycle: donePayload?.runtime_hints?.routing?.execution_lifecycle || null,
    routing_source: donePayload?.runtime_hints?.routing?.routing_source || null,
    route_applied: !!donePayload?.runtime_hints?.routing?.route_applied,
    done_payload: donePayload,
    draft_text: draftText,
  };
}

const REALTIME_IDLE_FOLLOWUP_ENABLED = ((ORKIO_ENV.VITE_REALTIME_IDLE_FOLLOWUP_ENABLED || import.meta.env.VITE_REALTIME_IDLE_FOLLOWUP_ENABLED || "true").toString().trim().toLowerCase() !== "false");
const REALTIME_IDLE_FOLLOWUP_MS = Math.max(5000, Number(ORKIO_ENV.VITE_REALTIME_IDLE_FOLLOWUP_MS || import.meta.env.VITE_REALTIME_IDLE_FOLLOWUP_MS || 10000) || 10000);
const REALTIME_REARM_AFTER_ASSISTANT_MS = Math.max(800, Number(ORKIO_ENV.VITE_REALTIME_RESTART_AFTER_TTS_MS || import.meta.env.VITE_REALTIME_RESTART_AFTER_TTS_MS || 1800) || 1800);

const REALTIME_AUTO_RESPONSE_ENABLED = ((ORKIO_ENV.VITE_REALTIME_AUTO_RESPONSE_ENABLED || import.meta.env.VITE_REALTIME_AUTO_RESPONSE_ENABLED || "true").toString().trim().toLowerCase() !== "false");
const REALTIME_SERVER_VAD_SILENCE_MS = Math.max(250, Number(ORKIO_ENV.VITE_REALTIME_VAD_SILENCE_MS || import.meta.env.VITE_REALTIME_VAD_SILENCE_MS || 900) || 900);
const REALTIME_SERVER_VAD_PREFIX_MS = Math.max(0, Number(ORKIO_ENV.VITE_REALTIME_VAD_HOLD_MS || import.meta.env.VITE_REALTIME_VAD_HOLD_MS || 300) || 300);


function resolveRealtimeIdleDisplayName(userObj) {
  const raw = (userObj?.name || userObj?.full_name || "").toString().trim();
  if (!raw) return "";
  const first = raw.split(/\s+/).filter(Boolean)[0] || raw;
  return first.replace(/[^\p{L}\p{N}]/gu, "") || "";
}


function normalizeAgentVoiceId(raw, fallback = "cedar") {
  const voice = String(raw || "").trim().toLowerCase();
  const aliases = {
    marine: "marin",
    marin: "marin",
    nova: "cedar",
    onyx: "echo",
    fable: "sage",
  };
  const valid = new Set(["alloy","ash","ballad","cedar","coral","echo","fable","marin","nova","onyx","sage","shimmer","verse"]);
  const normalized = aliases[voice] || voice;
  return valid.has(normalized) ? normalized : (String(fallback || "cedar").trim().toLowerCase() || "cedar");
}

function resolveAgentVoice(agentLike) {
  const name = String(agentLike?.agent_name || agentLike?.name || "").trim().toLowerCase();
  const dbVoice = String(agentLike?.voice_id || "").trim();
  const envMap = {
    orkio: (window.__ORKIO_ENV__?.VITE_ORKIO_VOICE_ID || import.meta.env.VITE_ORKIO_VOICE_ID || "").trim(),
    chris: (window.__ORKIO_ENV__?.VITE_CHRIS_VOICE_ID || import.meta.env.VITE_CHRIS_VOICE_ID || "").trim(),
    orion: (window.__ORKIO_ENV__?.VITE_ORION_VOICE_ID || import.meta.env.VITE_ORION_VOICE_ID || "").trim(),
  };
  const defaultVoice = (window.__ORKIO_ENV__?.VITE_REALTIME_VOICE || import.meta.env.VITE_REALTIME_VOICE || "cedar").trim() || "cedar";
  return normalizeAgentVoiceId(dbVoice || envMap[name] || defaultVoice, defaultVoice);
}

function logRealtimeStep(step, payload = undefined) {
  try {
    const stamp = new Date().toISOString();
    if (payload === undefined) {
      console.log(`[Realtime][${stamp}] ${step}`);
    } else {
      console.log(`[Realtime][${stamp}] ${step}`, payload);
    }
  } catch {}
}


function hasAdminAccess(userObj) {
  if (!userObj) return false;
  const role = String(userObj?.role || "").trim().toLowerCase();
  return !!(
    role === "admin"
    || role === "owner"
    || role === "superadmin"
    || userObj?.is_admin === true
    || userObj?.admin === true
  );
}



// Icons (inline SVG)
const IconPlus = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
    <line x1="12" y1="5" x2="12" y2="19" />
    <line x1="5" y1="12" x2="19" y2="12" />
  </svg>
);

const IconSend = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
    <line x1="22" y1="2" x2="11" y2="13" />
    <polygon points="22 2 15 22 11 13 2 9 22 2" />
  </svg>
);

const IconPaperclip = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
    <path d="M21.44 11.05l-8.49 8.49a5 5 0 0 1-7.07-7.07l9.19-9.19a3.5 3.5 0 0 1 4.95 4.95l-9.19 9.19a2 2 0 0 1-2.83-2.83l8.49-8.49" />
  </svg>
);

const IconEdit = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 20h9" />
    <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z" />
  </svg>
);

const IconLogout = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
    <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
    <polyline points="16 17 21 12 16 7" />
    <line x1="21" y1="12" x2="9" y2="12" />
  </svg>
);

const IconSettings = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
    <circle cx="12" cy="12" r="3" />
    <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.6 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.6a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
  </svg>
);

const IconMessage = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
    <path d="M21 15a4 4 0 0 1-4 4H7l-4 4V7a4 4 0 0 1 4-4h10a4 4 0 0 1 4 4z" />
  </svg>
);

const IconTrash = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polyline points="3 6 5 6 21 6" />
    <path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6" />
    <path d="M10 11v6" />
    <path d="M14 11v6" />
    <path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2" />
  </svg>
);


function tryParseEvent(content) {
  try {
    if (!content || typeof content !== "string") return null;
    const idx = content.indexOf("ORKIO_EVENT:");
    if (idx < 0) return null;
    const jsonStr = content.slice(idx + "ORKIO_EVENT:".length);
    return JSON.parse(jsonStr);
  } catch {
    return null;
  }
}

function stripEventMarker(content) {
  if (!content || typeof content !== "string") return content;
  const idx = content.indexOf("ORKIO_EVENT:");
  if (idx < 0) return content;
  return content.slice(0, idx).trim();
}

function formatTs(ts) {
  try {
    if (!ts) return "";
    return formatDateTime(ts);
  } catch {
    return "";
  }
}

function formatDateTime(ts) {
  if (ts === null || ts === undefined || ts === "") return "";
  try {
    let ms;
    if (typeof ts === "number") {
      // If value looks like milliseconds (13 digits), keep; if seconds (10 digits), convert.
      ms = ts > 10_000_000_000 ? ts : ts * 1000;
    } else {
      // ISO string or numeric string
      const n = Number(ts);
      if (!Number.isNaN(n) && Number.isFinite(n)) {
        ms = n > 10_000_000_000 ? n : n * 1000;
      } else {
        ms = new Date(ts).getTime();
      }
    }
    const d = new Date(ms);
    return new Intl.DateTimeFormat("pt-BR", {
      timeZone: "America/Sao_Paulo",
      hour12: false,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }).format(d);
  } catch {
    return "";
  }
}

function fmtUsd(value) {
  const n = Number(value || 0);
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: n % 1 === 0 ? 0 : 2,
    }).format(n);
  } catch {
    return `$${n.toFixed(2)}`;
  }
}


function normalizeWalletErrorPayload(err) {
  const payload = err?.payload || err?.data || null;
  if (payload && typeof payload === "object" && !Array.isArray(payload)) {
    if (payload.code === "WALLET_INSUFFICIENT_BALANCE") return payload;
    if (payload.detail && typeof payload.detail === "object" && payload.detail.code === "WALLET_INSUFFICIENT_BALANCE") {
      return payload.detail;
    }
  }
  if (typeof payload === "string") {
    try {
      const parsed = JSON.parse(payload);
      if (parsed?.code === "WALLET_INSUFFICIENT_BALANCE") return parsed;
      if (parsed?.detail?.code === "WALLET_INSUFFICIENT_BALANCE") return parsed.detail;
    } catch {}
  }
  const message = String(err?.message || "").trim();
  if (message) {
    try {
      const parsed = JSON.parse(message);
      if (parsed?.code === "WALLET_INSUFFICIENT_BALANCE") return parsed;
      if (parsed?.detail?.code === "WALLET_INSUFFICIENT_BALANCE") return parsed.detail;
    } catch {}
  }
  return null;
}

function buildWalletBlockedMessage(detail) {
  if (!detail) return "Sua wallet não possui saldo suficiente para continuar.";
  const current = Number(detail.current_balance_usd || detail.wallet?.balance_usd || 0);
  const required = Number(detail.required_usd || 0);
  const missing = Number(
    detail.missing_usd != null
      ? detail.missing_usd
      : Math.max(required - current, 0)
  );
  if (missing > 0.0001) {
    return `Saldo insuficiente na wallet. Faltam ${fmtUsd(missing)} para continuar.`;
  }
  if (required > 0.0001) {
    return `Saldo insuficiente na wallet. É recomendado manter pelo menos ${fmtUsd(required)} disponível.`;
  }
  return "Sua wallet não possui saldo suficiente para continuar.";
}

function isWalletBlockedError(err) {
  if (!err) return false;
  if (String(err?.status || "") === "402") return true;
  return normalizeWalletErrorPayload(err)?.code === "WALLET_INSUFFICIENT_BALANCE";
}




function summarizeExecutionStatus(payload = {}) {
  const raw = String(payload?.status || payload?.message || "").trim();
  if (!raw) return "Executando etapa";
  return raw.length > 140 ? `${raw.slice(0, 137)}...` : raw;
}

function buildExecutionDoneDetail(payload = {}) {
  const routing = payload?.runtime_hints?.routing || {};
  const parts = [];
  if (routing?.mode) parts.push(`modo ${routing.mode}`);
  if (routing?.routing_source) parts.push(`origem ${routing.routing_source}`);
  if (routing?.execution_cursor?.current_node) parts.push(`nó ${routing.execution_cursor.current_node}`);
  return parts.join(" • ");
}


function normalizeCapabilityPayload(payload = null) {
  if (!payload || typeof payload !== "object") return null;
  const multiagent = payload?.multiagent && typeof payload.multiagent === "object" ? payload.multiagent : {};
  const github = payload?.github && typeof payload.github === "object" ? payload.github : {};
  return {
    multiagent,
    github,
  };
}

function formatGithubRuntimeStatus(capabilities = null) {
  const normalized = normalizeCapabilityPayload(capabilities);
  const github = normalized?.github || {};
  if (!github?.available) return "sem acesso";
  const mode = String(github?.mode || "").trim().toLowerCase();
  if (mode === "governed_pr_only") return "PR-only";
  if (github?.read_enabled && !github?.write_enabled) return "read-only";
  if (github?.write_enabled) return "conectado";
  return "conectado";
}

function formatActiveAgentRuntime(agentName = "") {
  const slug = String(agentName || "").trim().toLowerCase();
  if (!slug) return "";
  if (slug.startsWith("orion")) return "Orion analisando";
  if (slug.startsWith("chris")) return "Chris validando";
  if (slug.startsWith("auditor")) return "Auditor revisando";
  return "Orkio respondendo";
}

function traceStepTone(kind = "status") {
  if (kind === "error") return { icon: "⚠️", color: "#fca5a5", border: "rgba(248,113,113,0.24)", background: "rgba(127,29,29,0.22)" };
  if (kind === "done") return { icon: "✅", color: "#86efac", border: "rgba(74,222,128,0.24)", background: "rgba(20,83,45,0.22)" };
  if (kind === "agent") return { icon: "🧩", color: "#bfdbfe", border: "rgba(96,165,250,0.24)", background: "rgba(30,64,175,0.16)" };
  if (kind === "system") return { icon: "⚙️", color: "#c4b5fd", border: "rgba(139,92,246,0.24)", background: "rgba(76,29,149,0.18)" };
  return { icon: "⏳", color: "#e5e7eb", border: "rgba(148,163,184,0.20)", background: "rgba(15,23,42,0.26)" };
}

function resolveRealtimeTranscriptionLanguage(languageProfile) {
  const raw = (languageProfile || "").trim();
  if (!raw) return "";
  if (raw.toLowerCase() === "auto") return "";
  if (raw === "pt-BR") return "pt";
  return raw;
}



const ONBOARDING_USER_TYPES = [
  { value: "founder", label: "Founder" },
  { value: "investor", label: "Investor" },
  { value: "operator", label: "Operator" },
  { value: "partner", label: "Partner" },
  { value: "other", label: "Other" },
];

const ONBOARDING_INTENTS = [
  { value: "explore", label: "Explorar a plataforma" },
  { value: "meeting", label: "Agendar conversa" },
  { value: "pilot", label: "Avaliar piloto" },
  { value: "funding", label: "Discutir investimento" },
  { value: "other", label: "Outro" },
];

const ONBOARDING_COUNTRIES = [
  { value: "BR", label: "Brasil" },
  { value: "US", label: "Estados Unidos" },
  { value: "ES", label: "Espanha" },
  { value: "PT", label: "Portugal" },
  { value: "AR", label: "Argentina" },
  { value: "MX", label: "México" },
  { value: "CO", label: "Colômbia" },
  { value: "CL", label: "Chile" },
  { value: "UY", label: "Uruguai" },
  { value: "OTHER", label: "Outro" },
];

const ONBOARDING_LANGUAGES = [
  { value: "pt-BR", label: "Português (Brasil)" },
  { value: "en-US", label: "English (US)" },
  { value: "es-ES", label: "Español" },
  { value: "pt-PT", label: "Português (Portugal)" },
];

const DEFAULT_LANGUAGE_BY_COUNTRY = {
  BR: "pt-BR",
  PT: "pt-PT",
  ES: "es-ES",
  AR: "es-ES",
  MX: "es-ES",
  CO: "es-ES",
  CL: "es-ES",
  UY: "es-ES",
  US: "en-US",
  OTHER: "en-US",
};

function normalizeOnboardingUserType(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  const aliases = {
    founder: "founder",
    investor: "investor",
    operator: "operator",
    enterprise: "operator",
    developer: "operator",
    partner: "partner",
    other: "other",
  };
  return aliases[raw] || "";
}

function normalizeOnboardingIntent(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  const aliases = {
    explore: "explore",
    exploring: "explore",
    curious: "explore",
    meeting: "meeting",
    partnership: "meeting",
    pilot: "pilot",
    company_eval: "pilot",
    funding: "funding",
    investment: "funding",
    other: "other",
  };
  return aliases[raw] || "";
}

function suggestOnboardingLanguage(country) {
  const code = String(country || "").trim().toUpperCase();
  return DEFAULT_LANGUAGE_BY_COUNTRY[code] || "en-US";
}

function normalizeWhatsapp(value) {
  return String(value || "").replace(/[^\d+]/g, "").trim();
}

function sanitizeOnboardingForm(data) {
  const country = String(data?.country || "").trim().toUpperCase() || "BR";
  const language = String(data?.language || "").trim() || suggestOnboardingLanguage(country);
  return {
    company: String(data?.company || "").trim(),
    role: String(data?.role || data?.profile_role || "").trim(),
    user_type: normalizeOnboardingUserType(data?.user_type) || "other",
    intent: normalizeOnboardingIntent(data?.intent) || "explore",
    country,
    language,
    whatsapp: normalizeWhatsapp(data?.whatsapp || ""),
    notes: String(data?.notes || "").trim(),
  };
}

export default function AppConsole() {

  const SHOW_REALTIME_AUDIT = false;

  const nav = useNavigate();


// Summit presence heartbeat (keeps online status accurate)
// P0 HOTFIX: não enviar heartbeat sem token válido e parar em 401.
React.useEffect(() => {
  return startSessionHeartbeat({
    intervalMs: 20000,
  });
}, []);
  const [tenant, setTenant] = useState(getTenant() || "public");
  const [token, setToken] = useState(getToken());
  const [user, setUser] = useState(getUser());
  const canAccessAdmin = hasAdminAccess(user);

  useEffect(() => {
    try {
      console.log("ADMIN_RUNTIME_USER", user);
      console.log("ADMIN_RUNTIME_CAN_ACCESS", canAccessAdmin);
    } catch {}
  }, [user, canAccessAdmin]);

const [onboardingChecked, setOnboardingChecked] = useState(false);
const [onboardingOpen, setOnboardingOpen] = useState(false);
const [onboardingBusy, setOnboardingBusy] = useState(false);
const [onboardingStatus, setOnboardingStatus] = useState("");
const [onboardingForm, setOnboardingForm] = useState(() => sanitizeOnboardingForm(user));
  const [health, setHealth] = useState("checking");
  const [isMobile, setIsMobile] = useState(typeof window !== "undefined" ? window.innerWidth <= 820 : false);

  const [threads, setThreads] = useState([]);
  const [threadId, setThreadId] = useState("");
  const [messages, setMessages] = useState([]);
  const [agents, setAgents] = useState([]);
  const agentsByNameRef = useRef(new Map());

  const [text, setText] = useState("");
  const [sending, setSending] = useState(false);
  const [runtimeHints, setRuntimeHints] = useState(null);
  const showRuntimeHints = Boolean(user?.role === "admin" && typeof window !== "undefined" && window.localStorage?.getItem("orkio_show_runtime_hints") === "1");
  const showOrionSquad = Boolean(user?.role === "admin" && typeof window !== "undefined" && window.localStorage?.getItem("orkio_show_orion_squad") === "1");
  const [lastTraceId, setLastTraceId] = useState(null);
  const [agentCapabilities, setAgentCapabilities] = useState(null);
  const [activeRuntimeAgent, setActiveRuntimeAgent] = useState("");
  const [runtimeHandoffLabel, setRuntimeHandoffLabel] = useState("");
  const [orionSquadHealth, setOrionSquadHealth] = useState(null);
  const [orionSquadPreview, setOrionSquadPreview] = useState(null);
  const [walletSummary, setWalletSummary] = useState(null);
  const [walletSummaryLoading, setWalletSummaryLoading] = useState(false);
  const [walletSummaryError, setWalletSummaryError] = useState("");
  const [executionTrace, setExecutionTrace] = useState([]);
  const [executionTraceExpanded, setExecutionTraceExpanded] = useState(() => {
    if (typeof window === "undefined") return true;
    return window.localStorage?.getItem("orkio_execution_trace_open") !== "0";
  });

  // Destination selector (Team / single / multi)
  const [destMode, setDestMode] = useState("single"); // team|single|multi
  const [destSingle, setDestSingle] = useState(""); // agent id
  const [destMulti, setDestMulti] = useState([]);   // agent ids

  // Upload modal
  const [uploadOpen, setUploadOpen] = useState(false);
  const [uploadFileObj, setUploadFileObj] = useState(null);
  const [uploadScope, setUploadScope] = useState("thread"); // thread|agents|institutional
  const [uploadAgentIds, setUploadAgentIds] = useState([]);
  const [uploadStatus, setUploadStatus] = useState("");
  const [handoffBusy, setHandoffBusy] = useState(false);
  const [handoffNotice, setHandoffNotice] = useState("");
  const [showHandoffModal, setShowHandoffModal] = useState(false);
  const [handoffDraft, setHandoffDraft] = useState("");
  const [handoffInterestType, setHandoffInterestType] = useState("general");
  const [uploadProgress, setUploadProgress] = useState(false);

const fileInputRef = useRef(null);
const executionTraceRef = useRef([]);

const resetExecutionTrace = (steps = []) => {
  const normalized = Array.isArray(steps) ? steps.map((step, idx) => ({
    id: step?.id || `trace-${Date.now()}-${idx}-${Math.random().toString(36).slice(2, 7)}`,
    ts: step?.ts || Date.now(),
    kind: step?.kind || "status",
    label: step?.label || "Executando etapa",
    detail: step?.detail || "",
    agentName: step?.agentName || "",
  })) : [];
  executionTraceRef.current = normalized;
  setExecutionTrace(normalized);
};

const appendExecutionTrace = (step) => {
  if (!step) return;
  setExecutionTrace((prev) => {
    const normalized = {
      id: step?.id || `trace-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      ts: step?.ts || Date.now(),
      kind: step?.kind || "status",
      label: step?.label || "Executando etapa",
      detail: step?.detail || "",
      agentName: step?.agentName || "",
    };
    const last = prev[prev.length - 1];
    if (last && last.kind === normalized.kind && last.label === normalized.label && last.detail === normalized.detail && last.agentName === normalized.agentName) {
      return prev;
    }
    const next = [...prev.slice(-11), normalized];
    executionTraceRef.current = next;
    return next;
  });
};

const describeExecutionStatus = (payload = {}) => ({
  kind: payload?.agent_id ? "agent" : "status",
  label: summarizeExecutionStatus(payload),
  detail: payload?.message && payload?.message !== payload?.status ? String(payload.message) : "",
  agentName: payload?.agent_name || "",
});

const describeExecutionEvent = (payload = {}) => {
  const isHandoff = payload?.step === "agent_handoff";
  const handoffLabel = isHandoff
    ? `${payload?.from_agent_name || payload?.from_agent_id || "Agente"} → ${payload?.to_agent_name || payload?.agent_name || payload?.to_agent_id || "Agente"}`
    : null;
  return {
    kind: payload?.kind || (payload?.scope === "agent" ? "agent" : "system"),
    label: handoffLabel || payload?.label || summarizeExecutionStatus(payload),
    detail: String(payload?.detail || payload?.message || "").trim(),
    agentName: payload?.agent_name || payload?.to_agent_name || "",
  };
};

const describeExecutionError = (payload = {}) => {
  const label = payload?.code === "WALLET_INSUFFICIENT_BALANCE"
    ? "Saldo insuficiente para executar"
    : payload?.agent_name
    ? `${payload.agent_name} sinalizou uma falha`
    : "Execução interrompida";
  const detail = String(payload?.message || payload?.detail || payload?.error || "").trim();
  return {
    kind: "error",
    label,
    detail,
    agentName: payload?.agent_name || "",
  };
};

const describeExecutionDone = (payload = {}) => ({
  kind: "done",
  label: "Execução concluída",
  detail: buildExecutionDoneDetail(payload),
  agentName: "",
});

useEffect(() => { executionTraceRef.current = executionTrace || []; }, [executionTrace]);

useEffect(() => {
  if (typeof window === "undefined") return;
  try {
    window.localStorage?.setItem("orkio_execution_trace_open", executionTraceExpanded ? "1" : "0");
  } catch {}
}, [executionTraceExpanded]);

useEffect(() => {
  let cancelled = false;
  async function loadCapabilities() {
    if (!token) {
      if (!cancelled) setAgentCapabilities(null);
      return;
    }
    try {
      const resp = await getAgentCapabilities({ token, org: tenant });
      if (!cancelled) setAgentCapabilities(normalizeCapabilityPayload(resp?.data || resp || null));
    } catch {
      if (!cancelled) setAgentCapabilities(null);
    }
  }
  void loadCapabilities();
  return () => { cancelled = true; };
}, [token, tenant]);

const messagesEndRef = useRef(null);
  const messagesRef = useRef([]); // PATCH0100_20B: keep latest messages for voice-to-voice sequencing

  // Voice-to-text (manual toggle)
  const [speechSupported] = useState(true);
  const speechRef = useRef(null);
  const [micEnabled, setMicEnabled] = useState(false);
  const micEnabledRef = useRef(false);
  const micRetryRef = useRef({ tries: 0, lastTry: 0 });

  // PATCH0100_13: Voice Mode (TTS + auto-send)
  const [voiceMode, setVoiceMode] = useState(SUMMIT_VOICE_MODE === "stt_tts");
  const voiceModeRef = useRef(SUMMIT_VOICE_MODE === "stt_tts");
  const [ttsPlaying, setTtsPlaying] = useState(false);
  const ttsAudioRef = useRef(null);
  const [ttsVoice, setTtsVoice] = useState(localStorage.getItem('orkio_tts_voice') || 'nova');
  const lastSpokenMsgRef = useRef('');
  const lastSpokenMessageIdRef = useRef(null);
  const micRestartTimeoutRef = useRef(null);
  const mediaRecorderStreamRef = useRef(null);
  const mediaRecorderSilenceIntervalRef = useRef(null);
  const mediaRecorderSilenceTimeoutRef = useRef(null);
  // PATCH0100_14: agent info from last chat response (for voice/avatar)
  const [lastAgentInfo, setLastAgentInfo] = useState(null);

  // PATCH0100_28: Terms acceptance modal
  const [showTermsModal, setShowTermsModal] = useState(false);
  const [composerViewportOffset, setComposerViewportOffset] = useState(0);

  
  // Realtime/WebRTC voice mode (ultra low latency)
  const [realtimeMode, setRealtimeMode] = useState(false);
  const realtimeModeRef = useRef(false);
  const rtcPcRef = useRef(null);
  const rtcDcRef = useRef(null);
  const rtcAudioElRef = useRef(null);
  const rtcAudioProcessingRef = useRef(null);
  const rtcTextBufRef = useRef("");
  const rtcLastMagicRef = useRef("");
  const [rtcReadyToRespond, setRtcReadyToRespond] = useState(false);
  const rtcLastFinalTranscriptRef = useRef("");
  const rtcMagicEnabledRef = useRef(true);
  const rtcVoiceRef = useRef("cedar");
  const rtcAudioTranscriptBufRef = useRef("");
  const rtcLastAssistantFinalRef = useRef("");
  const rtcAssistantFinalCommittedRef = useRef(false);
  const rtcResponseTimeoutRef = useRef(null);
  const rtcFallbackActiveRef = useRef(false);
  const rtcResponseInFlightRef = useRef(false);


const rtcIdleFollowupTimerRef = useRef(null);
const rtcIdleFollowupSentRef = useRef(false);
const rtcLastUserActivityAtRef = useRef(0);

  // PATCH0100_27A: Realtime persistence (audit)
  const rtcSessionIdRef = useRef(null);
  const rtcThreadIdRef = useRef(null);
  const rtcEventQueueRef = useRef([]);
  const rtcFlushTimerRef = useRef(null);
  const rtcLivePollTimerRef = useRef(null);
  const rtcSeenBackendResponseIdsRef = useRef(new Set());
  const rtcConnectingRef = useRef(false);
  // PATCH0100_27_2B: UI log + punct status
  const [rtcAuditEvents, setRtcAuditEvents] = useState([]);
  const [rtcPunctStatus, setRtcPunctStatus] = useState(null); // null | 'pending' | 'done' | 'timeout'
  const [lastRealtimeSessionId, setLastRealtimeSessionId] = useState(null);
  const [summitSessionScore, setSummitSessionScore] = useState(null);
  const [summitReviewPending, setSummitReviewPending] = useState(false);
  const summitRuntimeModeRef = useRef((((window.__ORKIO_ENV__?.VITE_ORKIO_RUNTIME_MODE || import.meta.env.VITE_ORKIO_RUNTIME_MODE || "summit")).trim().toLowerCase() === "summit") ? "summit" : "platform");
  const summitLanguageProfileRef = useRef((((window.__ORKIO_ENV__?.VITE_SUMMIT_LANGUAGE_PROFILE || import.meta.env.VITE_SUMMIT_LANGUAGE_PROFILE || "pt-BR")).trim() || "auto"));



// V2V-PATCH: trace_id por tentativa + status de fase + MediaRecorder
  const v2vTraceRef = useRef(null);

  // STREAM-STAB: anti-zombie (AbortController + runId)

// PATCH0113: Summit capacity modal (STREAM_LIMIT)
const [capacityOpen, setCapacityOpen] = React.useState(false);
const [capacitySeconds, setCapacitySeconds] = React.useState(30);
const capacityTimerRef = React.useRef(null);
const capacityPendingRef = React.useRef(null); // { msg }

const openCapacityModal = (msg) => {
  setCapacityOpen(true);
  setCapacitySeconds(30);
  capacityPendingRef.current = { msg: msg || "" };
  try { if (capacityTimerRef.current) clearInterval(capacityTimerRef.current); } catch {}
  capacityTimerRef.current = setInterval(() => {
    setCapacitySeconds((s) => {
      const next = Math.max(0, (s || 0) - 1);
      if (next === 0) {
        try { if (capacityTimerRef.current) clearInterval(capacityTimerRef.current); } catch {}
        capacityTimerRef.current = null;
        // auto retry (Summit)
        const pending = capacityPendingRef.current;
        if (pending?.msg) {
          try { sendMessage(pending.msg, { isRetry: true }); } catch {}
        }
      }
      return next;
    });
  }, 1000);
};

const closeCapacityModal = () => {
  setCapacityOpen(false);
  try { if (capacityTimerRef.current) clearInterval(capacityTimerRef.current); } catch {}
  capacityTimerRef.current = null;
};
  const streamCtlRef = useRef(null);
  const streamRunRef = useRef(0);

  const [v2vPhase, setV2vPhase] = useState(null); // null | 'recording' | 'stt' | 'chat' | 'tts' | 'playing' | 'error'
  const [v2vError, setV2vError] = useState(null);
  const [walletBlockedDetail, setWalletBlockedDetail] = useState(null);
  const mediaRecorderRef = useRef(null);
  const audioChunksRef = useRef([]);
  // BUG-02 FIX: flag para distinguir stop intencional (stopMicMediaRecorder)
  // de stop por VAD — evita processar áudio residual quando V2V é desligado
  const stopIntentionalRef = useRef(false);
  const [mediaRecorderSupported] = useState(!!(
    typeof window !== 'undefined' &&
    window.MediaRecorder &&
    navigator.mediaDevices?.getUserMedia
  ));

  
useEffect(() => {
  let alive = true;

  async function bootstrapUser() {
    const t = getToken();
    const u = getUser();
    const org = getTenant() || "public";
    setToken(t);
    setTenant(org);
    setUser(u);

    if (!t) {
      nav("/auth");
      return;
    }

    try {
      const { data } = await apiFetch("/api/me", { method: "GET", token: t, org });
      if (!alive) return;
      if (data) {
        const mergedUser = {
          ...(u || {}),
          ...data,
          org_slug: data?.org_slug || u?.org_slug || org,
          role: data?.role || u?.role || "user",
          signup_source: data?.signup_source ?? u?.signup_source ?? null,
          signup_code_label: data?.signup_code_label ?? u?.signup_code_label ?? null,
          product_scope: data?.product_scope ?? u?.product_scope ?? null,
          country: data?.country ?? u?.country ?? null,
          language: data?.language ?? u?.language ?? null,
          whatsapp: data?.whatsapp ?? u?.whatsapp ?? null,
          is_admin: hasAdminAccess({
            ...(u || {}),
            ...data,
            role: data?.role || u?.role || "user",
            is_admin: data?.is_admin === true || u?.is_admin === true,
            admin: data?.admin === true || u?.admin === true,
          }),
        };
        mergedUser.admin = mergedUser.is_admin === true;

        setUser(mergedUser);
        try { setSession({ token: t, user: mergedUser, tenant: mergedUser.org_slug || org }); } catch {}

        const explicitPendingApproval = (
          mergedUser?.pending_approval === true
          || mergedUser?.auth_status === "pending_approval"
          || mergedUser?.status === "pending"
        );

        if (explicitPendingApproval) {
          clearSession();
          nav("/auth?pending_approval=1");
          return;
        }

        if (!mergedUser?.onboarding_completed) {
          setOnboardingForm(sanitizeOnboardingForm(mergedUser));
          // PATCH27_2_CLEAN: onboarding is fluid/non-blocking. Keep data available but do not force modal open.
          setOnboardingOpen(false);
        }

        if (!mergedUser?.terms_accepted_at) {
          setShowTermsModal(true);
        }
      }
    } catch (err) {
      console.warn("bootstrapUser failed", err);
    } finally {
      if (alive) setOnboardingChecked(true);
    }
  }

  bootstrapUser();
  return () => { alive = false; };
}, []);

  useEffect(() => {
    setWalletSummary(null);
    setWalletSummaryLoading(false);
    setWalletSummaryError("");
    return undefined;
  }, [token, tenant]);

  useEffect(() => {
    const onResize = () => {
      try { setIsMobile(window.innerWidth <= 820); } catch {}
    };
    onResize();
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  useEffect(() => {
    if (SUMMIT_VOICE_MODE === "stt_tts") {
      setVoiceMode(true);
      voiceModeRef.current = true;
      if (realtimeModeRef.current) {
        try { void stopRealtime("voice_mode_lock"); } catch {}
        setRealtimeMode(false);
        realtimeModeRef.current = false;
      }
      return;
    }
    setVoiceMode(false);
    voiceModeRef.current = false;
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function checkHealth() {
      try {
        await apiFetch("/api/health", { token, org: tenant });
        if (!cancelled) setHealth("ok");
      } catch {
        if (!cancelled) setHealth("down");
      }
    }

    if (token) checkHealth();

    return () => {
      cancelled = true;
    };
  }, [token, tenant]);

  useEffect(() => {
    let cancelled = false;

    async function checkOrionSquadHealth() {
      if (!token || !showOrionSquad) {
        if (!cancelled) setOrionSquadHealth(null);
        return;
      }
      try {
        const resp = await getOrionSquadHealth({ token, org: tenant });
        if (!cancelled) setOrionSquadHealth(resp?.data || resp || null);
      } catch {
        if (!cancelled) setOrionSquadHealth({ ok: false });
      }
    }

    void checkOrionSquadHealth();

    return () => {
      cancelled = true;
    };
  }, [token, tenant, showOrionSquad]);

  async function refreshOrionSquadPreview(messageToPreview) {
    const previewMessage = String(messageToPreview || "").trim();
    if (!token || !showOrionSquad || !previewMessage) return;
    try {
      const resp = await getOrionSquadPreview({ token, org: tenant, message: previewMessage });
      setOrionSquadPreview(resp?.data || resp || null);
    } catch {
      // fail-open
    }
  }

  function scrollToBottom() {
    try {
      messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    } catch {}
  }

  useEffect(() => { scrollToBottom(); }, [messages]);

  const walletBalanceUsd = Number(walletSummary?.wallet?.balance_usd || 0);
  const walletLowBalanceThresholdUsd = Number(
    walletSummary?.wallet?.low_balance_threshold_usd
    ?? walletSummary?.wallet?.auto_recharge_threshold_usd
    ?? 3
  );
  const walletLowBalance = walletBalanceUsd <= walletLowBalanceThresholdUsd;
  const walletActivePlanName = walletSummary?.active_plan?.name || "";
  const walletAutoRechargeEnabled = !!walletSummary?.wallet?.auto_recharge_enabled;
  const walletSummaryUpdatedAt = walletSummary?.wallet?.updated_at || null;

  async function refreshWalletSummary() {
    setWalletSummary(null);
    setWalletSummaryLoading(false);
    setWalletSummaryError("");
    return null;
  }

  useEffect(() => {
    function onKeyDown(e) {
      if (!realtimeModeRef.current) return;
      if (!rtcReadyToRespond) return;
      // Don't hijack typing in inputs/textarea/contenteditable
      const el = document.activeElement;
      const tag = el?.tagName?.toLowerCase();
      const isTyping = tag === "input" || tag === "textarea" || el?.isContentEditable;
      if (isTyping) return;

      if (e.code === "Space" || e.key === " " || e.key === "Enter") {
        e.preventDefault();
        triggerRealtimeResponse("hotkey");
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [rtcReadyToRespond]);
  useEffect(() => { messagesRef.current = (messages || []); }, [messages]);

useEffect(() => {
  if (typeof window === "undefined" || !window.visualViewport) return undefined;
  const vv = window.visualViewport;
  const updateViewportOffset = () => {
    try {
      const keyboardOffset = Math.max(0, window.innerHeight - vv.height - vv.offsetTop);
      setComposerViewportOffset(keyboardOffset);
    } catch {
      setComposerViewportOffset(0);
    }
  };
  updateViewportOffset();
  vv.addEventListener("resize", updateViewportOffset);
  vv.addEventListener("scroll", updateViewportOffset);
  window.addEventListener("orientationchange", updateViewportOffset);
  return () => {
    vv.removeEventListener("resize", updateViewportOffset);
    vv.removeEventListener("scroll", updateViewportOffset);
    window.removeEventListener("orientationchange", updateViewportOffset);
  };
}, []);


  async function loadThreads() {
    try {
      const { data } = await apiFetch("/api/threads", { token, org: tenant });
      setThreads(data || []);
      if (!threadId && data?.[0]?.id) setThreadId(data[0].id);
    } catch (e) {
      console.error("loadThreads error:", e);
      clearSession();
      nav("/auth");
    }
  }

  async function loadMessages(tid) {
    if (!tid) return [];
    try {
      const { data } = await apiFetch(`/api/messages?thread_id=${encodeURIComponent(tid)}&include_welcome=0`, { token, org: tenant });
      setMessages(data || []);
      return (data || []);
    } catch (e) {
      console.error("loadMessages error:", e);
      return [];
    }
  }

  async function loadAgents() {
    try {
      const { data } = await apiFetch("/api/agents", { token, org: tenant });
      setAgents(data || []);
      try {
        const m = new Map();
        (data || []).forEach(a => { if (a?.name) m.set(String(a.name).trim(), a.id); });
        agentsByNameRef.current = m;
      } catch {}

      // Default destination (single) to Orkio if exists
      if (!destSingle && Array.isArray(data)) {
        const orkio = data.find(a => (a.name || "").toLowerCase() === "orkio") || data.find(a => a.is_default);
        if (orkio) setDestSingle(orkio.id);
      }
    } catch (e) {
      console.error("loadAgents error:", e);
    }
  }

  useEffect(() => {
    if (!token || !onboardingChecked || onboardingOpen) return;
    loadThreads();
    loadAgents();
  }, [token, tenant, onboardingChecked, onboardingOpen]);

  useEffect(() => { if (threadId) loadMessages(threadId); }, [threadId]);






  async function createThread() {
    try {
      const { data } = await apiFetch("/api/threads", {
        method: "POST",
        token,
        org: tenant,
        body: { title: "Nova conversa" },
      });
      if (data?.id) {
        await loadThreads();
        setThreadId(data.id);
      }
    } catch (e) {
      alert(e?.message || "Falha ao criar conversa");
    }
  }

  async function deleteThread(threadId) {
    if (!threadId) return;
    if (!confirm('Deletar esta conversa?')) return;
    try {
      await apiFetch(`/api/threads/${encodeURIComponent(threadId)}`, {
        method: "DELETE",
        token,
        org: tenant,
      });
      // Reload threads and pick a safe next one
      const { data } = await apiFetch("/api/threads", { token, org: tenant });
      const list = data || [];
      setThreads(list);
      const nextId = list?.[0]?.id || "";
      setThreadId(nextId);
      if (nextId) await loadMessages(nextId);
      else setMessages([]);
    } catch (e) {
      console.error("deleteThread error:", e);
      alert(e?.message || "Falha ao deletar conversa");
    }
  }

  async function renameThread(tid) {
    const t = threads.find((x) => x.id === tid);
    const current = t?.title || "Nova conversa";
    const next = prompt("Renomear conversa:", current);
    if (!next) return;
    try {
      await apiFetch(`/api/threads/${encodeURIComponent(tid)}`, {
        method: "PATCH",
        token,
        org: tenant,
        body: { title: next },
      });
      await loadThreads();
    } catch (e) {
      alert(e?.message || "Falha ao renomear");
    }
  }

  async function doLogout() {
    try {
      await logout({ org: tenant, token });
    } finally {
      clearSession();
      nav("/auth");
    }
  }

  function buildMessagePrefix() {
    if (destMode === "team") return "@Team ";
    if (destMode === "single") {
      const ag = agents.find(a => a.id === destSingle);
      return ag ? `@${ag.name} ` : "";
    }
    if (destMode === "multi") {
      const names = agents.filter(a => destMulti.includes(a.id)).map(a => a.name);
      if (!names.length) return "@Team ";
      // backend parser supports @Name tokens; join them
      return names.map(n => `@${n}`).join(" ") + " ";
    }
    return "";
  }


  function appendToPlaceholder(delta) {
    if (!delta) return;

    setMessages((prev) => {
      const messages = Array.isArray(prev) ? [...prev] : [];

      for (let i = messages.length - 1; i >= 0; i--) {
        const m = messages[i];

        if (
          m?.role === "assistant" &&
          String(m?.id || "").startsWith("tmp-ass-")
        ) {
          const oldContent =
            m.content === "⌛ Preparando resposta..."
              ? ""
              : (m.content || "");

          messages[i] = {
            ...m,
            content: oldContent + delta,
          };

          return messages;
        }
      }

      messages.push({
        id: `tmp-ass-${Date.now()}`,
        role: "assistant",
        content: delta,
        agent_name: "Orkio",
        created_at: Math.floor(Date.now() / 1000),
      });

      return messages;
    });
  }

  async function sendMessage(presetMsg = null, opts = {}) {
    const isRetry = !!opts?.isRetry;
    clearRealtimeIdleFollowup();
    const msg = ((presetMsg ?? text) || "").trim();
    if (!msg || sending) return;
    setSending(true);

    // STREAM-STAB: start new run and abort any previous stream
    streamRunRef.current += 1;
    const myRun = streamRunRef.current;
    try { streamCtlRef.current?.abort(); } catch {}
    const ctl = new AbortController();
    streamCtlRef.current = ctl;
    const isStale = () => (myRun !== streamRunRef.current || ctl.signal.aborted);

    // UX: show progress while waiting
    try { setUploadStatus('⌛ Gerando resposta...'); } catch {}

    try {
      const pref = buildMessagePrefix();
      const finalMsg = pref + msg;
      void refreshOrionSquadPreview(finalMsg);

      const agentIdToSend = destSingle || null; // host agent for both single + team (team uses host voice)

      const optimisticUserId = `tmp-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const draftAssistantId = `tmp-ass-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

      // optimistic message
      if (!isRetry) {
        setMessages((prev) => [
          ...prev,
          {
            id: optimisticUserId,
            role: "user",
            content: msg,
            user_name: user?.name || user?.email,
            created_at: Math.floor(Date.now() / 1000),
          },
          {
            id: draftAssistantId,
            role: "assistant",
            content: "⌛ Preparando resposta...",
            agent_name: "Orkio",
            created_at: Math.floor(Date.now() / 1000),
          },
        ]);
        setText("");
      }

      // V2V-PATCH: gerar trace_id por tentativa de V2V (correlaciona logs backend)
      const traceId = `v2v-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const clientMessageId = (globalThis.crypto && crypto.randomUUID) ? crypto.randomUUID() : (`cm-${Date.now()}-${Math.random().toString(36).slice(2,10)}`);
      v2vTraceRef.current = traceId;
      setV2vPhase('chat');
      setV2vError(null);
      setWalletBlockedDetail(null);
      setExecutionTraceExpanded(true);
      setActiveRuntimeAgent("Orkio");
      setRuntimeHandoffLabel("");
      resetExecutionTrace([
        {
          kind: "system",
          label: "Solicitação recebida",
          detail: destMode === "team"
            ? "Modo Team acionado."
            : destMode === "multi"
            ? "Execução multiagente preparada."
            : agentIdToSend
            ? "Agente definido para esta execução."
            : "Roteamento automático preparado.",
        },
        {
          kind: "status",
          label: "Enviando para o runtime do Orkio",
          detail: "Aguardando resposta do trilho principal.",
        },
      ]);

      // v4: SSE becomes the primary chat rail, with JSON fallback preserved.
      let resp = null;
      let newThreadId = threadId;

      if (ORKIO_CHAT_STREAM_PRIMARY) {
        try {
          const streamResp = await chatStream({
            token,
            org: tenant,
            thread_id: threadId,
            message: finalMsg,
            agent_id: agentIdToSend,
            trace_id: traceId,
            client_message_id: clientMessageId,
            signal: ctl.signal,
          });
          const streamMeta = await consumeChatStream(streamResp, {
            onStatus: (payload) => {
              if (payload?.status) setUploadStatus(`⌛ ${payload.status}`);
              if (payload?.agent_name) setActiveRuntimeAgent(payload.agent_name);
              appendExecutionTrace(describeExecutionStatus(payload));
            },
            onError: (payload) => {
              appendExecutionTrace(describeExecutionError(payload));
              if (payload?.code === "WALLET_INSUFFICIENT_BALANCE") {
                setWalletBlockedDetail(payload);
                setV2vError(buildWalletBlockedMessage(payload));
                return;
              }
              if (payload?.agent_id && payload?.code !== "SERVER_BUSY") return;
              if (payload?.message) setV2vError(String(payload.message));
            },
            onExecution: (payload) => {
              if (payload?.step === "agent_handoff") {
                const handoff = `${payload?.from_agent_name || payload?.from_agent_id || "Agente"} → ${payload?.to_agent_name || payload?.agent_name || payload?.to_agent_id || "Agente"}`;
                setRuntimeHandoffLabel(handoff);
                setActiveRuntimeAgent(payload?.to_agent_name || payload?.agent_name || "");
              } else if (payload?.agent_name) {
                setActiveRuntimeAgent(payload.agent_name);
              }
              appendExecutionTrace(describeExecutionEvent(payload));
            },
            onChunk: (_payload, draftText) => {
              setMessages((prev) => prev.map((m) => (
                m.id === draftAssistantId
                  ? { ...m, content: draftText || "⌛ Preparando resposta..." }
                  : m
              )));
            },
            onAgentDone: (payload) => {
              if (payload?.agent_name) setActiveRuntimeAgent(payload.agent_name);
              appendExecutionTrace({
                kind: "agent",
                label: `${payload?.agent_name || payload?.agent_id || "Agente"} concluiu uma etapa`,
                detail: payload?.message || payload?.status || "Resposta parcial pronta.",
                agentName: payload?.agent_name || "",
              });
              setMessages((prev) => prev.map((m) => (
                m.id === draftAssistantId
                  ? { ...m, content: (m.content || "").replace(/^⌛\s*/, "") || "Resposta concluída." }
                  : m
              )));
            },
            onKeepalive: () => {},
            onDone: (payload) => {
              appendExecutionTrace(describeExecutionDone(payload));
              if (payload?.thread_id) newThreadId = payload.thread_id;
              if (payload?.runtime_hints) {
                setRuntimeHints(payload.runtime_hints);
                if (payload.runtime_hints?.capabilities) {
                  setAgentCapabilities(normalizeCapabilityPayload(payload.runtime_hints.capabilities));
                }
              }
              if (payload?.trace_id) setLastTraceId(payload.trace_id);
            },
          });
          resp = {
            data: {
              thread_id: streamMeta?.thread_id || newThreadId,
              used_stream: true,
              runtime_hints: streamMeta?.runtime_hints || null,
              trace_id: streamMeta?.trace_id || traceId,
              execution_cursor: streamMeta?.execution_cursor || null,
              execution_lifecycle: streamMeta?.execution_lifecycle || null,
              routing_source: streamMeta?.routing_source || null,
              route_applied: !!streamMeta?.route_applied,
            }
          };
          if (streamMeta?.thread_id) newThreadId = streamMeta.thread_id;
        } catch (streamErr) {
          if (streamErr instanceof StreamSemanticError) {
            throw streamErr;
          }
          appendExecutionTrace({
            kind: "system",
            label: "Alternando para resposta direta",
            detail: "O stream foi degradado e o Orkio seguiu pelo fallback seguro.",
          });
          resp = await chat({
            token,
            org: tenant,
            thread_id: threadId,
            message: finalMsg,
            agent_id: agentIdToSend,
            trace_id: traceId,
            client_message_id: clientMessageId,
            signal: ctl.signal,
          });
        }
      } else {
        resp = await chat({
          token,
          org: tenant,
          thread_id: threadId,
          message: finalMsg,
          agent_id: agentIdToSend,
          trace_id: traceId,
          client_message_id: clientMessageId,
          signal: ctl.signal,
        });
      }

      if (resp?.status === 429) {
        closeCapacityModal();
        openCapacityModal(msg);
        return;
      }

       // V2V-PATCH: se fallback /api/chat criou thread, capturar thread_id do resp
       if (resp?.data?.thread_id) newThreadId = resp.data.thread_id;
      // F-03 FIX: usar newThreadId (var local) em vez de threadId (closure stale do React)
      // Se a conversa foi criada durante o SSE stream, threadId ainda aponta para a thread antiga
      const effectiveTidForLoad = newThreadId || threadId;
      if (effectiveTidForLoad && effectiveTidForLoad !== threadId) {
        setThreadId(effectiveTidForLoad);
      }
      const freshMessages = await loadMessages(effectiveTidForLoad);
      void refreshWalletSummary({ silent: true });

      // PATCH0100_14: store agent info from response
      if (resp?.data) {
        const ai = { agent_id: resp.data.agent_id, agent_name: resp.data.agent_name, voice_id: resolveAgentVoice({ agent_name: resp.data.agent_name, voice_id: resp.data.voice_id }), avatar_url: resp.data.avatar_url };
        setLastAgentInfo(ai);
        if (resp.data.agent_name) setActiveRuntimeAgent(resp.data.agent_name);
        if (resp.data.runtime_hints) {
          setRuntimeHints(resp.data.runtime_hints || null);
          if (resp.data.runtime_hints?.capabilities) {
            setAgentCapabilities(normalizeCapabilityPayload(resp.data.runtime_hints.capabilities));
          }
        }
        if (resp.data.trace_id) setLastTraceId(resp.data.trace_id);
        if (!resp?.data?.used_stream) {
          appendExecutionTrace({
            kind: "agent",
            label: `${resp.data.agent_name || "Orkio"} consolidou a resposta`,
            detail: resp.data.runtime_hints?.routing?.mode ? `modo ${resp.data.runtime_hints.routing.mode}` : "",
            agentName: resp.data.agent_name || "",
          });
          appendExecutionTrace({
            kind: "done",
            label: "Execução concluída",
            detail: buildExecutionDoneDetail({ runtime_hints: resp.data.runtime_hints || null }),
          });
        }
      }
      // V2V-PATCH: Auto-play TTS — fase TTS + fase playing com trace_id
      if (voiceModeRef.current) {
        if (micEnabledRef.current) stopMic();
        const prevLast = messagesRef.current?.slice?.().reverse?.().find?.(m => m.role === "assistant" && !String(m?.id||"").startsWith("tmp-ass-"))?.created_at || null;
        const fresh = (freshMessages || []);
        const assistants = (fresh || []).filter(m => m.role === "assistant" && !String(m.id || "").startsWith("tmp-ass-"));
        let toSpeak = assistants;
        if (prevLast) {
          // F-04: epoch Unix (segundos) → ms para JS
          const prevT = new Date((prevLast || 0) * 1000).getTime();
          toSpeak = assistants.filter(m => {
            const t = new Date((m.created_at || 0) * 1000).getTime();
            // BUG-03 FIX: filtro estrito (>) — não incluir a msg anterior (prevT)
            return isFinite(t) && t > prevT;
          });
        } else {
          toSpeak = assistants.slice(-1);
        }

        // Team: fala cada mensagem sequencialmente com voz correta por agente
        // Single: só a última
        if (destMode !== "team" && toSpeak.length > 1) toSpeak = toSpeak.slice(-1);

        const currentTrace = v2vTraceRef.current || traceId;
        const shouldAutoSpeakThisTurn =
          !!opts?.explicitVoiceRequested ||
          !!opts?.voiceRequested ||
          !!opts?.realtimeTurn;

        for (const m of toSpeak) {
          const content = (m.content || "").trim();
          if (!content) continue;
          if (!shouldAutoSpeakThisTurn) continue;
          const agentIdFallback = m.agent_id || null;
          // preferir message_id (backend resolve voz); agent_id só como fallback
          setV2vPhase('tts');
          try { setUploadStatus(`🔊 Gerando voz (${m.agent_name || 'agente'})...`); } catch {}
          await playTts(content, agentIdFallback, {
            forceAuto: true,
            messageId: m.id || null,
            traceId: currentTrace,
          });
        }
        setV2vPhase(null);
        setV2vError(null);
        // BUG-01 FIX: fallback — se playTts não reiniciou o mic (ex: autoplay bloqueado)
        // garantir que o ciclo V2V continua ouvindo
        if (voiceModeRef.current && !micEnabledRef.current) {
          scheduleMicRestart("post-tts");
        }
      }

    } catch (e) {
      console.error("[V2V] sendMessage error:", e);
      setV2vPhase('error');
      const walletDetail = normalizeWalletErrorPayload(e);
      if (walletDetail || isWalletBlockedError(e)) {
        const detail = walletDetail || { code: "WALLET_INSUFFICIENT_BALANCE" };
        appendExecutionTrace({
          kind: "error",
          label: "Wallet bloqueou a execução",
          detail: buildWalletBlockedMessage(detail),
        });
        setWalletBlockedDetail(detail);
        setV2vError(buildWalletBlockedMessage(detail));
        setMessages((prev) => prev.map((m) => (
          m.id === draftAssistantId
            ? {
                ...m,
                content: buildWalletBlockedMessage(detail),
                agent_name: m.agent_name || "Wallet",
              }
            : m
        )));
        void refreshWalletSummary({ silent: false });
      } else {
        setWalletBlockedDetail(null);
        appendExecutionTrace({
          kind: "error",
          label: "Execução falhou",
          detail: e?.message || "Falha ao enviar mensagem",
        });
        // BUG-04 FIX: trocar alert() por setV2vError — alert() bloqueia JS thread
        // e impede o V2V de reiniciar o microfone
        setV2vError(e?.message || "Falha ao enviar mensagem");
      }
    } finally {
      setSending(false);
      try { if (!ttsPlaying) setUploadStatus(''); } catch {}
    }
  }

  function handleKeyDown(e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }

  // Voice recognition helpers
  function ensureSpeech() {
    if (!speechSupported) return null;
    if (speechRef.current) return speechRef.current;
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) return null;
    const rec = new SR();
    rec.lang = SPEECH_RECOGNITION_LANG;
    rec.interimResults = true;
    rec.continuous = true;
    speechRef.current = rec;
    return rec;
  }

  function stopMic() {
    micEnabledRef.current = false;
    setMicEnabled(false);
    if (micRestartTimeoutRef.current) {
      clearTimeout(micRestartTimeoutRef.current);
      micRestartTimeoutRef.current = null;
    }
    // V2V-PATCH: parar MediaRecorder se ativo
    stopMicMediaRecorder();
    // parar SpeechRecognition se ativo
    const rec = speechRef.current;
    if (rec) {
      try { rec.onend = null; rec.stop(); } catch {}
    }
  }

  function clearMediaRecorderTimers() {
    if (mediaRecorderSilenceIntervalRef.current) {
      clearInterval(mediaRecorderSilenceIntervalRef.current);
      mediaRecorderSilenceIntervalRef.current = null;
    }
    if (mediaRecorderSilenceTimeoutRef.current) {
      clearTimeout(mediaRecorderSilenceTimeoutRef.current);
      mediaRecorderSilenceTimeoutRef.current = null;
    }
  }

  function scheduleMicRestart(reason = "unknown", delay = 300) {
    if (!voiceModeRef.current || micEnabledRef.current) return;
    if (micRestartTimeoutRef.current) clearTimeout(micRestartTimeoutRef.current);
    micRestartTimeoutRef.current = setTimeout(() => {
      micRestartTimeoutRef.current = null;
      if (!voiceModeRef.current || micEnabledRef.current) return;
      console.info('[V2V] mic restart reason=%s', reason);
      startMic();
    }, Math.max(0, Number(delay) || 0));
  }

  function resolveSttLanguage() {
    const explicit = (
      window.__ORKIO_ENV__?.VITE_STT_LANGUAGE ||
      window.__ORKIO_ENV__?.VITE_REALTIME_TRANSCRIBE_LANGUAGE ||
      import.meta.env.VITE_STT_LANGUAGE ||
      import.meta.env.VITE_REALTIME_TRANSCRIBE_LANGUAGE ||
      ""
    ).trim();
    if (explicit) return explicit;
    return voiceModeRef.current ? "pt" : null;
  }

  // V2V-PATCH: startMic usa MediaRecorder (webm/opus) quando disponível.
  // MediaRecorder → /api/stt (Whisper) → texto → sendMessage()
  // Fallback: SpeechRecognition (Chrome-only) → texto → sendMessage()
  function startMic() {
    if (micRestartTimeoutRef.current) {
      clearTimeout(micRestartTimeoutRef.current);
      micRestartTimeoutRef.current = null;
    }
    clearMediaRecorderTimers();
    micEnabledRef.current = true;
    setMicEnabled(true);
    setV2vError(null);

    // ── Caminho 1: MediaRecorder (todos os browsers modernos, qualidade superior) ──
    if (mediaRecorderSupported && voiceModeRef.current) {
      navigator.mediaDevices.getUserMedia({ audio: true, video: false })
        .then(stream => {
          if (!micEnabledRef.current) {
            stream.getTracks().forEach(t => t.stop());
            return;
          }
          mediaRecorderStreamRef.current = stream;

          const mimeType = MediaRecorder.isTypeSupported('audio/webm;codecs=opus')
            ? 'audio/webm;codecs=opus'
            : (MediaRecorder.isTypeSupported('audio/webm') ? 'audio/webm' : 'audio/mp4');

          const mr = new MediaRecorder(stream, { mimeType });
          mediaRecorderRef.current = mr;
          audioChunksRef.current = [];

          mr.ondataavailable = (e) => {
            if (e.data && e.data.size > 0) audioChunksRef.current.push(e.data);
          };

          mr.onstop = async () => {
            stream.getTracks().forEach(t => t.stop());
            // BUG-02 FIX: stop intencional (stopMicMediaRecorder) → descartar chunks
            if (stopIntentionalRef.current) {
              stopIntentionalRef.current = false;
              audioChunksRef.current = [];
              return;
            }
            if (!micEnabledRef.current && !voiceModeRef.current) return;

            const chunks = audioChunksRef.current;
            audioChunksRef.current = [];
            if (!chunks.length) return;

            const blob = new Blob(chunks, { type: mimeType });
            if (blob.size < 500) {
              console.warn('[V2V] áudio muito curto, ignorando');
              return;
            }

            const trace = `v2v-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
            v2vTraceRef.current = trace;
            setV2vPhase('stt');
            setUploadStatus('🎙️ Transcrevendo fala...');

            console.info('[V2V] v2v_record_received trace_id=%s size=%d', trace, blob.size);

            try {
              const sttLang = resolveSttLanguage();
              const result = await transcribeAudio(blob, { token, org: tenant, trace_id: trace, language: sttLang || null });
              const text = (result?.text || '').trim();
              console.info('[V2V] v2v_stt_ok trace_id=%s chars=%d preview=%s', trace, text.length, text.slice(0, 60));

              if (!text) {
                console.warn('[V2V] v2v_stt_fail trace_id=%s reason=empty_transcript', trace);
                setV2vPhase('error');
                setV2vError('Nenhum texto reconhecido. Fale novamente.');
                setUploadStatus('⚠️ Fala não reconhecida. Tente novamente.');
                setTimeout(() => setUploadStatus(''), 2500);
                // Reiniciar escuta
                if (micEnabledRef.current && voiceModeRef.current) {
                  scheduleMicRestart('empty_transcript', 800);
                }
                return;
              }

              setText(text);
              setV2vPhase('chat');
              setUploadStatus(`🎙️ "${text.slice(0, 50)}${text.length > 50 ? '…' : ''}"`);

              if (voiceModeRef.current && micEnabledRef.current) {
                micEnabledRef.current = false;
                setMicEnabled(false);
                // pequeno delay para garantir que o texto aparece no input
                setTimeout(() => sendMessage(), 80);
              }
            } catch (e) {
              console.error('[V2V] v2v_stt_fail trace_id=%s error:', trace, e);
              setV2vPhase('error');
              setV2vError(`STT falhou: ${e?.message || 'erro desconhecido'}`);
              setUploadStatus(`❌ STT: ${e?.message || 'Erro de transcrição'}`);
              setTimeout(() => setUploadStatus(''), 3000);
            }
          };

          // Gravar em segmentos de 4s — silêncio detectado por VAD simples (tamanho do chunk)
          mr.start(100); // PATCH0100_24D: smaller chunks for better VAD // coleta chunks a cada 4s

          // Auto-stop após 30s máximo ou quando detectar silêncio
          let silenceTimer = null;
          let lastSize = 0;

          // PATCH0100_24D: VAD menos agressivo (1.5s de silêncio consecutivo)
          let consecutiveSilences = 0;

          mediaRecorderSilenceIntervalRef.current = setInterval(() => {
            const currentSize = audioChunksRef.current.reduce((s, c) => s + c.size, 0);
            const delta = currentSize - lastSize;
            lastSize = currentSize;

            // Espera acumular um mínimo de áudio e só encerra após 3 janelas silenciosas (~1.5s)
            if (currentSize > 3000 && delta < 500) {
              consecutiveSilences += 1;
            } else {
              consecutiveSilences = 0;
            }

            if (consecutiveSilences >= 3) {
              clearMediaRecorderTimers();
              
              try { mr.stop(); } catch {}
            }
          }, 500);

          silenceTimer = setTimeout(() => {
            clearMediaRecorderTimers();
            if (mr.state === 'recording') {
              try { mr.stop(); } catch {}
            }
          }, 30000);

          mr.onerror = (e) => {
            clearMediaRecorderTimers();
            clearTimeout(silenceTimer);
            console.error('[V2V] MediaRecorder error:', e);
            micEnabledRef.current = false;
            setMicEnabled(false);
            setV2vPhase('error');
            setV2vError('Erro no microfone. Verifique permissões.');
          };
        })
        .catch(err => {
          console.warn('[V2V] getUserMedia falhou, fallback SpeechRecognition:', err?.message);
          micEnabledRef.current = false;
          setMicEnabled(false);
          // fallback para SpeechRecognition
          _startSpeechRecognition();
        });
      return;
    }

    // ── Caminho 2: SpeechRecognition (fallback Chrome/Edge) ──
    _startSpeechRecognition();
  }

  function stopMicMediaRecorder() {
    // PATCH0100_24D: não zerar chunks antes do onstop (race condition)
    // BUG-02 FIX: sinalizar stop intencional para que onstop descarte os chunks
    stopIntentionalRef.current = true;
    clearMediaRecorderTimers();
    const mr = mediaRecorderRef.current;
    mediaRecorderRef.current = null;

    const stream = mediaRecorderStreamRef.current;
    mediaRecorderStreamRef.current = null;
    if (stream) {
      try { stream.getTracks().forEach(t => t.stop()); } catch {}
    }

    // NÃO limpar audioChunksRef aqui: o handler onstop consome os chunks.
    if (mr && mr.state === 'recording') {
      try { mr.stop(); } catch {}
    }
  }

  function _startSpeechRecognition() {
    const rec = ensureSpeech();
    if (!rec) {
      setV2vError('Microfone não disponível neste browser. Use Chrome ou ative permissões.');
      micEnabledRef.current = false;
      setMicEnabled(false);
      return;
    }

    let finalText = "";
    let autoSendTimer = null;
    rec.onresult = (evt) => {
      let interim = "";
      for (let i = evt.resultIndex; i < evt.results.length; i++) {
        const transcript = evt.results[i][0].transcript;
        if (evt.results[i].isFinal) finalText += transcript;
        else interim += transcript;
      }
      const merged = (finalText || interim || "").trim();
      if (merged) setText(merged);

      if (voiceModeRef.current && finalText.trim()) {
        if (autoSendTimer) clearTimeout(autoSendTimer);
        autoSendTimer = setTimeout(() => {
          const toSend = finalText.trim();
          if (toSend && voiceModeRef.current) {
            finalText = "";
            try { rec.stop(); } catch {}
            micEnabledRef.current = false;
            setMicEnabled(false);
            sendMessage();
          }
        }, 1500);
      }
    };

    rec.onerror = () => { /* keep enabled; onend will handle retry */ };

    rec.onend = () => {
      if (!micEnabledRef.current) return;
      const now = Date.now();
      const st = micRetryRef.current;
      if (now - st.lastTry > 20000) { st.tries = 0; }
      st.lastTry = now;
      st.tries += 1;
      if (st.tries > 3) {
        micEnabledRef.current = false;
        setMicEnabled(false);
        setUploadStatus("Microfone pausou. Clique no 🎙️ para retomar.");
        setTimeout(() => setUploadStatus(""), 2500);
        return;
      }
      setTimeout(() => {
        if (micEnabledRef.current) { try { rec.start(); } catch {} }
      }, 300);
    };

    try { rec.start(); } catch {}
  }

  function toggleMic() {
    if (!speechSupported) return;
    if (micEnabled) stopMic();
    else startMic();
  }

  // PATCH0100_13: Voice Mode helpers
  function toggleVoiceMode() {
    if (SUMMIT_VOICE_MODE !== "stt_tts") return;
    const next = !voiceMode;
    if (next && realtimeModeRef.current) {
      void stopRealtime('voice_mode_selected');
      setRealtimeMode(false);
      realtimeModeRef.current = false;
    }
    setVoiceMode(next);
    voiceModeRef.current = next;
    if (next) {
      setV2vPhase(null);
      setV2vError(null);
      const canRecord = mediaRecorderSupported;
      if (canRecord && !micEnabled) startMic();
      const modeLabel = 'MediaRecorder + STT';
      setUploadStatus(`Voice mode active (${modeLabel}) — speak naturally and Orkio will answer out loud.`);
      setTimeout(() => setUploadStatus(''), 4000);
    } else {
      if (micEnabled) stopMic();
      stopTts();
      setV2vPhase(null);
      setV2vError(null);
      setUploadStatus('');
    }
  }



function inferInterestType(raw) {
  const s = (raw || "").toLowerCase();
  if (/(invest|aportar|aporte|funding|investor)/i.test(s)) return "investor";
  if (/(comprar|contratar|adquirir|saas|demo|pricing|plan|plano)/i.test(s)) return "sales";
  if (/(parceria|partner|partnership)/i.test(s)) return "partnership";
  return "general";
}

function buildFounderHandoffMessage() {
  const draft = (text || "").trim();
  if (draft) return draft;
  const lastUser = [...(messagesRef.current || [])].reverse().find((m) => m?.role === "user" && (m?.content || "").trim());
  return (lastUser?.content || "The user would like to speak with Daniel about a strategic opportunity.").trim();
}

function handleFounderHandoff() {
  const message = buildFounderHandoffMessage();
  if (!message || handoffBusy) return;
  setHandoffDraft(message);
  setHandoffInterestType(inferInterestType(message));
  setShowHandoffModal(true);
}

async function confirmFounderHandoff() {
  const message = (handoffDraft || buildFounderHandoffMessage()).trim();
  if (!message || handoffBusy) return;
  setHandoffBusy(true);
  setHandoffNotice("");
  try {
    await requestFounderHandoff({
      token,
      org: tenant,
      thread_id: threadId || null,
      interest_type: handoffInterestType || inferInterestType(message),
      message,
      source: "app_console",
      consent_contact: true,
    });
    setShowHandoffModal(false);
    setHandoffDraft("");
    setHandoffNotice("Founder follow-up requested. Daniel will review the context and continue with the right next step.");
    setTimeout(() => setHandoffNotice(""), 6000);
  } catch (e) {
    const detail = typeof e?.message === "string" ? e.message : "Could not request founder follow-up.";
    setHandoffNotice(detail);
    setTimeout(() => setHandoffNotice(""), 6000);
  } finally {
    setHandoffBusy(false);
  }
}



  function clearRealtimeResponseTimeout() {
    if (rtcResponseTimeoutRef.current) {
      try { clearTimeout(rtcResponseTimeoutRef.current); } catch {}
      rtcResponseTimeoutRef.current = null;
    }
  }


function clearRealtimeIdleFollowup() {
  if (rtcIdleFollowupTimerRef.current) {
    try { clearTimeout(rtcIdleFollowupTimerRef.current); } catch {}
    rtcIdleFollowupTimerRef.current = null;
  }
}

function markRealtimeUserActivity() {
  rtcLastUserActivityAtRef.current = Date.now();
  rtcIdleFollowupSentRef.current = false;
  clearRealtimeIdleFollowup();
}

function scheduleRealtimeIdleFollowup() {
  clearRealtimeIdleFollowup();
  if (!REALTIME_IDLE_FOLLOWUP_ENABLED) return;
  if (!realtimeModeRef.current) return;

  const assistantAgentId = destSingle || null;
  const displayName = resolveRealtimeIdleDisplayName(user);
  rtcIdleFollowupTimerRef.current = setTimeout(async () => {
    try {
      if (!realtimeModeRef.current) return;
      if (rtcIdleFollowupSentRef.current) return;
      const idleFor = Date.now() - (rtcLastUserActivityAtRef.current || 0);
      if (idleFor < REALTIME_IDLE_FOLLOWUP_MS) return;

      rtcIdleFollowupSentRef.current = true;
      const prompt = displayName
        ? `${displayName}, você ainda está online? Estou aqui caso queira continuar.`
        : "Você ainda está comigo? Estou aqui caso queira continuar.";

      const mid = `rtc_idle_${Date.now()}_${Math.random().toString(16).slice(2)}`;
      setMessages((prev) => prev.concat([{
        id: mid,
        role: "assistant",
        content: prompt,
        agent_id: assistantAgentId ? String(assistantAgentId) : null,
        agent_name: "Orkio",
        created_at: Math.floor(Date.now()/1000),
      }]));
      queueRealtimeEvent({ event_type: 'response.final', role: 'assistant', content: prompt, is_final: true, meta: { source: 'idle_followup' } });

      try {
        await playTts(prompt, assistantAgentId, { forceAuto: true });
      } catch (err) {
        console.warn("[Realtime] idle follow-up tts failed", err);
      }
    } catch (err) {
      console.warn("[Realtime] idle follow-up failed", err);
    }
  }, REALTIME_IDLE_FOLLOWUP_MS);
}


  async function activateSilentRealtimeFallback(reason = "realtime_fallback", options = {}) {
    const shouldDisarm = options?.disarm !== false;
    if (rtcFallbackActiveRef.current && shouldDisarm) return;
    rtcFallbackActiveRef.current = true;
    clearRealtimeResponseTimeout();
    clearRealtimeIdleFollowup();
    logRealtimeStep("fallback:activate", { reason, shouldDisarm });
    try { await stopRealtime(reason); } catch {}
    if (shouldDisarm) {
      setRealtimeMode(false);
      realtimeModeRef.current = false;
      setV2vPhase("fallback");
      setUploadStatus("Realtime fallback active.");
      setTimeout(() => setUploadStatus(""), 1200);
      try {
        setVoiceMode(true);
        voiceModeRef.current = true;
        if (!micEnabledRef.current) startMic();
      } catch {}
    } else {
      setV2vPhase("error");
      setUploadStatus(`❌ Realtime diagnostic: ${reason}`);
      setTimeout(() => setUploadStatus(""), 2500);
    }
  }

  async function guardAndMaybeBlockRealtimeTranscript(raw) {
    const message = (raw || "").toString().trim();
    if (!message) return false;
    try {
      const res = await guardRealtimeTranscript({ thread_id: rtcThreadIdRef.current || threadId || null, message });
      const payload = res?.data || {};
      if (!payload?.blocked) return false;
      setRtcReadyToRespond(false);
      rtcLastFinalTranscriptRef.current = "";
      queueRealtimeEvent({ event_type: "response.final", role: "assistant", content: payload.reply || "", is_final: true, meta: { source: "server_guard" } });
      commitRealtimeAssistantFinal(payload.reply || "", { source: "server_guard" });
      return true;
    } catch (err) {
      console.warn("[Realtime] guard check failed", err);
      return false;
    }
  }

  async function startRealtime() {
    if (rtcConnectingRef.current) {
      console.warn("[Realtime] start skipped: already connecting");
      logRealtimeStep("start:skip_connecting");
      return;
    }

    if (rtcSessionIdRef.current && rtcPcRef.current && rtcDcRef.current) {
      console.warn("[Realtime] start skipped: active session already present", { sessionId: rtcSessionIdRef.current });
      logRealtimeStep("start:skip_active_session", { sessionId: rtcSessionIdRef.current });
      return;
    }

    rtcConnectingRef.current = true;

    try {
      try { console.log("REALTIME_START_BEGIN", { threadId, destSingle, sessionId: rtcSessionIdRef.current || null }); } catch {}
      logRealtimeStep('start:begin', { threadId, destSingle, summitRuntimeMode: summitRuntimeModeRef.current, summitLanguageProfile: summitLanguageProfileRef.current });
      setV2vError(null);
      setV2vPhase('connecting');
      setUploadStatus('⚡ Conectando Realtime (WebRTC)...');

      if (rtcSessionIdRef.current) {
        await stopRealtime('restart_existing_session');
      } else if (rtcPcRef.current || rtcDcRef.current || rtcAudioElRef.current) {
        try { rtcDcRef.current?.close?.(); } catch {}
        rtcDcRef.current = null;
        try { rtcPcRef.current?.close?.(); } catch {}
        rtcPcRef.current = null;
        try {
          const staleAudio = rtcAudioElRef.current;
          if (staleAudio) {
            try { staleAudio.pause?.(); } catch {}
            try { staleAudio.srcObject = null; } catch {}
            try { staleAudio.remove?.(); } catch {}
          }
        } catch {}
        rtcAudioElRef.current = null;
      }

      try { setRtcAuditEvents([]); } catch {}
      try { setRtcPunctStatus(null); } catch {}
      try { setSummitSessionScore(null); } catch {}


      const agentIdToSend = destSingle || null; // host agent for both single + team (team uses host voice)
      const ORKIO_ENV = (typeof window !== "undefined" && window.__ORKIO_ENV__) ? window.__ORKIO_ENV__ : {};
      const envVoice = (ORKIO_ENV.VITE_REALTIME_VOICE || import.meta.env.VITE_REALTIME_VOICE || "").trim();
      const rtModel = (ORKIO_ENV.VITE_REALTIME_MODEL || import.meta.env.VITE_REALTIME_MODEL || "gpt-realtime-mini").trim();
      const magicEnabled = (ORKIO_ENV.VITE_REALTIME_MAGICWORDS || import.meta.env.VITE_REALTIME_MAGICWORDS || "true").toString().trim().toLowerCase() !== "false";
      rtcMagicEnabledRef.current = magicEnabled;

      // Voice priority: agent.voice_id (Admin) > env default > fallback ("cedar")
      const selectedAgentObj = (agents || []).find(a => String(a.id) === String(agentIdToSend));
      const agentVoice = ((selectedAgentObj?.voice_id || selectedAgentObj?.voice || selectedAgentObj?.tts_voice || selectedAgentObj?.voiceId || "")).toString().trim();
      const rtVoice = coerceVoiceId(agentVoice || envVoice || "cedar");
      rtcVoiceRef.current = rtVoice;

      // PATCH stage-quality: explicit Summit mode without hardcoding contracts in-component
      const runtimeMode = summitRuntimeModeRef.current === "summit" ? "summit" : "platform";
      const languageProfile = (summitLanguageProfileRef.current || "auto").trim() || "auto";
      const start = runtimeMode === "summit"
        ? await startSummitSession({
            agent_id: agentIdToSend,
            thread_id: threadId || null,
            voice: rtVoice,
            model: rtModel,
            ttl_seconds: 600,
            mode: "summit",
            response_profile: "stage",
            language_profile: languageProfile,
          })
        : await startRealtimeSession({ agent_id: agentIdToSend, thread_id: threadId || null, voice: rtVoice, model: rtModel, ttl_seconds: 600 });
      logRealtimeStep('start:session_ok', start);
      const EPHEMERAL_KEY = start?.client_secret?.value || start?.client_secret_value || start?.value || null;
      if (!EPHEMERAL_KEY) {
        logRealtimeStep('start:ephemeral_missing', start);
        throw new Error('Realtime token vazio');
      }
      logRealtimeStep('start:ephemeral_ok', { session_id: start?.session_id || null, thread_id: start?.thread_id || null });

      rtcSessionIdRef.current = start?.session_id || null;
      try { console.log("REALTIME_SESSION_STARTED", { sessionId: start?.session_id || null, threadId: start?.thread_id || threadId || null }); } catch {}
      setLastRealtimeSessionId(start?.session_id || null);
      rtcThreadIdRef.current = start?.thread_id || threadId || null;
      if (start?.thread_id && start.thread_id !== threadId) {
        try { setThreadId(start.thread_id); } catch {}
      }

      rtcEventQueueRef.current = [];
      rtcSeenBackendResponseIdsRef.current = new Set();
      if (rtcFlushTimerRef.current) { try { clearInterval(rtcFlushTimerRef.current); } catch {} }
      rtcFlushTimerRef.current = setInterval(() => { try { flushRealtimeEvents(); } catch {} }, 400);
      startRealtimeLivePoll();

      const pc = new RTCPeerConnection();
      rtcPcRef.current = pc;

      // Remote audio output
      const audioEl = document.createElement('audio');
      audioEl.autoplay = true;
      audioEl.playsInline = true;
      rtcAudioElRef.current = audioEl;
      pc.ontrack = (e) => {
        try {
          audioEl.srcObject = e.streams[0];
          // Ensure element is connected for better autoplay compatibility
          if (!audioEl.isConnected) {
            audioEl.style.display = "none";
            document.body.appendChild(audioEl);
          }
          const p = audioEl.play?.();
          if (p && typeof p.catch === "function") p.catch(() => {});
        } catch {}
      };

      // Mic input
      logRealtimeStep('start:request_mic');
      const micConstraints = {
        audio: {
          channelCount: 1,
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: false,
          sampleRate: 16000,
        },
        video: false,
      };
      const ms = await navigator.mediaDevices.getUserMedia(micConstraints);
      const rawTrack = ms.getAudioTracks?.()[0] || ms.getTracks?.()[0] || null;
      if (!rawTrack) throw new Error("Microfone indisponível");

      let outboundStream = ms;
      let outboundTrack = rawTrack;

      try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        if (AudioCtx) {
          const ctx = new AudioCtx({ sampleRate: 16000, latencyHint: "interactive" });
          const source = ctx.createMediaStreamSource(ms);
          const gainNode = ctx.createGain();
          gainNode.gain.value = 0.75;
          const destination = ctx.createMediaStreamDestination();
          source.connect(gainNode);
          gainNode.connect(destination);
          const processedTrack = destination.stream.getAudioTracks?.()[0] || null;
          if (processedTrack) {
            outboundStream = destination.stream;
            outboundTrack = processedTrack;
            rtcAudioProcessingRef.current = { ctx, source, gainNode, destination, rawStream: ms };
          } else {
            try { ctx.close?.(); } catch {}
          }
        }
      } catch (audioErr) {
        console.warn("[Realtime] mic processing chain unavailable, using raw track", audioErr);
      }

      logRealtimeStep('start:mic_ok', { label: outboundTrack?.label || null, readyState: outboundTrack?.readyState || null });
      outboundTrack.onended = () => {
        try {
          logRealtimeStep("mic:ended");
          if (realtimeModeRef.current) {
            void activateSilentRealtimeFallback("mic_ended");
          }
        } catch {}
      };
      pc.addTrack(outboundTrack, outboundStream);

      // Events channel
      const dc = pc.createDataChannel('oai-events');
      rtcDcRef.current = dc;

      pc.onconnectionstatechange = () => {
        const state = pc.connectionState || "unknown";
        logRealtimeStep("pc:connection_state", { state });
        if (state === "failed" || state === "disconnected" || state === "closed") {
          setV2vError(`Realtime connection ${state}`);
          if (realtimeModeRef.current) void activateSilentRealtimeFallback(`pc_${state}`);
        }
      };

      dc.addEventListener("close", () => {
        logRealtimeStep("dc:close");
        rtcResponseInFlightRef.current = false;
        if (realtimeModeRef.current) {
          setV2vPhase("error");
          setV2vError("Realtime channel closed");
          void activateSilentRealtimeFallback("dc_closed");
        }
      });

      dc.addEventListener("error", (err) => {
        console.warn("[Realtime] datachannel error", err);
        logRealtimeStep("dc:error", { message: err?.message || null });
      });

            dc.addEventListener('open', () => {
        setV2vPhase('listening');
        setUploadStatus('⚡ Realtime ativo — fale normalmente.');
        setTimeout(() => setUploadStatus(''), 1500);

        // Summit language hint is locked to English unless explicitly overridden by env.
        try {
          const envLang = (window.__ORKIO_ENV__?.VITE_REALTIME_TRANSCRIBE_LANGUAGE || import.meta.env.VITE_REALTIME_TRANSCRIBE_LANGUAGE || "").trim();
          const preferredLang = summitRuntimeModeRef.current === "summit" ? (summitLanguageProfileRef.current || envLang || "") : envLang;
          const langHint = resolveRealtimeTranscriptionLanguage(preferredLang);
          const transcription = { model: "gpt-4o-mini-transcribe" };
          if (langHint) transcription.language = langHint;
          dc.send(JSON.stringify({
            type: "session.update",
            session: {
              type: "realtime",
              audio: {
                input: {
                  transcription,
                  turn_detection: {
                    type: "server_vad",
                    silence_duration_ms: REALTIME_SERVER_VAD_SILENCE_MS,
                    prefix_padding_ms: REALTIME_SERVER_VAD_PREFIX_MS,
                    create_response: true
                  }
                }
              }
            }
          }));
        } catch {}
      });

      dc.addEventListener('message', (e) => {
        try {
          const ev = JSON.parse(e.data);

                    // Turn arming + optional Magic Words (B3)
          // server_vad + create_response=true is the source of truth.
          // We do not auto-fire response.create on final transcript here; we wait for the server
          // to emit the response events, while still allowing optional manual / magic-word triggers
          // when explicitly requested by the user.
          if (ev?.type === 'conversation.item.input_audio_transcription.completed') {
            const raw = (ev?.transcript || ev?.text || ev?.result?.transcript || '').toString();
            queueRealtimeEvent({ event_type: 'transcript.final', role: 'user', content: raw, is_final: true });
            try {} catch {}
            rtcLastFinalTranscriptRef.current = raw;
            markRealtimeUserActivity();

            Promise.resolve(guardAndMaybeBlockRealtimeTranscript(raw)).then((blocked) => {
              if (blocked) return;
              setRtcReadyToRespond(!!raw.trim());
              const norm = raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();
              const endsWithCmd = (s, cmd) => s === cmd || s.endsWith(' ' + cmd);
              const isMagic = endsWithCmd(norm, 'continue') || endsWithCmd(norm, 'please') || endsWithCmd(norm, 'prossiga') || endsWithCmd(norm, 'por favor');
              if (rtcMagicEnabledRef.current && isMagic) {
                try {
                  if (rtcLastMagicRef.current !== norm) {
                    rtcLastMagicRef.current = norm;
                    triggerRealtimeResponse("magic");
                  }
                } catch (err) {
                  console.warn('[Realtime] magic trigger failed', err);
                }
              } else if (raw.trim()) {
                setRtcReadyToRespond(false);
                logRealtimeStep('runtime:awaiting_server_auto_response', {
                  transcript: raw,
                });
              }
            });
          }
// Basic telemetry + optional live captions
          if (ev?.type === 'response.text.delta' && ev?.delta) {
            clearRealtimeResponseTimeout();
            rtcTextBufRef.current += ev.delta;
          }
          if (ev?.type === 'response.created') {
            clearRealtimeResponseTimeout();
            rtcResponseInFlightRef.current = true;
            setV2vPhase('responding');
            rtcTextBufRef.current = '';
            rtcAudioTranscriptBufRef.current = '';
            rtcLastAssistantFinalRef.current = '';
            rtcAssistantFinalCommittedRef.current = false;
          }
          if (ev?.type === 'response.output_item.added') {
            clearRealtimeResponseTimeout();
          }
          if (ev?.type === 'response.content_part.added') {
            clearRealtimeResponseTimeout();
          }
          if (ev?.type === 'response.text.done') {
            clearRealtimeResponseTimeout();
            rtcResponseInFlightRef.current = false;
            const t = (rtcTextBufRef.current || '').trim();
            rtcTextBufRef.current = '';
            rtcAudioTranscriptBufRef.current = '';
            if (t && !rtcAssistantFinalCommittedRef.current) {
              logRealtimeStep('runtime:response_finalized', { source: 'response.text.done', finalText: t });
              commitRealtimeAssistantFinal(t, { source: 'response.text.done' });
            }
          }
          // Audio transcript (when model outputs audio without text)
          if (ev?.type === 'response.audio.delta') {
            clearRealtimeResponseTimeout();
          }
          if (ev?.type === 'response.audio_transcript.delta' && ev?.delta) {
            clearRealtimeResponseTimeout();
            rtcAudioTranscriptBufRef.current = (rtcAudioTranscriptBufRef.current || '') + ev.delta;
          }
          if (ev?.type === 'response.audio_transcript.done' || ev?.type === 'response.audio_transcript.final') {
            clearRealtimeResponseTimeout();
            rtcResponseInFlightRef.current = false;
            const at = ((rtcAudioTranscriptBufRef.current || '') + (ev?.transcript || '')).trim();
            rtcAudioTranscriptBufRef.current = '';
            if (at && !rtcAssistantFinalCommittedRef.current) {
              logRealtimeStep('runtime:response_finalized', { source: 'response.audio_transcript', finalText: at });
              commitRealtimeAssistantFinal(at, { source: 'response.audio_transcript' });
            }
          }

          if (ev?.type === 'response.output_item.done') {
            clearRealtimeResponseTimeout();
            logRealtimeStep('runtime:response_output_item_done', {
              item_id: ev?.item?.id || ev?.item_id || null,
            });
          }

          if (ev?.type === 'response.done') {
            clearRealtimeResponseTimeout();
            rtcResponseInFlightRef.current = false;

            const textFinal = (rtcTextBufRef.current || '').trim();
            const audioFinal = ((rtcAudioTranscriptBufRef.current || '') + (ev?.transcript || '')).trim();
            const finalText = textFinal || audioFinal || '';

            rtcTextBufRef.current = '';
            rtcAudioTranscriptBufRef.current = '';

            if (finalText && !rtcAssistantFinalCommittedRef.current) {
              logRealtimeStep('runtime:response_finalized', {
                source: textFinal ? 'response.done:text_fallback' : 'response.done:audio_fallback',
                finalText,
              });
              commitRealtimeAssistantFinal(finalText, { source: 'response.done' });
            } else {
              logRealtimeStep('runtime:response_done_without_text', {
                source: 'response.done',
                textBuf: textFinal.length,
                audioTranscriptBuf: audioFinal.length,
              });
            }
          }

          if (ev?.type === 'error') {
            clearRealtimeResponseTimeout();
            rtcResponseInFlightRef.current = false;
            console.warn('[Realtime] error', ev);
            logRealtimeStep('runtime:error_event', ev);
            setV2vError(ev?.error?.message || 'Erro Realtime');
            setV2vPhase('error');
            void activateSilentRealtimeFallback('realtime_error', { disarm: false });
          }
        } catch (err) {
          console.warn('[Realtime] DataChannel handler error', err, e?.data);
          logRealtimeStep('runtime:message_handler_error', {
            message: err?.message || null,
            raw: e?.data || null,
          });
        }
      });

      // SDP handshake
      logRealtimeStep('start:create_offer');
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      logRealtimeStep('start:local_description_set', { sdpLength: offer?.sdp?.length || 0 });

      const sdpResponse = await fetch('https://api.openai.com/v1/realtime/calls', {
        method: 'POST',
        body: offer.sdp,
        headers: {
          Authorization: `Bearer ${EPHEMERAL_KEY}`,
          'Content-Type': 'application/sdp',
        },
      });

      const sdpText = await sdpResponse.text().catch(() => '');
      if (!sdpResponse.ok) {
        logRealtimeStep('start:sdp_error', { status: sdpResponse.status, body: sdpText || sdpResponse.statusText });
        throw new Error(`SDP handshake falhou (${sdpResponse.status}): ${sdpText || sdpResponse.statusText}`);
      }

      logRealtimeStep('start:sdp_ok', { answerLength: sdpText.length });
      const answer = { type: 'answer', sdp: sdpText };
      await pc.setRemoteDescription(answer);
      logRealtimeStep('start:ready', { sessionId: start?.session_id || null, threadId: start?.thread_id || threadId || null });

    } catch (e) {
      console.error('[Realtime] startRealtime error', e);
      logRealtimeStep('start:catch', {
        message: e?.message || 'Falha ao iniciar Realtime',
        stack: e?.stack || null,
        sessionId: rtcSessionIdRef.current || null,
        threadId: rtcThreadIdRef.current || threadId || null,
      });
      setV2vPhase('error');
      setV2vError(e?.message || 'Falha ao iniciar Realtime');
      setUploadStatus('❌ Realtime: ' + (e?.message || 'falha'));
      setTimeout(() => setUploadStatus(''), 4000);
      await stopRealtime('start_error_diagnostic_cleanup');
    } finally {
      rtcConnectingRef.current = false;
    }
  }

  
  function triggerRealtimeResponse(reason = "manual") {
    try {
      const dc = rtcDcRef.current;
      if (!dc || dc.readyState !== "open") {
        throw new Error("DataChannel não está aberto");
      }
      if (rtcResponseInFlightRef.current) {
        logRealtimeStep("response:skip_inflight", { reason });
        return;
      }
      const lastTranscript = (rtcLastFinalTranscriptRef.current || "").trim();
      if (!lastTranscript) {
        logRealtimeStep("response:skip_empty", { reason });
        return;
      }
      rtcResponseInFlightRef.current = true;
      clearRealtimeResponseTimeout();
      clearRealtimeIdleFollowup();
      rtcResponseTimeoutRef.current = setTimeout(() => {
        setUploadStatus("⌛ Realtime ainda processando...");
        setTimeout(() => setUploadStatus(""), 1200);
      }, 7000);
      dc.send(JSON.stringify({ type: "response.create", response: { output_modalities: ["audio", "text"], audio: { output: { voice: rtcVoiceRef.current } } } }));
      setRtcReadyToRespond(false);
      setV2vPhase("responding");
      setUploadStatus(reason === "magic" ? "✨ Command received — responding..." : reason === "auto_vad" ? "🎙️ Speech detected — responding..." : "▶️ Responding...");
      setTimeout(() => setUploadStatus(""), 1500);
    } catch (e) {
      rtcResponseInFlightRef.current = false;
      console.warn("[Realtime] triggerRealtimeResponse failed", e);
      setUploadStatus("❌ Failed to trigger realtime response.");
      setTimeout(() => setUploadStatus(""), 2000);
      void activateSilentRealtimeFallback("trigger_failed");
    }
  }


  // PATCH0100_27A: Realtime event logging (batched, non-blocking)
  function queueRealtimeEvent({ event_type, role, content = null, is_final = false, meta = null } = {}) {
    const sid = rtcSessionIdRef.current;
    if (!sid) return;
    rtcEventQueueRef.current.push({
      session_id: sid,
      client_event_id: (globalThis.crypto && crypto.randomUUID) ? crypto.randomUUID() : (`ce-${Date.now()}-${Math.random().toString(36).slice(2,10)}`),
      event_type,
      role,
      content,
      created_at: Math.floor(Date.now()/1000),
      is_final,
      meta,
    });
    try {
      if (is_final && (content || '').toString().trim()) {
        const item = {
          session_id: sid,
          event_type,
          role,
          content: (content || '').toString(),
          transcript_punct: null,
          created_at: Math.floor(Date.now()/1000),
        };
        setRtcAuditEvents(prev => prev.concat([item]));
      }
    } catch {}
  }

  async function flushRealtimeEvents() {
    const sid = rtcSessionIdRef.current;
    if (!sid) return;
    const q = rtcEventQueueRef.current || [];
    if (!q.length) return;
    // Take a snapshot to avoid races
    rtcEventQueueRef.current = [];
    try {
      await postRealtimeEventsBatch({ session_id: sid, events: q });
    } catch (err) {
      // On failure, put events back to try later (best-effort)
      rtcEventQueueRef.current = q.concat(rtcEventQueueRef.current || []);
      console.warn('[Realtime] events batch failed', err);
    }
  }



  function clearRealtimeLivePoll() {
    if (rtcLivePollTimerRef.current) {
      try { clearInterval(rtcLivePollTimerRef.current); } catch {}
      rtcLivePollTimerRef.current = null;
    }
  }

  function safeParseRealtimeMeta(meta) {
    if (!meta) return {};
    if (typeof meta === "object") return meta;
    try { return JSON.parse(meta); } catch { return {}; }
  }

  async function handleBackendRealtimeAssistantResponses(payload) {
    const sid = rtcSessionIdRef.current;
    if (!sid) return;

    const events = Array.isArray(payload?.events) ? payload.events : [];
    if (events.length) {
      setRtcAuditEvents(events);
    }

    const candidateEvents = events.filter((ev) => {
      const eventType = String(ev?.event_type || "").trim();
      const speakerType = String(ev?.speaker_type || ev?.role || "").trim().toLowerCase();
      return (
        (eventType === "response.final" || eventType === "transcript.final")
        && speakerType === "agent"
      );
    });

    for (const ev of candidateEvents) {
      const evId = String(ev?.id || "");
      if (!evId || rtcSeenBackendResponseIdsRef.current.has(evId)) continue;

      const meta = safeParseRealtimeMeta(ev?.meta);
      const content =
        String(
          ev?.transcript_punct ||
          ev?.transcript_raw ||
          ev?.content ||
          ""
        ).trim();

      if (!content) continue;

      rtcSeenBackendResponseIdsRef.current.add(evId);

      const agentName = String(ev?.agent_name || meta?.agent_name || "Orkio").trim() || "Orkio";
      const agentId = ev?.agent_id || ev?.speaker_id || meta?.agent_id || null;
      const resolvedVoice = resolveAgentVoice({
        agent_name: agentName,
        voice_id: ev?.voice_id || meta?.voice_id || null,
      });

      setMessages((prev) => {
        const exists = (prev || []).some((m) => String(m?.id || "") === evId);
        if (exists) return prev;
        return (prev || []).concat([{
          id: evId,
          role: "assistant",
          content,
          agent_id: agentId ? String(agentId) : null,
          agent_name: agentName,
          voice_id: resolvedVoice,
          created_at: Math.floor(Date.now() / 1000),
        }]);
      });

      setUploadStatus(`📝 ${agentName}: ${content.slice(0, 80)}${content.length > 80 ? '…' : ''}`);
      setTimeout(() => setUploadStatus(''), 2200);

      try {
        await playTts(content, agentId, {
          forceAuto: true,
          messageId: null,
          traceId: v2vTraceRef.current || null,
          voiceOverride: resolvedVoice,
        });
      } catch (err) {
        console.warn("[Realtime] backend response TTS failed", err);
      }
    }
  }

  function startRealtimeLivePoll() {
    clearRealtimeLivePoll();
    const sid = rtcSessionIdRef.current;
    if (!sid) return;

    rtcLivePollTimerRef.current = setInterval(async () => {
      try {
        if (!realtimeModeRef.current || !rtcSessionIdRef.current) return;
        const data = await getRealtimeSession({ session_id: sid, finals_only: true });
        await handleBackendRealtimeAssistantResponses(data || {});
      } catch (err) {
        console.warn("[Realtime] live poll failed", err);
      }
    }, 1400);
  }

  // PATCH0100_27_2B: finalize session on server + poll punctuated finals (best-effort)
  async function finalizeRealtimeSession(reason = 'client_stop') {
    const sid = rtcSessionIdRef.current;
    if (!sid) return;
    // stop timers
    if (rtcFlushTimerRef.current) { try { clearInterval(rtcFlushTimerRef.current); } catch {} rtcFlushTimerRef.current = null; }
    clearRealtimeLivePoll();
    // flush pending events
    try { await flushRealtimeEvents(); } catch {}
    // end session (best-effort)
    try { await endRealtimeSession({ session_id: sid, ended_at: Date.now(), meta: { reason } }); } catch {}

    // poll for punct updates (best-effort, bounded)
    try {
      setRtcPunctStatus('pending');
      const started = Date.now();
      const deadlineMs = 15000;
      let last = null;
      while (Date.now() - started < deadlineMs) {
        try {
          const data = await getRealtimeSession({ session_id: sid, finals_only: true });
          last = data;
          if (data?.events) {
            setRtcAuditEvents(data.events);
          }
          await handleBackendRealtimeAssistantResponses(data || {});
          if (data?.punct?.done) {
            setRtcPunctStatus('done');
            return;
          }
        } catch {}
        await new Promise(r => setTimeout(r, 900));
      }
      // timeout but still set last snapshot
      if (last?.events) setRtcAuditEvents(last.events);
      setRtcPunctStatus('timeout');
    } catch {
      setRtcPunctStatus('timeout');
    }
  }

  function commitRealtimeAssistantFinal(rawText, { source = 'unknown' } = {}) {
    const finalText = (rawText || '').toString().trim();
    if (!finalText) return;
    const dedupeKey = finalText
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .trim();

    const sourceKey = String(source || '').trim().toLowerCase();
    const canonicalSources = new Set(['response.text.done', 'response.done', 'server_guard']);

    if (rtcLastAssistantFinalRef.current === dedupeKey) return;

    if (rtcAssistantFinalCommittedRef.current) {
      if (!canonicalSources.has(sourceKey)) return;
      if (sourceKey === 'response.done') return;
    }

    rtcLastAssistantFinalRef.current = dedupeKey;
    rtcAssistantFinalCommittedRef.current = true;

    queueRealtimeEvent({ event_type: 'response.final', role: 'assistant', content: finalText, is_final: true, meta: { source } });
    try {
      const selectedAgentObj2 = (agents || []).find(a => String(a.id) === String(destSingle || ""));
      const metaAgentName = source && String(source).includes(":") ? String(source).split(":")[1].trim() : "";
      const agentName2 = metaAgentName || selectedAgentObj2?.name || "Orkio";
      const agentId2 = selectedAgentObj2?.id || (destSingle || null);
      const mid = `rtc_ass_${Date.now()}_${Math.random().toString(16).slice(2)}`;
      setMessages((prev) => prev.concat([{
        id: mid,
        role: "assistant",
        content: finalText,
        agent_id: agentId2 ? String(agentId2) : null,
        agent_name: agentName2,
        created_at: Math.floor(Date.now()/1000),
      }]));
    } catch {}

    setUploadStatus('📝 ' + finalText.slice(0, 80) + (finalText.length > 80 ? '…' : ''));
    setTimeout(() => setUploadStatus(''), 2500);
    setTimeout(() => { try { scheduleRealtimeIdleFollowup(); } catch {} }, REALTIME_REARM_AFTER_ASSISTANT_MS);
  }


  async function downloadRealtimeAta() {
    try {
      const sid = rtcSessionIdRef.current;
      if (!sid) {
        setUploadStatus('ℹ️ Nenhuma sessão Realtime disponível para exportar relatório.');
        setTimeout(() => setUploadStatus(''), 2000);
        return;
      }
      const blob = await downloadRealtimeAtaFile({ session_id: sid });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `orkio-ata-${sid}.txt`;
      a.rel = 'noopener';
      document.body.appendChild(a);
      a.click();
      a.remove();
      try { URL.revokeObjectURL(url); } catch {}
      setUploadStatus('⬇️ Baixando relatório executivo da sessão...');
      setTimeout(() => setUploadStatus(''), 1800);
    } catch (e) {
      console.error('[Realtime] download report failed', e);
      setUploadStatus('❌ Falha ao baixar ata.');
      setTimeout(() => setUploadStatus(''), 2000);
    }
  }

async function stopRealtime(reason = 'client_stop') {
    const sid = rtcSessionIdRef.current;
    try {
      console.log("REALTIME_STOP_REASON", reason, { sessionId: sid });
    } catch {}
    rtcConnectingRef.current = false;
    try {
      clearRealtimeResponseTimeout();
      clearRealtimeIdleFollowup();
      rtcFallbackActiveRef.current = false;
      if (rtcFlushTimerRef.current) { try { clearInterval(rtcFlushTimerRef.current); } catch {} rtcFlushTimerRef.current = null; }
      clearRealtimeLivePoll();

      try {
        if (sid) {
          await flushRealtimeEvents();
          await endRealtimeSession({ session_id: sid, ended_at: Date.now(), meta: { reason, mode: summitRuntimeModeRef.current } });
          try {
            const data = await getRealtimeSession({ session_id: sid, finals_only: true });
            if (data?.events) setRtcAuditEvents(data.events);
            await handleBackendRealtimeAssistantResponses(data || {});
          } catch {}
          try {
            if (summitRuntimeModeRef.current === "summit") {
              const scoreRes = await getSummitSessionScore({ session_id: sid });
              setSummitSessionScore(scoreRes?.data?.score || null);
            }
          } catch {}
        }
      } catch (err) {
        console.warn('[Realtime] stop finalize failed', err);
      }

      const dc = rtcDcRef.current;
      rtcDcRef.current = null;
      if (dc) { try { dc.close(); } catch {} }

      const pc = rtcPcRef.current;
      rtcPcRef.current = null;
      if (pc) {
        try { pc.getSenders?.().forEach((sender) => { try { sender.track?.stop?.(); } catch {} }); } catch {}
        try { pc.getReceivers?.().forEach((receiver) => { try { receiver.track?.stop?.(); } catch {} }); } catch {}
        try { pc.close(); } catch {}
      }

      const a = rtcAudioElRef.current;
      rtcAudioElRef.current = null;
      if (a) {
        try { a.pause(); } catch {}
        try { a.srcObject = null; } catch {}
        try { if (a.isConnected) a.remove(); } catch {}
      }

      rtcTextBufRef.current = '';
      rtcAudioTranscriptBufRef.current = '';
      rtcAssistantFinalCommittedRef.current = false;
      rtcLastAssistantFinalRef.current = '';
      rtcResponseInFlightRef.current = false;
      rtcSeenBackendResponseIdsRef.current = new Set();

      const processing = rtcAudioProcessingRef.current;
      rtcAudioProcessingRef.current = null;
      if (processing) {
        try { processing.destination?.stream?.getTracks?.().forEach((t) => { try { t.stop?.(); } catch {} }); } catch {}
        try { processing.rawStream?.getTracks?.().forEach((t) => { try { t.stop?.(); } catch {} }); } catch {}
        try { processing.ctx?.close?.(); } catch {}
      }
    } catch {}
  }


  async function submitStageReview(clarity, naturalness, institutionalFit) {
    const sid = rtcSessionIdRef.current || lastRealtimeSessionId || null;
    const targetSid = sid || lastRealtimeSessionId;
    if (!targetSid) return;
    try {
      setSummitReviewPending(true);
      const res = await submitSummitSessionReview({
        session_id: targetSid,
        clarity,
        naturalness,
        institutional_fit: institutionalFit,
      });
      try {
        const scoreRes = await getSummitSessionScore({ session_id: targetSid });
        setSummitSessionScore(scoreRes?.data?.score || { human_review: res?.data?.review || null });
      } catch {
        setSummitSessionScore((prev) => ({ ...(prev || {}), human_review: res?.data?.review || null }));
      }
      setUploadStatus("✅ Avaliação do Summit registrada.");
      setTimeout(() => setUploadStatus(""), 1800);
    } catch (err) {
      console.warn("[Summit] review failed", err);
    } finally {
      setSummitReviewPending(false);
    }
  }

  function toggleRealtimeMode() {
    if (SUMMIT_VOICE_MODE !== "realtime") return;
    const next = !realtimeMode;
    setRealtimeMode(next);
    realtimeModeRef.current = next;

    if (next) {
      // Disable classic voice mode to avoid mic contention
      if (voiceModeRef.current) {
        setVoiceMode(false);
        voiceModeRef.current = false;
      }
      try { stopMic(); } catch {}
      try { stopTts(); } catch {}
      startRealtime();
    } else {
      void stopRealtime('toggle_off');
      setV2vPhase(null);
      setV2vError(null);
      setUploadStatus('');
    }
  }

  function stopTts() {
    if (ttsAudioRef.current) {
      ttsAudioRef.current.pause();
      ttsAudioRef.current = null;
    }
    setTtsPlaying(false);
  }

  async function playTts(textToSpeak, agentId, opts = {}) {
    // F-01 FIX: desestruturar opts no início da função
    const { forceAuto = false, messageId = null, traceId = null, voiceOverride = null } = opts || {};
    if (!textToSpeak || textToSpeak.length < 2) return;
    // Evitar reler a mesma mensagem (idempotência)
    if (messageId && messageId === lastSpokenMessageIdRef.current) return;
    if (!messageId && textToSpeak === lastSpokenMsgRef.current) return;
    lastSpokenMessageIdRef.current = messageId || null;
    lastSpokenMsgRef.current = textToSpeak;

    // Limpar markdown para fala mais natural
    let clean = textToSpeak
      .replace(/```[\s\S]*?```/g, ' código omitido ')
      .replace(/`[^`]+`/g, '')
      .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
      .replace(/[#*_~>|]/g, '')
      .replace(/\n{2,}/g, '. ')
      .replace(/\n/g, ' ')
      .trim();
    if (voiceModeRef.current) {
      if (clean.length > 1200) clean = clean.slice(0, 1200);
    } else {
      if (clean.length > 4096) clean = clean.slice(0, 4096);
    }
    if (clean.length < 2) return;

    stopTts();
    setTtsPlaying(true);
    setV2vPhase('playing');

    const effectiveTrace = traceId || v2vTraceRef.current || null;
    console.info('[V2V] v2v_play_start trace_id=%s message_id=%s agent_id=%s', effectiveTrace, messageId, agentId);

    try {
      const base = (window.__ORKIO_ENV__?.VITE_API_BASE_URL || import.meta.env.VITE_API_BASE_URL || '').trim().replace(/\/$/, '');
      const apiUrl = base.endsWith('/api') ? base.slice(0, -4) : base;

      const ttsHeaders = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
        'X-Org-Slug': tenant,
      };
      if (effectiveTrace) ttsHeaders['X-Trace-Id'] = effectiveTrace;

      const res = await fetch(`${apiUrl}/api/tts`, {
        method: 'POST',
        headers: ttsHeaders,
        // V2V-PATCH: preferir message_id (backend resolve voz correta por agente)
        // agent_id só como fallback se message_id não disponível
        body: JSON.stringify({
          text: clean,
          voice: voiceOverride ? resolveAgentVoice({ voice_id: voiceOverride }) : ((forceAuto || messageId) ? null : (ttsVoice === "auto" ? null : ttsVoice)),
          speed: 1.0,
          agent_id: messageId ? null : (agentId || null),
          message_id: messageId || null,
        }),
      });

      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        console.warn('[V2V] v2v_tts_fail trace_id=%s status=%d body=%s', effectiveTrace, res.status, errText.slice(0, 200));
        setTtsPlaying(false);
        setV2vPhase('error');
        setV2vError(`TTS falhou (HTTP ${res.status})`);
        if (res.status === 401) {
          alert("Sessão expirada. Faça login novamente.");
          try { localStorage.removeItem("orkio_token"); } catch (_) {}
          window.location.href = "/auth";
        }
        return;
      }

      const blob = await res.blob();
      if (!blob || blob.size < 50) {
        console.warn('[V2V] v2v_tts_fail trace_id=%s reason=empty_blob size=%d', effectiveTrace, blob?.size);
        setTtsPlaying(false);
        setV2vPhase('error');
        setV2vError('TTS retornou áudio vazio');
        return;
      }

      console.info('[V2V] v2v_tts_ok trace_id=%s bytes=%d', effectiveTrace, blob.size);
      const url = URL.createObjectURL(blob);
      const audio = new Audio(url);
      ttsAudioRef.current = audio;

      await new Promise((resolve, reject) => {
        audio.onended = () => {
          console.info('[V2V] v2v_play_end trace_id=%s', effectiveTrace);
          setTtsPlaying(false);
          setV2vPhase(null);
          URL.revokeObjectURL(url);
          ttsAudioRef.current = null;
          // Reiniciar microfone após fala (ciclo V2V)
          if (voiceModeRef.current && (speechSupported || mediaRecorderSupported) && !micEnabledRef.current) {
            scheduleMicRestart('tts_end', 0);
          }
          resolve();
        };
        audio.onerror = (err) => {
          console.error('[V2V] audio.onerror trace_id=%s', effectiveTrace, err);
          setTtsPlaying(false);
          setV2vPhase('error');
          setV2vError('Erro ao reproduzir áudio');
          URL.revokeObjectURL(url);
          ttsAudioRef.current = null;
          reject(new Error('Audio playback error'));
        };
        audio.play().catch(err => {
          // autoplay bloqueado pelo browser — fallback silencioso
          console.warn('[V2V] autoplay blocked trace_id=%s:', effectiveTrace, err?.message);
          setTtsPlaying(false);
          setV2vPhase(null);
          URL.revokeObjectURL(url);
          ttsAudioRef.current = null;
          // BUG-01 FIX: reiniciar mic mesmo sem áudio — ciclo V2V não pode morrer aqui
          if (voiceModeRef.current && !micEnabledRef.current) {
            scheduleMicRestart('tts_autoplay_blocked', 300);
          }
          resolve(); // não rejeitar — V2V deve continuar mesmo sem áudio
        });
      });
    } catch (e) {
      console.error('[V2V] v2v_tts_fail trace_id=%s error:', effectiveTrace, e);
      setTtsPlaying(false);
      setV2vPhase('error');
      setV2vError(e?.message || 'Erro desconhecido no TTS');
    }
  }

  function changeTtsVoice(v) {
    setTtsVoice(v);
    localStorage.setItem('orkio_tts_voice', v);
  }

  // Upload flow
  function onPickFile(e) {
    const f = e.target.files?.[0];
    if (!f) return;
    e.target.value = "";
    setUploadFileObj(f);
    setUploadScope("thread");
    setUploadAgentIds([]);
    setUploadOpen(true);
  }

  async function confirmUpload() {
    const f = uploadFileObj;
    if (!f) return;
    // PATCH0100_17_ENSURE_THREAD_BEFORE_UPLOAD: uploads need a thread to be visible in chat
    let effectiveThreadId = threadId;
    if (!effectiveThreadId && (uploadScope === "thread" || uploadScope === "institutional")) {
      try {
        const created = await apiFetch("/api/threads", { method: "POST", token, org: tenant, body: { title: "Nova conversa" }});
        effectiveThreadId = created?.data?.id;
        if (effectiveThreadId) setThreadId(effectiveThreadId);
      } catch (e) {
        console.warn("could not create thread before upload", e);
      }
    }

    try {
      setUploadProgress(true);
      setUploadStatus("Enviando arquivo...");

      if (uploadScope === "thread") {
        console.info("[Upload] start", { scope: "thread", filename: f?.name, threadId: effectiveThreadId, size: f?.size || null });
        await uploadFile(f, { token, org: tenant, threadId: effectiveThreadId, intent: "chat" });
        setUploadStatus("Arquivo anexado à conversa ✅");
        try { await loadMessages(effectiveThreadId); } catch {}
      } else if (uploadScope === "agents") {
        console.info("[Upload] start", { scope: "agents", filename: f?.name, agentIds: uploadAgentIds, size: f?.size || null });
        if (!uploadAgentIds.length) {
          alert("Selecione ao menos um agente.");
          return;
        }
        await uploadFile(f, { token, org: tenant, agentIds: uploadAgentIds, intent: "agent" });
        setUploadStatus("Arquivo vinculado aos agentes ✅");
      } else if (uploadScope === "institutional") {
        console.info("[Upload] start", { scope: "institutional", filename: f?.name, threadId: effectiveThreadId, size: f?.size || null });
        const admin = isAdmin(user);
        if (admin) {
          await uploadFile(f, { token, org: tenant, threadId: effectiveThreadId, intent: "institutional", linkAllAgents: true });
          setUploadStatus("Arquivo institucional (global) ✅");
          // STAB: reload com effectiveThreadId para garantir que mensagem system aparece
          try {
            if (effectiveThreadId) await loadMessages(effectiveThreadId);
          } catch (e) { console.warn("loadMessages after institutional upload failed:", e); }
        } else {
          // B2: request institutionalization; keep accessible in this thread
          await uploadFile(f, { token, org: tenant, threadId: effectiveThreadId, intent: "chat", institutionalRequest: true });
          setUploadStatus("Solicitação enviada ao admin (institucional) ✅");
          try { await loadMessages(effectiveThreadId); } catch {}
        }
      }

      setUploadOpen(false);
      setUploadFileObj(null);
      setTimeout(() => setUploadStatus(""), 2200);
    } catch (e) {
      console.error("upload error", e);
      console.warn("[Upload] failed", {
        scope: uploadScope,
        filename: f?.name || uploadFileObj?.name || null,
        message: e?.message || null,
      });
      setUploadStatus(e?.message || "Falha no upload");
      setTimeout(() => setUploadStatus(""), 2500);
    } finally {
      setUploadProgress(false);
    }
  }

  const styles = {
    layout: {
      display: "flex",
      minHeight: "100dvh",
      background:
        "radial-gradient(1200px 700px at 30% -10%, rgba(124,92,255,0.25), transparent 60%), linear-gradient(180deg, #05060a, #03030a)",
      color: "#fff",
      fontFamily: "system-ui",
    },
    sidebar: {
      width: "330px",
      borderRight: "1px solid rgba(255,255,255,0.08)",
      display: "flex",
      flexDirection: "column",
      padding: "16px",
      gap: "12px",
    },
    brand: { fontSize: "18px", fontWeight: 800, letterSpacing: "-0.02em" },
    badge: {
      display: "inline-flex",
      alignItems: "center",
      gap: "8px",
      padding: "6px 10px",
      borderRadius: "999px",
      fontSize: "12px",
      border: "1px solid rgba(255,255,255,0.1)",
      background: "rgba(255,255,255,0.04)",
      color: "rgba(255,255,255,0.8)",
    },
    topRow: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "12px" },
    newThreadBtn: {
      display: "inline-flex",
      alignItems: "center",
      gap: "8px",
      padding: "10px 12px",
      borderRadius: "14px",
      border: "1px solid rgba(255,255,255,0.1)",
      background: "rgba(255,255,255,0.05)",
      color: "#fff",
      cursor: "pointer",
    },
    threads: { flex: 1, overflowY: "auto", padding: "0 8px" },
    emptyThreads: { padding: "20px", textAlign: "center", color: "rgba(255,255,255,0.3)", fontSize: "13px" },
    threadItem: {
      display: "flex",
      alignItems: "center",
      gap: "10px",
      width: "100%",
      padding: "12px",
      background: "transparent",
      border: "none",
      borderRadius: "10px",
      color: "rgba(255,255,255,0.7)",
      fontSize: "13px",
      cursor: "pointer",
      textAlign: "left",
      marginBottom: "4px",
    },
    threadItemActive: { background: "rgba(255,255,255,0.1)", color: "#fff" },
    threadTitle: { flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
    threadEditBtn: {
      border: "none",
      background: "transparent",
      color: "rgba(255,255,255,0.55)",
      padding: "4px",
      borderRadius: "8px",
      cursor: "pointer",
    },
    userSection: {
      padding: "16px",
      borderTop: "1px solid rgba(255,255,255,0.08)",
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "10px",
    },
    userInfo: { display: "flex", alignItems: "center", gap: "10px" },
    userAvatar: {
      width: "36px",
      height: "36px",
      borderRadius: "50%",
      background: "linear-gradient(135deg, #7c5cff 0%, #35d0ff 100%)",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      fontWeight: 800,
    },
    userDetails: { display: "flex", flexDirection: "column" },
    userName: { fontSize: "13px", fontWeight: 700 },
    userEmail: { fontSize: "12px", color: "rgba(255,255,255,0.55)" },
    userActions: { display: "flex", alignItems: "center", gap: "8px" },
    iconBtn: {
      width: "36px",
      height: "36px",
      borderRadius: "12px",
      border: "1px solid rgba(255,255,255,0.1)",
      background: "rgba(255,255,255,0.05)",
      color: "#fff",
      cursor: "pointer",
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
    },

    main: { flex: 1, display: "flex", flexDirection: "column" },
    topbar: {
      padding: "16px 18px",
      borderBottom: "1px solid rgba(255,255,255,0.08)",
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "10px",
    },
    title: { fontSize: "16px", fontWeight: 900 },
    health: { fontSize: "12px", color: "rgba(255,255,255,0.6)" },
    chatArea: { flex: 1, overflowY: "auto", padding: "16px 18px" },
    messageRow: { display: "flex", marginBottom: "12px" },
    messageBubble: {
      maxWidth: "820px",
      padding: "12px 12px",
      borderRadius: "16px",
      border: "1px solid rgba(255,255,255,0.1)",
      background: "rgba(255,255,255,0.04)",
    },
    userBubble: { background: "rgba(124,92,255,0.12)", border: "1px solid rgba(124,92,255,0.25)" },
    agentBubble: { background: "rgba(53,208,255,0.10)", border: "1px solid rgba(53,208,255,0.22)" },
    systemBubble: { background: "rgba(255,255,255,0.02)", border: "1px dashed rgba(255,255,255,0.18)" },
    bubbleHeaderRow: { display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: "12px", marginBottom: "6px" },
    bubbleHeaderName: { fontSize: "12px", color: "rgba(255,255,255,0.70)", fontWeight: 900 },
    bubbleHeaderTime: { fontSize: "12px", color: "rgba(255,255,255,0.55)", fontWeight: 700 },
    nameUser: { color: "rgba(196,176,255,0.95)" },
    nameAgent: { color: "rgba(160,240,255,0.95)" },
    nameSystem: { color: "rgba(255,255,255,0.82)" },
    messageContent: { whiteSpace: "pre-wrap", lineHeight: 1.45, fontSize: "14px" },
    messageTime: { marginTop: "8px", fontSize: "11px", color: "rgba(255,255,255,0.55)" },

    uploadStatus: {
      padding: "10px 18px",
      fontSize: "13px",
      color: "rgba(255,255,255,0.85)",
      borderTop: "1px solid rgba(255,255,255,0.08)",
      background: "rgba(255,255,255,0.03)",
    },

    realtimeAudit: {
      padding: "10px 18px",
      fontSize: "12px",
      color: "rgba(255,255,255,0.82)",
      borderTop: "1px solid rgba(255,255,255,0.08)",
      background: "rgba(80,160,255,0.06)",
      maxHeight: "220px",
      overflowY: "auto",
    },
    realtimeAuditHeader: { display: "flex", alignItems: "center", justifyContent: "space-between", gap: "10px", marginBottom: "8px" },
    realtimeAuditTitle: { fontWeight: 900, letterSpacing: "0.2px" },
    realtimeAuditPill: { padding: "2px 8px", borderRadius: "999px", border: "1px solid rgba(255,255,255,0.16)", background: "rgba(255,255,255,0.05)", fontSize: "11px" },
    realtimeAuditItem: { padding: "8px 10px", borderRadius: "12px", border: "1px solid rgba(255,255,255,0.10)", background: "rgba(255,255,255,0.03)", marginBottom: "8px" },
    realtimeAuditMeta: { display: "flex", justifyContent: "space-between", gap: "10px", marginBottom: "6px", opacity: 0.8 },
    realtimeAuditWho: { fontWeight: 900 },
    realtimeAuditText: { whiteSpace: "pre-wrap", lineHeight: 1.45 },


    composerContainer: { position: "sticky", bottom: composerViewportOffset, zIndex: 8, padding: "14px 18px calc(14px + env(safe-area-inset-bottom, 0px))", borderTop: "1px solid rgba(255,255,255,0.08)", background: "rgba(7,9,16,0.96)", backdropFilter: "blur(10px)" },
    composer: {
      display: "flex",
      alignItems: "flex-end",
      gap: "10px",
      padding: "10px",
      borderRadius: "18px",
      border: "1px solid rgba(255,255,255,0.12)",
      background: "rgba(255,255,255,0.04)",
    },
    attachBtn: {
      width: "42px",
      height: "42px",
      borderRadius: "14px",
      border: "1px solid rgba(255,255,255,0.1)",
      background: "rgba(255,255,255,0.05)",
      color: "#fff",
      cursor: "pointer",
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
      opacity: uploadProgress ? 0.6 : 1,
    },
    textarea: {
      flex: 1,
      minHeight: "42px",
      maxHeight: "180px",
      resize: "none",
      background: "transparent",
      border: "none",
      outline: "none",
      color: "#fff",
      fontSize: "14px",
      lineHeight: 1.4,
      padding: "10px 8px",
    },
    micBtn: {
      width: "42px",
      height: "42px",
      borderRadius: "14px",
      border: "1px solid rgba(255,255,255,0.1)",
      background: micEnabled ? "rgba(53,208,255,0.15)" : "rgba(255,255,255,0.05)",
      color: "#fff",
      cursor: speechSupported ? "pointer" : "not-allowed",
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
      opacity: speechSupported ? 1 : 0.6,
    },
    sendBtn: {
      width: "42px",
      height: "42px",
      borderRadius: "14px",
      border: "1px solid rgba(255,255,255,0.1)",
      background: "rgba(255,255,255,0.05)",
      color: "#fff",
      cursor: "pointer",
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
      opacity: sending ? 0.6 : 1,
    },
    select: {
      padding: "8px 10px",
      borderRadius: "12px",
      border: "1px solid rgba(255,255,255,0.12)",
      background: "rgba(255,255,255,0.05)",
      color: "#fff",
      fontSize: "12px",
    },
    modalBack: {
      position: "fixed",
      inset: 0,
      background: "rgba(0,0,0,0.55)",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      zIndex: 50,
      padding: "16px",
    },
    modal: {
      width: "min(720px, 96vw)",
      borderRadius: "18px",
      border: "1px solid rgba(255,255,255,0.12)",
      background: "rgba(12,12,20,0.96)",
      padding: "16px",
    },
    modalTitle: { fontSize: "14px", fontWeight: 900 },
    radioRow: { display: "flex", gap: "10px", alignItems: "center", marginTop: "10px", color: "rgba(255,255,255,0.85)" },
    modalActions: { display: "flex", justifyContent: "flex-end", gap: "10px", marginTop: "14px" },
    btn: { border: "1px solid rgba(255,255,255,0.12)", background: "rgba(255,255,255,0.06)", color: "#fff", padding: "10px 12px", borderRadius: "14px", cursor: "pointer" },
    btnPrimary: { background: "rgba(124,92,255,0.22)", border: "1px solid rgba(124,92,255,0.35)", fontWeight: 800 },
    checkGrid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: "8px", marginTop: "10px" },
    checkItem: { display: "flex", gap: "8px", alignItems: "center", padding: "8px", borderRadius: "12px", border: "1px solid rgba(255,255,255,0.08)", background: "rgba(255,255,255,0.03)" },
    hint: { fontSize: "12px", color: "rgba(255,255,255,0.6)", marginTop: "6px" },
  };

  const meName = user?.name || user?.email || "Você";

  if (!onboardingChecked) {
    return <div style={{ minHeight: "100vh", display: "grid", placeItems: "center", background: "#0f1115", color: "#fff", fontFamily: "system-ui" }}>Carregando sua experiência...</div>;
  }

  return (
    <>
    <PWAInstallPrompt />
    {showTermsModal && (
      <TermsModal onAccepted={async () => {
        setShowTermsModal(false);
        const resolvedTermsVersion = await fetchCurrentTermsVersion();
        const acceptedAt = Math.floor(Date.now() / 1000);
        // Update local user object + React state to avoid stale gate after acceptance
        const u = getUser();
        if (u) {
          const nextUser = { ...u, terms_accepted_at: acceptedAt, terms_version: resolvedTermsVersion };
          localStorage.setItem("orkio_user", JSON.stringify(nextUser));
          setUser(nextUser);
        } else {
          setUser((prev) => (prev ? { ...prev, terms_accepted_at: acceptedAt, terms_version: resolvedTermsVersion } : prev));
        }
      }} />
    )}

{onboardingOpen && (
      <OnboardingModal
        user={user}
        onComplete={(nextUser) => {
          const refreshedToken = nextUser?.access_token || token;
          const mergedUser = {
            ...(user || {}),
            ...(nextUser || {}),
            org_slug: nextUser?.org_slug || user?.org_slug || tenant,
            role: nextUser?.role || user?.role || "user",
            approved_at: nextUser?.approved_at ?? user?.approved_at ?? null,
            usage_tier: nextUser?.usage_tier ?? user?.usage_tier ?? null,
            signup_source: nextUser?.signup_source ?? user?.signup_source ?? null,
            signup_code_label: nextUser?.signup_code_label ?? user?.signup_code_label ?? null,
            product_scope: nextUser?.product_scope ?? user?.product_scope ?? null,
            onboarding_completed: true,
          };
          mergedUser.is_admin = hasAdminAccess(mergedUser);
          mergedUser.admin = mergedUser.is_admin === true;
          setUser(mergedUser);
          try {
            setSession({
              token: refreshedToken,
              user: mergedUser,
              tenant: mergedUser?.org_slug || tenant,
            });
            setToken(refreshedToken);
          } catch {}
          setOnboardingOpen(false);
          setOnboardingStatus("");
          setUploadStatus("✅ Onboarding concluído.");
          setTimeout(() => setUploadStatus(""), 1800);
        }}
      />
    )}
    <div style={styles.layout}>
      {/* Sidebar */}
      <div style={{ ...styles.sidebar, display: isMobile ? "none" : "flex" }}>
        <div style={styles.topRow}>
          <div>
            <div style={styles.brand}>Orkio</div>
            <div style={{ marginTop: 6, display: "flex", gap: 8, flexWrap: "wrap" }}>
              <span style={styles.badge}>org: {tenant}</span>
              <span style={styles.badge}>{health === "ok" ? "ready" : health}</span>
            </div>
          </div>

          <button style={styles.newThreadBtn} onClick={createThread} title="Nova conversa">
            <IconPlus /> Novo
          </button>
        </div>

        <div style={styles.threads}>
          {threads.length === 0 ? (
            <div style={styles.emptyThreads}>Nenhuma conversa ainda.</div>
          ) : (
            threads.map((t) => (
              <button
                key={t.id}
                onClick={() => setThreadId(t.id)}
                style={{
                  ...styles.threadItem,
                  ...(t.id === threadId ? styles.threadItemActive : {}),
                }}
              >
                <IconMessage />
                <span style={styles.threadTitle}>{t.title}</span>
                <button
                  style={styles.threadEditBtn}
                  onClick={(e) => { e.stopPropagation(); renameThread(t.id); }}
                  title="Renomear conversa"
                >
                  <IconEdit />
                </button>
                <button
                  style={styles.threadEditBtn}
                  onClick={(e) => { e.stopPropagation(); deleteThread(t.id); }}
                  title="Deletar conversa"
                >
                  <IconTrash />
                </button>
              </button>
            ))
          )}
        </div>

        <div style={styles.userSection}>
          <div style={styles.userInfo}>
            <div style={styles.userAvatar}>{meName.charAt(0).toUpperCase()}</div>
            <div style={styles.userDetails}>
              <div style={styles.userName}>{user?.name || "Usuário"}</div>
              <div style={styles.userEmail}>{user?.email || ""}</div>
            </div>
          </div>

          <div style={styles.userActions}>
            {WALLET_UI_ENABLED ? (
              <button style={styles.iconBtn} onClick={() => nav("/wallet")} title={walletLowBalance ? "Recarregar wallet" : "Wallet & usage"}>
                💳
              </button>
            ) : null}
            {canAccessAdmin && (
              <button style={styles.iconBtn} onClick={() => nav("/admin")} title="Admin Console">
                <IconSettings />
              </button>
            )}
            <button style={styles.iconBtn} onClick={doLogout} title="Sair">
              <IconLogout />
            </button>
          </div>
        </div>
      </div>

      {/* Main */}
      <div style={styles.main}>
        <div style={{ ...styles.topbar, padding: isMobile ? "12px 14px" : styles.topbar.padding }}>
          <div>
            <div style={styles.title}>{threads.find((t) => t.id === threadId)?.title || "Conversa"}</div>
            <div style={styles.health}>Destino: {destMode === "team" ? "Team" : destMode === "single" ? "Agente" : "Multi"} • @Team / @Orkio / @Chris / @Orion</div>
          </div>

          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: isMobile ? "wrap" : "nowrap", justifyContent: "flex-end" }}>
            {isMobile ? (
              <button
                type="button"
                onClick={doLogout}
                style={{
                  border: "1px solid rgba(255,255,255,0.14)",
                  background: "rgba(255,255,255,0.08)",
                  color: "#fff",
                  minHeight: 40,
                  padding: "8px 12px",
                  borderRadius: 12,
                  fontWeight: 700,
                  cursor: "pointer",
                }}
                title="Sair"
              >
                Sair
              </button>
            ) : null}
            <select style={styles.select} value={destMode} onChange={(e) => setDestMode(e.target.value)}>
              <option value="team">Team</option>
              <option value="single">1 agente</option>
              <option value="multi">multi</option>
            </select>

            {destMode === "single" ? (
              <select style={styles.select} value={destSingle} onChange={(e) => setDestSingle(e.target.value)}>
                {agents.map(a => <option key={a.id} value={a.id}>{a.name}{a.is_default ? " (default)" : ""}</option>)}
              </select>
            ) : null}

            {destMode === "multi" && !isMobile ? (
              <select style={styles.select} value="choose" onChange={() => {}}>
                <option value="choose">Selecionar no envio...</option>
              </select>
            ) : null}
          </div>
        </div>

        {(activeRuntimeAgent || runtimeHandoffLabel || agentCapabilities) ? (
          <div
            style={{
              margin: isMobile ? "10px 12px 0" : "12px 16px 0",
              padding: "10px 12px",
              borderRadius: 12,
              border: "1px solid rgba(255,255,255,0.08)",
              background: "rgba(255,255,255,0.035)",
              display: "flex",
              alignItems: "center",
              gap: 10,
              flexWrap: "wrap",
              color: "rgba(255,255,255,0.82)",
              fontSize: 12,
            }}
          >
            {activeRuntimeAgent ? (
              <span style={{ fontWeight: 800 }}>
                {formatActiveAgentRuntime(activeRuntimeAgent)}
              </span>
            ) : null}
            {runtimeHandoffLabel ? (
              <span style={{ color: "rgba(255,255,255,0.68)" }}>{runtimeHandoffLabel}</span>
            ) : null}
            {agentCapabilities ? (
              <span style={{ marginLeft: "auto", color: "rgba(255,255,255,0.68)" }}>
                GitHub: {formatGithubRuntimeStatus(agentCapabilities)}
              </span>
            ) : null}
          </div>
        ) : null}

        {WALLET_UI_ENABLED ? (

        <div
          style={{
            margin: isMobile ? "10px 12px 0" : "12px 16px 0",
            borderRadius: 18,
            border: walletLowBalance ? "1px solid rgba(251,191,36,0.28)" : "1px solid rgba(255,255,255,0.08)",
            background: walletLowBalance ? "linear-gradient(135deg, rgba(120,53,15,0.28), rgba(30,41,59,0.72))" : "rgba(255,255,255,0.04)",
            padding: isMobile ? "12px" : "14px 16px",
            display: "flex",
            flexWrap: "wrap",
            gap: 12,
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div style={{ display: "grid", gap: 4 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
              <span style={{ fontSize: 11, fontWeight: 900, letterSpacing: "0.12em", textTransform: "uppercase", color: walletLowBalance ? "#fbbf24" : "rgba(103,232,249,0.88)" }}>
                Wallet
              </span>
              {walletActivePlanName ? (
                <span style={{ fontSize: 12, color: "rgba(255,255,255,0.72)" }}>
                  Plano ativo: {walletActivePlanName}
                </span>
              ) : (
                <span style={{ fontSize: 12, color: "rgba(255,255,255,0.58)" }}>
                  Acesso orientado por wallet
                </span>
              )}
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "baseline" }}>
              <div style={{ fontSize: isMobile ? 22 : 24, fontWeight: 900 }}>{fmtUsd(walletBalanceUsd)}</div>
              <div style={{ fontSize: 13, color: walletLowBalance ? "#fde68a" : "rgba(255,255,255,0.64)" }}>
                {walletLowBalance
                  ? `Saldo baixo. Recomendado manter acima de ${fmtUsd(walletLowBalanceThresholdUsd)}.`
                  : walletAutoRechargeEnabled
                  ? "Auto-recharge ativo."
                  : "Saldo pronto para uso."}
              </div>
            </div>
            {walletSummaryError ? (
              <div style={{ fontSize: 12, color: "#fda4af" }}>{walletSummaryError}</div>
            ) : walletSummaryUpdatedAt ? (
              <div style={{ fontSize: 11, color: "rgba(255,255,255,0.44)" }}>
                Atualizado em {formatDateTime(walletSummaryUpdatedAt)}
              </div>
            ) : null}
          </div>

          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center", justifyContent: "flex-end" }}>
            <button
              type="button"
              onClick={() => { void refreshWalletSummary({ silent: false }); }}
              disabled={walletSummaryLoading}
              style={{
                borderRadius: 12,
                padding: "10px 12px",
                border: "1px solid rgba(255,255,255,0.12)",
                background: "rgba(255,255,255,0.06)",
                color: "#fff",
                fontWeight: 700,
                cursor: walletSummaryLoading ? "default" : "pointer",
                opacity: walletSummaryLoading ? 0.7 : 1,
              }}
            >
              {walletSummaryLoading ? "Atualizando..." : "Atualizar"}
            </button>
            <button
              type="button"
              onClick={() => nav("/wallet")}
              style={{
                borderRadius: 12,
                padding: "10px 12px",
                border: walletLowBalance ? 0 : "1px solid rgba(255,255,255,0.12)",
                background: walletLowBalance ? "linear-gradient(135deg, #f59e0b, #fb7185)" : "rgba(255,255,255,0.08)",
                color: walletLowBalance ? "#111827" : "#fff",
                fontWeight: 900,
                cursor: "pointer",
              }}
            >
              {walletLowBalance ? "Recarregar wallet" : "Abrir wallet"}
            </button>
          </div>
        </div>

        ) : null}

        {/* Messages */}
        <div style={{ ...styles.chatArea, padding: isMobile ? "12px 12px 18px" : styles.chatArea.padding }}>
          {messages.length === 0 ? (
            <div style={{ color: "rgba(255,255,255,0.55)", fontSize: "14px", padding: "8px" }}>
              Nenhuma mensagem ainda. Você pode chamar múltiplos agentes com <b>@Team</b> ou usar o seletor acima.
            </div>
          ) : (
            messages.map((m) => (
              <div
                key={m.id}
                style={{
                  ...styles.messageRow,
                  justifyContent: m.role === "user" ? "flex-end" : (m.role === "system" ? "center" : "flex-start"),
                }}
              >
                {/* PATCH0100_14: Agent avatar */}
                {m.role === "assistant" && lastAgentInfo?.avatar_url && (
                  <div style={{ marginRight: 8, flexShrink: 0, alignSelf: "flex-start", marginTop: 4 }}>
                    <img
                      src={lastAgentInfo.avatar_url}
                      alt={m.agent_name || "Agent"}
                      style={{ width: 36, height: 36, borderRadius: "50%", objectFit: "cover", border: "2px solid rgba(255,255,255,0.15)" }}
                      onError={(e) => { e.target.style.display = 'none'; }}
                    />
                  </div>
                )}
                <div
                  style={{
                    ...styles.messageBubble,
                    ...(m.role === "user"
                      ? styles.userBubble
                      : m.role === "system"
                      ? styles.systemBubble
                      : styles.agentBubble),
                  }}
                >
                  {(() => {
                    const evt = tryParseEvent(m.content);
                    const isUser = m.role === "user";
                    const isSystem = m.role === "system";
                    const name = isUser
                      ? (m.user_name || meName)
                      : (m.agent_name || (isSystem ? "Sistema" : "Agente"));
                    const nameTone = isUser ? styles.nameUser : isSystem ? styles.nameSystem : styles.nameAgent;
                    const created = formatDateTime(m.created_at);
                    const visible = stripEventMarker(m.content);

                    return (
                      <>
                        <div style={styles.bubbleHeaderRow}>
                          <div style={{ ...styles.bubbleHeaderName, ...nameTone }}>{name}</div>
                          <div style={styles.bubbleHeaderTime}>{created}</div>
                        </div>

                        {evt && evt.type === "file_upload" ? (
                          <div style={styles.messageContent}>
                            <div style={{ fontWeight: 900 }}>📎 Upload registrado</div>
                            <div style={{ marginTop: 6 }}>{evt.filename || "arquivo"}</div>
                            <div style={{ marginTop: 6, fontSize: 12, opacity: 0.78 }}>
                              {evt.text || `por ${evt.uploader_name || evt.uploader_email || "Usuário"} • ${formatTs(evt.ts || evt.created_at)}`}
                            </div>
                          </div>
                        ) : (
                          <div style={styles.messageContent}>
                            {visible || m.content}
                            {!isUser && !isSystem && (visible || m.content) && (
                              <button
                                onClick={() => playTts((visible || m.content), (m.agent_id || null), { messageId: m.id || null })}
                                style={{ marginLeft: "8px", background: "none", border: "none", cursor: "pointer", opacity: 0.6, fontSize: "14px", padding: "2px" }}
                                title="Ouvir esta mensagem"
                              >
                                🔊
                              </button>
                            )}
                          </div>
                        )}
                      </>
                    );
                  })()}
                </div>
              </div>
            ))
          )}
          <div ref={messagesEndRef} />
        </div>

        {/* V2V-PATCH: status panel por fase */}
        {v2vPhase && (
          <div style={{
            padding: "6px 14px", margin: "4px 0",
            borderRadius: "6px", fontSize: "12px", fontWeight: 500,
            background: v2vPhase === 'error' ? "rgba(192,57,43,0.15)" : "rgba(10,126,140,0.12)",
            color: v2vPhase === 'error' ? "#e74c3c" : "#0A7E8C",
            border: `1px solid ${v2vPhase === 'error' ? "rgba(192,57,43,0.3)" : "rgba(10,126,140,0.25)"}`,
            display: "flex", alignItems: "center", gap: "8px",
          }}>
            <span>{
              v2vPhase === 'recording' ? "🔴 Gravando..." :
              v2vPhase === 'stt'       ? "⚙️ Transcrevendo fala..." :
              v2vPhase === 'chat'      ? "🤖 Gerando resposta..." :
              v2vPhase === 'tts'       ? "🔊 Sintetizando voz..." :
              v2vPhase === 'playing'   ? "🔈 Reproduzindo..." :
              v2vPhase === 'error'     ? `❌ ${v2vError || "Erro no V2V"}` :
              "⏳ Aguardando..."
            }</span>
            {v2vPhase === 'error' && walletBlockedDetail?.code === "WALLET_INSUFFICIENT_BALANCE" && (
              <button
                type="button"
                onClick={() => nav("/wallet")}
                style={{
                  marginLeft: "auto",
                  border: 0,
                  borderRadius: "999px",
                  padding: "7px 12px",
                  cursor: "pointer",
                  fontWeight: 800,
                  background: "linear-gradient(135deg, #f59e0b, #fb7185)",
                  color: "#111827",
                }}
              >
                Recarregar wallet
              </button>
            )}
            {v2vPhase === 'error' && (
              <button type="button" onClick={() => { setV2vPhase(null); setV2vError(null); setWalletBlockedDetail(null); }}
                style={{ marginLeft: walletBlockedDetail?.code === "WALLET_INSUFFICIENT_BALANCE" ? 0 : "auto", background: "none", border: "none", cursor: "pointer", color: "#e74c3c", fontSize: "14px" }}>
                ✕
              </button>
            )}
          </div>
        )}
        {uploadStatus ? <div style={styles.uploadStatus}>{uploadStatus}</div> : null}

        {/* Composer */}
        <div style={{ ...styles.composerContainer, padding: isMobile ? "10px 12px calc(10px + env(safe-area-inset-bottom, 0px))" : styles.composerContainer.padding }}>
          {showOrionSquad && (orionSquadHealth || orionSquadPreview) ? (
            <div
              style={{
                marginBottom: "8px",
                padding: "8px 10px",
                borderRadius: "10px",
                border: "1px solid rgba(89,165,255,0.25)",
                background: "rgba(89,165,255,0.08)",
                fontSize: "12px",
                lineHeight: 1.35,
                color: "rgba(255,255,255,0.86)",
              }}
            >
              <div><strong>Orion Squad:</strong> {orionSquadHealth?.ok === false ? "offline" : "online"}</div>
              {orionSquadHealth?.agents_loaded ? <div><strong>Agents loaded:</strong> {orionSquadHealth.agents_loaded}</div> : null}
              {orionSquadPreview?.primary_specialist ? <div><strong>Primary specialist:</strong> {orionSquadPreview.primary_specialist}</div> : null}
              {orionSquadPreview?.secondary_specialist ? <div><strong>Secondary specialist:</strong> {orionSquadPreview.secondary_specialist}</div> : null}
            </div>
          ) : null}


{executionTrace.length ? (
  <div
    style={{
      marginBottom: "8px",
      borderRadius: "14px",
      border: "1px solid rgba(255,255,255,0.08)",
      background: "rgba(255,255,255,0.035)",
      overflow: "hidden",
    }}
  >
    <button
      type="button"
      onClick={() => setExecutionTraceExpanded((prev) => !prev)}
      style={{
        width: "100%",
        border: 0,
        background: "transparent",
        color: "#fff",
        padding: "12px 14px",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 12,
        cursor: "pointer",
        textAlign: "left",
      }}
    >
      <div>
        <div style={{ fontSize: 13, fontWeight: 900, letterSpacing: "0.01em" }}>Execution trace</div>
        <div style={{ fontSize: 12, color: "rgba(255,255,255,0.62)", marginTop: 2 }}>
          {sending ? "Orkio está executando etapas desta solicitação." : "Última execução registrada no console."}
        </div>
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        {lastTraceId ? (
          <span style={{ fontSize: 11, color: "rgba(255,255,255,0.44)", fontFamily: "monospace" }}>
            {lastTraceId}
          </span>
        ) : null}
        <span style={{ fontSize: 16, opacity: 0.8 }}>{executionTraceExpanded ? "▾" : "▸"}</span>
      </div>
    </button>

    {executionTraceExpanded ? (
      <div style={{ padding: "0 14px 14px", display: "grid", gap: 8 }}>
        {executionTrace.map((step) => {
          const tone = traceStepTone(step.kind);
          return (
            <div
              key={step.id}
              style={{
                borderRadius: 12,
                border: `1px solid ${tone.border}`,
                background: tone.background,
                padding: "10px 12px",
                display: "grid",
                gap: 4,
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 8, minWidth: 0 }}>
                <span style={{ fontSize: 14 }}>{tone.icon}</span>
                <span style={{ fontSize: 13, fontWeight: 800, color: tone.color, minWidth: 0 }}>
                  {step.label}
                </span>
                {step.agentName ? (
                  <span
                    style={{
                      marginLeft: "auto",
                      fontSize: 11,
                      padding: "3px 8px",
                      borderRadius: 999,
                      background: "rgba(255,255,255,0.08)",
                      color: "rgba(255,255,255,0.7)",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {step.agentName}
                  </span>
                ) : null}
              </div>
              {(step.detail || step.ts) ? (
                <div style={{ fontSize: 12, color: "rgba(255,255,255,0.72)", lineHeight: 1.4 }}>
                  {step.detail || ""}
                  {step.ts ? (
                    <span style={{ color: "rgba(255,255,255,0.38)", marginLeft: step.detail ? 8 : 0 }}>
                      {new Date(step.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
                    </span>
                  ) : null}
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    ) : null}
  </div>
) : null}
          {showRuntimeHints && runtimeHints ? (
            <div
              style={{
                marginBottom: "8px",
                padding: "8px 10px",
                borderRadius: "10px",
                border: "1px solid rgba(255,255,255,0.08)",
                background: "rgba(255,255,255,0.03)",
                fontSize: "12px",
                lineHeight: 1.35,
                color: "rgba(255,255,255,0.78)",
              }}
            >
              {runtimeHints?.planner?.primary_objective ? <div><strong>Focus:</strong> {runtimeHints.planner.primary_objective}</div> : null}
              {runtimeHints?.resume_hint ? <div><strong>Continuity:</strong> {runtimeHints.resume_hint}</div> : null}
              {runtimeHints?.trial?.recommended_action ? <div><strong>Next:</strong> {runtimeHints.trial.recommended_action}</div> : null}
              {runtimeHints?.routing?.mode ? <div><strong>Route:</strong> {runtimeHints.routing.mode}</div> : null}
              {runtimeHints?.trial?.stage ? <div><strong>Stage:</strong> {runtimeHints.trial.stage}</div> : null}
              {(runtimeHints?.planner?.confidence ?? null) !== null ? <div><strong>Confidence:</strong> {runtimeHints.planner.confidence}</div> : null}
              {(runtimeHints?.trial?.activation_probability ?? null) !== null ? <div><strong>Activation probability:</strong> {runtimeHints.trial.activation_probability}</div> : null}
              {(runtimeHints?.memory?.strong_resume_ready ?? null) !== null ? <div><strong>Resume readiness:</strong> {runtimeHints.memory.strong_resume_ready ? "ready" : "warming"}</div> : null}
              {runtimeHints?.routing?.execution_cursor?.current_node ? <div><strong>Current node:</strong> {runtimeHints.routing.execution_cursor.current_node}</div> : null}
              {(runtimeHints?.routing?.routing_confidence ?? null) !== null ? <div><strong>Routing confidence:</strong> {runtimeHints.routing.routing_confidence}</div> : null}
              {runtimeHints?.capabilities?.multiagent?.available_agents?.length ? <div><strong>Agents:</strong> {runtimeHints.capabilities.multiagent.available_agents.join(", ")}</div> : null}
              {runtimeHints?.capabilities?.github ? <div><strong>GitHub:</strong> {formatGithubRuntimeStatus(runtimeHints.capabilities)}</div> : null}
              {lastTraceId ? <div><strong>Trace:</strong> {lastTraceId}</div> : null}
            </div>
          ) : null}
          <div style={{ ...styles.composer, gap: isMobile ? "8px" : styles.composer.gap }}>
            <input
              type="file"
              ref={fileInputRef}
              onChange={onPickFile}
              accept=".pdf,.docx,.doc,.txt,.md"
              style={{ display: "none" }}
            />

            {!isMobile ? (
              <button
                type="button"
                style={styles.attachBtn}
                onClick={() => fileInputRef.current?.click()}
                disabled={uploadProgress}
                title="Attach file (PDF, DOCX, TXT)"
              >
                <IconPaperclip />
              </button>
            ) : null}

            <textarea
              value={text}
              onChange={(e) => setText(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Type your message..."
              style={styles.textarea}
              rows={1}
              disabled={sending}
            />

            {SUMMIT_VOICE_MODE === "stt_tts" ? (
              <button
                type="button"
                style={{ ...styles.micBtn, opacity: speechSupported ? 1 : 0.6 }}
                onClick={toggleMic}
                title={micEnabled ? "Stop voice input" : "Start voice input"}
              >
                🎙️
              </button>
            ) : (
              <button
                type="button"
                style={{
                  ...styles.micBtn,
                  background: realtimeMode ? "rgba(80,160,255,0.25)" : "rgba(255,255,255,0.05)",
                  border: realtimeMode ? "1px solid rgba(80,160,255,0.5)" : "1px solid rgba(255,255,255,0.1)",
                  position: "relative",
                  opacity: 1,
                  cursor: "pointer",
                }}
                onClick={toggleRealtimeMode}
                title={realtimeMode ? "Disable realtime voice" : "Enable realtime voice"}
              >
                <span style={{ fontSize: "16px" }}>⚡</span>
                {realtimeMode && <span style={{ position: "absolute", top: "-2px", right: "-2px", width: "8px", height: "8px", borderRadius: "50%", background: "#50a0ff", animation: "pulse 1.5s infinite" }} />}
              </button>
            )}

            {!isMobile && realtimeMode && SUMMIT_VOICE_MODE === "realtime" ? (
              <button
                type="button"
                style={{
                  ...styles.sendBtn,
                  opacity: rtcReadyToRespond ? 1 : 0.5,
                  cursor: rtcReadyToRespond ? "pointer" : "not-allowed",
                }}
                onClick={() => rtcReadyToRespond && triggerRealtimeResponse("manual")}
                disabled={!rtcReadyToRespond}
                title={rtcReadyToRespond ? "Respond now (realtime)" : "Waiting for speech to finish"}
              >
                ▶️
              </button>
            ) : null}

            <button
              type="button"
              style={{ ...styles.micBtn, opacity: handoffBusy ? 0.7 : 1 }}
              onClick={handleFounderHandoff}
              disabled={handoffBusy}
              title="Talk to founder"
            >
              🤝
            </button>

            <button type="button" style={styles.sendBtn} onMouseDown={(e) => e.preventDefault()} onClick={() => sendMessage()} disabled={sending} title="Enviar">
              <IconSend />
            </button>
          </div>
          {handoffNotice ? (
            <div style={{ marginTop: 8, fontSize: 12, color: "rgba(255,255,255,0.78)" }}>{handoffNotice}</div>
          ) : null}
          <div style={{ marginTop: 8, display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
            <div style={{ fontSize: 12, color: "rgba(255,255,255,0.62)" }}>
              AI-generated responses may contain inaccuracies. Always verify important information before relying on them.
            </div>
            <div style={{ display: isMobile ? "none" : "flex", gap: 8 }}>
              <button
                onClick={downloadRealtimeAta}
                style={{ ...styles.btn, padding: "6px 10px", fontSize: "12px", opacity: rtcSessionIdRef.current ? 1 : 0.6 }}
                title="Baixar relatório executivo da sessão"
                disabled={!rtcSessionIdRef.current}
              >
                ⬇️ Relatório
              </button>
            </div>
          </div>

          {/* Voice Mode controls — PATCH0100_14 enhanced */}
          {voiceMode && SUMMIT_VOICE_MODE === "stt_tts" && !isMobile && (
            <div style={{ display: "flex", alignItems: "center", gap: "8px", padding: "4px 8px", fontSize: "12px", color: "rgba(255,255,255,0.7)", flexWrap: "wrap" }}>
              {lastAgentInfo?.avatar_url && (
                <img src={lastAgentInfo.avatar_url} alt="" style={{ width: 24, height: 24, borderRadius: "50%", objectFit: "cover" }} onError={(e) => { e.target.style.display = 'none'; }} />
              )}
              {lastAgentInfo?.agent_name && <span style={{ fontWeight: 600, color: "rgba(255,255,255,0.85)" }}>{lastAgentInfo.agent_name}</span>}
              <span>🔊 Voz:</span>
              <select
                value={ttsVoice}
                onChange={(e) => changeTtsVoice(e.target.value)}
                style={{ ...styles.select, padding: "4px 8px", fontSize: "11px" }}
              >
                <option value="auto">Auto (voz do agente)</option>
                {ORKIO_VOICES.map(v => (
                  <option key={v.id} value={v.id}>{v.label}</option>
                ))}
</select>
              {ttsPlaying && (
                <button
                  onClick={stopTts}
                  style={{ ...styles.btn, padding: "4px 8px", fontSize: "11px" }}
                >
                  ⏹ Parar
                </button>
              )}
              <span style={{ opacity: 0.6 }}>
                {micEnabled ? "🔴 Ouvindo..." : ttsPlaying ? "🔊 Falando..." : "⏸ Aguardando"}
              </span>
              {!!(rtcSessionIdRef.current || rtcAuditEvents?.length) && (
                <button
                  onClick={downloadRealtimeAta}
                  style={{ ...styles.btn, padding: "4px 8px", fontSize: "11px" }}
                  title="Baixar relatório executivo da sessão"
                >
                  ⬇️ Relatório
                </button>
              )}
            </div>
          )}

          {/* PATCH0100_27_2B: Realtime Audit (finals + punctuação assíncrona) */}
          {SHOW_REALTIME_AUDIT && !isMobile && (rtcAuditEvents?.length > 0 || rtcPunctStatus) && (
            <div style={styles.realtimeAudit}>
              <div style={styles.realtimeAuditHeader}>
                <div style={styles.realtimeAuditTitle}>🧾 Realtime (auditável)</div>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <div style={styles.realtimeAuditPill}>
                    {rtcPunctStatus === 'pending' ? 'Pontuando…' : rtcPunctStatus === 'done' ? 'Pontuação OK' : rtcPunctStatus === 'timeout' ? 'Pontuação pendente' : 'Registro local'}
                  </div>
                  <button
                    onClick={downloadRealtimeAta}
                    style={{ ...styles.btn, padding: "4px 8px", fontSize: "11px" }}
                    title="Baixar ata da sessão"
                  >
                    ⬇️ Baixar ata
                  </button>
                </div>
              </div>
              {rtcAuditEvents.map((ev, idx) => {
                const who = ev?.role === 'user' ? 'Você' : (ev?.agent_name || 'Assistente');
                const when = ev?.created_at ? new Date(ev.created_at).toLocaleTimeString() : '';
                const text = (ev?.transcript_punct || ev?.content || '').toString();
                return (
                  <div key={(ev?.id || idx) + ''} style={styles.realtimeAuditItem}>
                    <div style={styles.realtimeAuditMeta}>
                      <div style={styles.realtimeAuditWho}>{who}</div>
                      <div style={{ opacity: 0.7 }}>{when}</div>
                    </div>
                    <div style={styles.realtimeAuditText}>{text}</div>
                  </div>
                );
              })}
              {summitSessionScore && (
                <div style={{ marginTop: 10, paddingTop: 10, borderTop: "1px solid rgba(255,255,255,0.08)" }}>
                  <div style={{ fontWeight: 800, marginBottom: 6 }}>🎯 Summit score</div>
                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap", fontSize: 12, opacity: 0.9 }}>
                    <span>Naturalidade: {summitSessionScore?.naturalness_score ?? "-"}</span>
                    <span>Persona: {summitSessionScore?.persona_score ?? "-"}</span>
                    <span>Duplicação: {summitSessionScore?.duplicate_count ?? 0}</span>
                    <span>Truncamento: {summitSessionScore?.truncation_count ?? 0}</span>
                  </div>
                  {!summitSessionScore?.human_review && summitRuntimeModeRef.current === "summit" && (
                    <div style={{ display: "flex", gap: 8, marginTop: 10, flexWrap: "wrap" }}>
                      <button disabled={summitReviewPending} onClick={() => submitStageReview(5, 5, 5)} style={{ ...styles.btn, padding: "4px 8px", fontSize: "11px" }}>✅ Forte</button>
                      <button disabled={summitReviewPending} onClick={() => submitStageReview(4, 4, 4)} style={{ ...styles.btn, padding: "4px 8px", fontSize: "11px" }}>🟨 Bom</button>
                      <button disabled={summitReviewPending} onClick={() => submitStageReview(2, 2, 2)} style={{ ...styles.btn, padding: "4px 8px", fontSize: "11px" }}>🛠 Ajustar</button>
                    </div>
                  )}
                </div>
              )}
              {rtcAuditEvents.length === 0 && <div style={{ opacity: 0.8 }}>Sem eventos finais ainda.</div>}
            </div>
          )}


          {destMode === "multi" ? (
            <div style={{...styles.hint, display: isMobile ? "none" : styles.hint.display}}>
              Multi: selecione os agentes abaixo (será usado no próximo envio).
              <div style={styles.checkGrid}>
                {agents.map(a => (
                  <label key={a.id} style={styles.checkItem}>
                    <input
                      type="checkbox"
                      checked={destMulti.includes(a.id)}
                      onChange={(e) => {
                        setDestMulti(prev => e.target.checked ? [...prev, a.id] : prev.filter(x => x !== a.id));
                      }}
                    />
                    <span>{a.name}</span>
                  </label>
                ))}
              </div>
            </div>
          ) : null}
        </div>
      </div>


      {showHandoffModal ? (
        <div style={styles.modalBack} onClick={() => { if (!handoffBusy) setShowHandoffModal(false); }}>
          <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
            <div style={styles.modalTitle}>Talk to founder</div>
            <div style={styles.hint}>
              You are about to share this conversation with the Orkio founder for follow-up.
            </div>
            <div style={{ ...styles.hint, marginTop: 8 }}>
              Orkio will share a concise summary of your context so the next step can be strategic, not repetitive.
            </div>
            <div style={{ marginTop: 12, padding: 12, borderRadius: 12, border: "1px solid rgba(255,255,255,0.08)", background: "rgba(255,255,255,0.03)", fontSize: 13, lineHeight: 1.45 }}>
              {handoffDraft || "Your latest strategic context will be shared with the founder."}
            </div>
            <div style={{ ...styles.hint, marginTop: 10 }}>
              By continuing, you explicitly authorize Orkio to share this conversation summary with the founder for direct follow-up.
            </div>
            <div style={styles.modalActions}>
              <button style={styles.btn} onClick={() => setShowHandoffModal(false)} disabled={handoffBusy}>Cancel</button>
              <button type="button" style={{ ...styles.btn, ...styles.btnPrimary, opacity: handoffBusy ? 0.7 : 1 }} onClick={confirmFounderHandoff} disabled={handoffBusy}>
                {handoffBusy ? "Sending..." : "Confirm and share"}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {/* Upload Modal */}
      {uploadOpen ? (
        <div style={styles.modalBack} onClick={() => { if (!uploadProgress) setUploadOpen(false); }}>
          <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
            <div style={styles.modalTitle}>Upload: {uploadFileObj?.name || "arquivo"}</div>
            <div style={styles.hint}>Escolha como este documento será usado.</div>

            <div style={styles.radioRow}>
              <input type="radio" checked={uploadScope === "thread"} onChange={() => setUploadScope("thread")} />
              <span>Somente nesta conversa (contexto do thread)</span>
            </div>

            <div style={styles.radioRow}>
              <input type="radio" checked={uploadScope === "agents"} onChange={() => setUploadScope("agents")} />
              <span>Vincular a agente(s) específico(s)</span>
            </div>

            {uploadScope === "agents" ? (
              <div style={styles.checkGrid}>
                {agents.map(a => (
                  <label key={a.id} style={styles.checkItem}>
                    <input
                      type="checkbox"
                      checked={uploadAgentIds.includes(a.id)}
                      onChange={(e) => {
                        setUploadAgentIds(prev => e.target.checked ? [...prev, a.id] : prev.filter(x => x !== a.id));
                      }}
                    />
                    <span>{a.name}</span>
                  </label>
                ))}
              </div>
            ) : null}

            <div style={styles.radioRow}>
              <input type="radio" checked={uploadScope === "institutional"} onChange={() => setUploadScope("institutional")} />
              <span>Institucional (global do tenant → todos os agentes)</span>
            </div>
            <div style={styles.hint}>
              {canAccessAdmin
                ? "Como admin, o documento vira institucional imediatamente."
                : "Como usuário, isso vira uma SOLICITAÇÃO para o admin aprovar/reprovar. Enquanto isso, ele fica disponível nesta conversa."}
            </div>

            <div style={styles.modalActions}>
              <button style={styles.btn} onClick={() => { if (!uploadProgress) setUploadOpen(false); }}>Cancelar</button>
              <button type="button" style={{ ...styles.btn, ...styles.btnPrimary, opacity: uploadProgress ? 0.7 : 1 }} onClick={confirmUpload} disabled={uploadProgress}>
                {uploadProgress ? "Enviando..." : "Enviar"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    
{capacityOpen ? (
  <div style={{
    position: "fixed", inset: 0, background: "rgba(0,0,0,0.6)",
    display: "flex", alignItems: "center", justifyContent: "center", zIndex: 9999
  }}>
    <div style={{
      background: "#0f0f10", color: "#fff", padding: 24, borderRadius: 12,
      maxWidth: 520, width: "92%", boxShadow: "0 10px 40px rgba(0,0,0,0.6)"
    }}>
      <div style={{ fontSize: 20, fontWeight: 700, marginBottom: 10 }}>
        Estamos operando no limite seguro da plataforma
      </div>
      <div style={{ opacity: 0.9, lineHeight: 1.4, marginBottom: 14 }}>
        Muitas pessoas estão acessando ao mesmo tempo. Para manter a estabilidade durante o evento,
        alguns acessos estão temporariamente limitados.
      </div>
      <div style={{ opacity: 0.9, marginBottom: 16 }}>
        Tentaremos novamente em <b>{capacitySeconds}s</b>.
      </div>
      <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
        <button style={{ padding: "10px 14px", borderRadius: 10 }} onClick={() => {
          const pending = capacityPendingRef.current;
          closeCapacityModal();
          if (pending?.msg) sendMessage(pending.msg, { isRetry: true });
        }}>
          Tentar agora
        </button>
        <button style={{ padding: "10px 14px", borderRadius: 10, opacity: 0.9 }} onClick={closeCapacityModal}>
          Voltar
        </button>
      </div>
    </div>
  </div>
) : null}

</div>
    </>
  );
}
