import {
  clearSession,
  getTenant as readTenant,
  getToken as readToken,
} from "../lib/auth.js";

function normalizeBaseUrl(v) {
  const s = String(v || "").trim();
  if (!s) return "/api";
  return s.replace(/\/+$/, "");
}

function normalizePath(path) {
  let p = String(path || "").trim();
  if (!p) return "/";
  if (!p.startsWith("/")) p = `/${p}`;

  if (p.startsWith("/api/")) return p;
  if (p === "/api") return p;
  return p;
}

const API_BASE = normalizeBaseUrl(
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_URL ||
  "/api"
);

export function joinApi(path = "") {
  const p = normalizePath(path);

  if (API_BASE.endsWith("/api") && p.startsWith("/api/")) {
    return `${API_BASE}${p.slice(4)}`;
  }

  if (API_BASE === "/api" && p.startsWith("/api/")) {
    return p;
  }

  return `${API_BASE}${p}`;
}

export function headers({ token, org, json = true, extra = {} } = {}) {
  const resolvedToken = token ?? readToken();
  const resolvedOrg = org ?? readTenant();

  const out = {
    ...extra,
  };

  if (json) out["Content-Type"] = "application/json";
  if (resolvedToken) out["Authorization"] = `Bearer ${resolvedToken}`;
  if (resolvedOrg) out["X-Org-Slug"] = resolvedOrg;

  return out;
}

function enrichResult(data, response) {
  if (data && typeof data === "object" && !Array.isArray(data)) {
    return { ...data, data, response };
  }
  return { data, response };
}

async function parseResponseBody(response) {
  const contentType = response.headers.get("content-type") || "";
  if (response.status === 204) return null;

  if (contentType.includes("application/json")) {
    try {
      return await response.json();
    } catch {
      return null;
    }
  }

  try {
    return await response.text();
  } catch {
    return null;
  }
}

export async function apiFetch(path, options = {}) {
  const url = joinApi(path);

  const isFormData =
    typeof FormData !== "undefined" && options.body instanceof FormData;

  const config = {
    method: options.method || "GET",
    headers: headers({
      token: options.token,
      org: options.org,
      json: !isFormData,
      extra: options.headers || {},
    }),
    credentials: options.credentials || "same-origin",
    signal: options.signal,
  };

  if (options.body !== undefined && options.body !== null) {
    if (isFormData) {
      config.body = options.body;
    } else if (typeof options.body === "string" || options.body instanceof Blob) {
      config.body = options.body;
    } else {
      config.body = JSON.stringify(options.body);
    }
  }

  console.log("API_FETCH_REQUEST", { url, method: config.method, hasBody: !!config.body, org: options.org, credentials: config.credentials });
  const response = await fetch(url, config);
  const payload = await parseResponseBody(response);
  console.log("API_FETCH_RESPONSE", { url, status: response.status, ok: response.ok, payload });

  if (response.status === 401) {
    if (!options.skipAuthRedirect) {
      clearSession();
      if (typeof window !== "undefined" && window.location?.pathname !== "/auth") {
        window.location.href = "/auth?session_expired=1";
      }
    }
    const detail =
      (payload && typeof payload === "object" && (payload.detail || payload.message)) ||
      "Session expired";
    const err = new Error(detail);
    err.status = 401;
    err.data = payload;
    throw err;
  }

  if (!response.ok) {
    const detail =
      (payload && typeof payload === "object" && (payload.detail || payload.message)) ||
      (typeof payload === "string" ? payload : `API error ${response.status}`);
    const err = new Error(detail);
    err.status = response.status;
    err.data = payload;
    throw err;
  }

  return enrichResult(payload, response);
}

/* =========================
 * AUTH
 * ========================= */

export const getMe = (opts = {}) => apiFetch("/api/me", opts);

export const forgotPassword = ({ email, tenant, org, token } = {}) =>
  apiFetch("/api/auth/forgot-password", {
    method: "POST",
    org: org || tenant,
    token,
    body: { email, tenant: tenant || org || readTenant() },
  });

export const resetPassword = ({ token: resetToken, password, password_confirm, tenant, org } = {}) =>
  apiFetch("/api/auth/reset-password", {
    method: "POST",
    org: org || tenant,
    body: {
      token: resetToken,
      password,
      password_confirm,
      tenant: tenant || org || readTenant(),
    },
  });

export const validateInvestorAccessCode = ({ code, email = null, tenant = null, org = null } = {}) =>
  apiFetch("/api/auth/validate-access-code", {
    method: "POST",
    org: org || tenant,
    body: {
      code,
      email,
      tenant: tenant || org || readTenant(),
      org: org || tenant || readTenant(),
    },
  });

export const heartbeat = ({ token, org } = {}) =>
  apiFetch("/api/auth/heartbeat", {
    method: "POST",
    token,
    org,
    skipAuthRedirect: true,
  });

/* =========================
 * ADMIN
 * ========================= */

export const getAdminUsers = (opts = {}) => apiFetch("/api/admin/users", opts);

export const approveUser = (userId, opts = {}) =>
  apiFetch(`/api/admin/users/${userId}/approve`, {
    method: "POST",
    ...opts,
  });

export const rejectUser = (userId, opts = {}) =>
  apiFetch(`/api/admin/users/${userId}/reject`, {
    method: "POST",
    ...opts,
  });

export const deleteUser = (userId, opts = {}) =>
  apiFetch(`/api/admin/users/${userId}`, {
    method: "DELETE",
    ...opts,
  });

/* =========================
 * ONBOARDING / PROFILE
 * ========================= */

export const submitOnboarding = (payload, opts = {}) =>
  apiFetch("/api/user/onboarding", {
    method: "POST",
    body: payload,
    ...opts,
  });

/* =========================
 * PUBLIC CHAT
 * ========================= */

export async function publicChat(
  { lead_id, message, thread_id = null } = {},
  opts = {}
) {
  const res = await apiFetch("/api/public/chat", {
    method: "POST",
    skipAuthRedirect: true,
    ...opts,
    body: {
      lead_id,
      message,
      thread_id,
    },
  });

  const payload = res?.data ?? res;
  return {
    ok: true,
    thread_id: payload?.thread_id || thread_id || null,
    reply:
      payload?.reply ||
      payload?.message ||
      payload?.content ||
      payload?.answer ||
      "",
    raw: payload,
  };
}

/* =========================
 * FILES
 * ========================= */

export async function uploadFile(
  file,
  {
    token,
    org,
    threadId = null,
    agentId = null,
    agentIds = null,
    intent = null,
    institutionalRequest = false,
    linkAllAgents = false,
    linkAgent = true,
  } = {}
) {
  const fd = new FormData();
  fd.append("file", file);

  if (threadId) fd.append("thread_id", threadId);
  if (agentId) fd.append("agent_id", agentId);

  if (Array.isArray(agentIds) && agentIds.length) {
    fd.append("agent_ids", agentIds.join(","));
  } else if (typeof agentIds === "string" && agentIds.trim()) {
    fd.append("agent_ids", agentIds.trim());
  }

  if (intent) fd.append("intent", intent);
  fd.append("institutional_request", institutionalRequest ? "true" : "false");
  fd.append("link_all_agents", linkAllAgents ? "true" : "false");
  fd.append("link_agent", linkAgent ? "true" : "false");

  return apiFetch("/api/files/upload", {
    method: "POST",
    token,
    org,
    body: fd,
  });
}

/* =========================
 * CHAT
 * ========================= */

export const chat = ({
  token,
  org,
  tenant,
  thread_id,
  message,
  agent_id,
  top_k,
  trace_id,
  client_message_id,
  signal,
} = {}) =>
  apiFetch("/api/chat", {
    method: "POST",
    token,
    org: org || tenant,
    signal,
    body: {
      thread_id,
      message,
      agent_id,
      top_k,
      trace_id,
      client_message_id,
      tenant: tenant || org || readTenant(),
    },
  });

export async function chatStream({
  token,
  org,
  tenant,
  thread_id,
  message,
  agent_id,
  top_k,
  trace_id,
  client_message_id,
  signal,
} = {}) {
  const response = await fetch(joinApi("/api/chat/stream"), {
    method: "POST",
    headers: headers({
      token,
      org: org || tenant,
      json: true,
    }),
    credentials: "include",
    signal,
    body: JSON.stringify({
      thread_id,
      message,
      agent_id,
      top_k,
      trace_id,
      client_message_id,
      tenant: tenant || org || readTenant(),
    }),
  });

  if (response.status === 401) {
    clearSession();
    if (typeof window !== "undefined") {
      window.location.href = "/auth?session_expired=1";
    }
    throw new Error("Session expired");
  }

  if (!response.ok) {
    const contentType = response.headers.get("content-type") || "";
    let payload = null;
    if (contentType.includes("application/json")) {
      payload = await response.json().catch(() => null);
    } else {
      const text = await response.text().catch(() => "");
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch {
          payload = text;
        }
      }
    }
    const detail =
      (payload && typeof payload === "object" && (payload.detail || payload.message)) ||
      (typeof payload === "string" ? payload : `HTTP ${response.status}`);
    const err = new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
    err.status = response.status;
    err.data = payload;
    throw err;
  }

  return response;
}

export const getOrionSquadHealth = ({ token, org, tenant } = {}) =>
  apiFetch("/api/internal/orion-squad/health", {
    method: "GET",
    token,
    org: org || tenant,
  });

export const getOrionSquadPreview = ({ message, token, org, tenant } = {}) =>
  apiFetch(`/api/internal/orion-squad/preview?message=${encodeURIComponent(message || "")}`, {
    method: "GET",
    token,
    org: org || tenant,
  });

export const getAgentCapabilities = ({ token, org, tenant } = {}) =>
  apiFetch("/api/agents/capabilities", {
    method: "GET",
    token,
    org: org || tenant,
  });

/* =========================
 * AUDIO / STT
 * ========================= */

export async function transcribeAudio(
  blob,
  { token, org, tenant, trace_id = null, language = null, filename = "audio.webm" } = {}
) {
  const fd = new FormData();

  let uploadBlob = blob;
  let uploadName = filename || "audio.webm";

  try {
    const blobType = String(blob?.type || "").toLowerCase();
    if (blobType.startsWith("audio/webm")) {
      uploadBlob = new Blob([blob], { type: "audio/webm" });
      if (!/\.webm$/i.test(uploadName)) uploadName = "audio.webm";
    } else if (blobType.startsWith("audio/mp4")) {
      uploadBlob = new Blob([blob], { type: "audio/mp4" });
      if (!/\.(m4a|mp4)$/i.test(uploadName)) uploadName = "audio.m4a";
    }
  } catch {}

  fd.append("file", uploadBlob, uploadName);
  if (language) fd.append("language", language);

  return apiFetch("/api/audio/transcriptions", {
    method: "POST",
    token,
    org: org || tenant,
    headers: trace_id ? { "X-Trace-Id": trace_id } : {},
    body: fd,
  }).then((res) => res.data || res);
}

/* =========================
 * FOUNDER HANDOFF
 * ========================= */

export const requestFounderHandoff = ({
  token,
  org,
  tenant,
  thread_id,
  interest_type,
  message,
  source,
  consent_contact = true,
} = {}) =>
  apiFetch("/api/founder/handoff", {
    method: "POST",
    token,
    org: org || tenant,
    body: {
      thread_id,
      interest_type,
      message,
      source,
      consent_contact,
    },
  });

/* =========================
 * REALTIME / SUMMIT
 * ========================= */

export const getRealtimeClientSecret = (payload = {}) =>
  startRealtimeSession({
    ...payload,
    mode:
      payload?.mode ||
      import.meta.env.VITE_ORKIO_RUNTIME_MODE ||
      window.__ORKIO_ENV__?.VITE_ORKIO_RUNTIME_MODE ||
      "platform",
  });

export async function startRealtimeSession({
  token,
  org,
  tenant,
  agent_id = null,
  thread_id = null,
  voice = null,
  model = null,
  ttl_seconds = 600,
  mode = "platform",
  response_profile = null,
  language_profile = null,
} = {}) {
  const { data } = await apiFetch("/api/realtime/start", {
    method: "POST",
    token,
    org: org || tenant,
    body: {
      agent_id,
      thread_id,
      voice,
      model,
      ttl_seconds,
      mode,
      response_profile,
      language_profile,
    },
  });
  return data;
}

export async function startSummitSession({
  token,
  org,
  tenant,
  agent_id = null,
  thread_id = null,
  voice = null,
  model = null,
  ttl_seconds = 600,
  mode = "summit",
  response_profile = "stage",
  language_profile = "auto",
} = {}) {
  const { data } = await apiFetch("/api/realtime/start", {
    method: "POST",
    token,
    org: org || tenant,
    body: {
      agent_id,
      thread_id,
      voice,
      model,
      ttl_seconds,
      mode,
      response_profile,
      language_profile,
      language: language_profile,
    },
  });
  return data;
}

export const postRealtimeEventsBatch = ({
  token,
  org,
  tenant,
  session_id,
  events,
} = {}) =>
  apiFetch("/api/realtime/events:batch", {
    method: "POST",
    token,
    org: org || tenant,
    body: { session_id, events: events || [] },
  });

export const endRealtimeSession = ({
  token,
  org,
  tenant,
  session_id,
  ended_at,
  meta,
} = {}) =>
  apiFetch("/api/realtime/end", {
    method: "POST",
    token,
    org: org || tenant,
    body: { session_id, ended_at, meta },
  });

export async function getRealtimeSession({
  token,
  org,
  tenant,
  session_id,
  finals_only = true,
} = {}) {
  const qs = new URLSearchParams();
  qs.set("finals_only", finals_only ? "true" : "false");

  const { data } = await apiFetch(
    `/api/realtime/sessions/${encodeURIComponent(session_id)}?${qs.toString()}`,
    {
      method: "GET",
      token,
      org: org || tenant,
    }
  );
  return data;
}

export const getSummitSessionScore = ({
  token,
  org,
  tenant,
  session_id,
} = {}) =>
  apiFetch(`/api/realtime/sessions/${encodeURIComponent(session_id)}/score`, {
    method: "GET",
    token,
    org: org || tenant,
  });

export const submitSummitSessionReview = ({
  token,
  org,
  tenant,
  session_id,
  clarity,
  naturalness,
  institutional_fit,
} = {}) =>
  apiFetch(`/api/realtime/sessions/${encodeURIComponent(session_id)}/review`, {
    method: "POST",
    token,
    org: org || tenant,
    body: {
      clarity,
      naturalness,
      institutional_fit,
    },
  });

export async function downloadRealtimeAta({
  token,
  org,
  tenant,
  session_id,
} = {}) {
  const response = await fetch(
    joinApi(`/api/realtime/sessions/${encodeURIComponent(session_id)}/ata.txt`),
    {
      method: "GET",
      headers: headers({ token, org: org || tenant, json: false }),
      credentials: "include",
    }
  );

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `HTTP ${response.status}`);
  }

  return await response.blob();
}

export const guardRealtimeTranscript = ({
  token,
  org,
  tenant,
  thread_id,
  message,
} = {}) =>
  apiFetch("/api/realtime/guard", {
    method: "POST",
    token,
    org: org || tenant,
    body: { thread_id, message },
  });

/* =========================
 * FOUNDER ESCALATIONS
 * ========================= */

export const getFounderEscalations = ({ token, org, tenant } = {}) =>
  apiFetch("/api/admin/founder-escalations", {
    method: "GET",
    token,
    org: org || tenant,
  });

export const getFounderEscalation = ({ escalation_id, token, org, tenant } = {}) =>
  apiFetch(`/api/admin/founder-escalations/${encodeURIComponent(escalation_id)}`, {
    method: "GET",
    token,
    org: org || tenant,
  });

export const setFounderEscalationAction = ({
  escalation_id,
  action_type,
  notes = null,
  token,
  org,
  tenant,
} = {}) =>
  apiFetch(`/api/admin/founder-escalations/${encodeURIComponent(escalation_id)}/action`, {
    method: "POST",
    token,
    org: org || tenant,
    body: {
      action_type,
      notes,
    },
  });


export const generateNumerologyProfile = ({ full_name, birth_date, preferred_name = null, context = null, consent = false, token, org } = {}) =>
  apiFetch("/api/numerology/profile", {
    method: "POST",
    token,
    org,
    body: { full_name, birth_date, preferred_name, context, consent },
  });




/* =========================
 * PUBLIC BILLING
 * ========================= */

export const getPublicPlans = (opts = {}) =>
  apiFetch("/api/billing/public/plans", {
    method: "GET",
    skipAuthRedirect: true,
    org: opts.org || "public",
  });

export const getPublicTopups = (opts = {}) =>
  apiFetch("/api/billing/public/topups", {
    method: "GET",
    skipAuthRedirect: true,
    org: opts.org || "public",
  });

export const getPublicUsageRates = (opts = {}) =>
  apiFetch("/api/billing/public/usage-rates", {
    method: "GET",
    skipAuthRedirect: true,
    org: opts.org || "public",
  });

export const createPublicCheckout = ({
  item_code,
  plan_code,
  checkout_kind = "plan",
  full_name,
  name,
  email,
  company,
  currency,
  org,
} = {}) =>
  apiFetch("/api/billing/public/checkout", {
    method: "POST",
    skipAuthRedirect: true,
    org: org || "public",
    body: {
      item_code: item_code || plan_code,
      checkout_kind,
      full_name: full_name || name,
      name: name || full_name,
      email,
      company,
      currency,
    },
  });

export const getPublicCheckoutStatus = ({ checkout_id, email, org } = {}) =>
  apiFetch(`/api/billing/public/checkout-status?checkout_id=${encodeURIComponent(checkout_id || "")}${email ? `&email=${encodeURIComponent(email)}` : ""}`, {
    method: "GET",
    skipAuthRedirect: true,
    org: org || "public",
  });

export const getWalletSummary = ({ token, org, tenant } = {}) =>
  apiFetch("/api/billing/wallet/summary", {
    method: "GET",
    token,
    org: org || tenant,
  });

export const getWalletLedger = ({ limit = 50, token, org, tenant } = {}) =>
  apiFetch(`/api/billing/wallet/ledger?limit=${encodeURIComponent(limit)}`, {
    method: "GET",
    token,
    org: org || tenant,
  });

export const consumeWalletAction = ({ action_key, quantity = 1, note = null, token, org, tenant } = {}) =>
  apiFetch("/api/billing/wallet/consume", {
    method: "POST",
    token,
    org: org || tenant,
    body: { action_key, quantity, note },
  });

export const setWalletAutoRecharge = ({ enabled, pack_code = null, threshold_usd = 3, token, org, tenant } = {}) =>
  apiFetch("/api/billing/wallet/auto-recharge", {
    method: "POST",
    token,
    org: org || tenant,
    body: { enabled, pack_code, threshold_usd },
  });
