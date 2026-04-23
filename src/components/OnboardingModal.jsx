import React, { useMemo, useState } from "react";
import { getTenant, getToken } from "../lib/auth.js";

const USER_TYPES = [
  { value: "founder", label: "Founder" },
  { value: "investor", label: "Investor" },
  { value: "operator", label: "Operator" },
  { value: "partner", label: "Partner" },
  { value: "other", label: "Other" },
];

const INTENTS = [
  { value: "explore", label: "Explore the platform" },
  { value: "meeting", label: "Schedule a conversation" },
  { value: "pilot", label: "Evaluate a pilot" },
  { value: "funding", label: "Discuss investment" },
  { value: "other", label: "Other" },
];

const COUNTRIES = [
  { value: "BR", label: "Brazil" },
  { value: "US", label: "United States" },
  { value: "ES", label: "Spain" },
  { value: "PT", label: "Portugal" },
  { value: "AR", label: "Argentina" },
  { value: "MX", label: "Mexico" },
  { value: "CO", label: "Colombia" },
  { value: "CL", label: "Chile" },
  { value: "UY", label: "Uruguay" },
  { value: "OTHER", label: "Other" },
];

const LANGUAGES = [
  { value: "en-US", label: "English (US)" },
  { value: "pt-BR", label: "Português (Brasil)" },
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

function normalizeUserType(value) {
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

function normalizeIntent(value) {
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

function normalizeWhatsapp(value) {
  return String(value || "").replace(/[^\d+]/g, "").trim();
}

function suggestLanguage(country) {
  const code = String(country || "").trim().toUpperCase();
  return DEFAULT_LANGUAGE_BY_COUNTRY[code] || "en-US";
}

function sanitizeOnboardingPayload(payload) {
  const country = String(payload?.country || "").trim().toUpperCase() || "BR";
  return {
    company: String(payload?.company || "").trim(),
    role: String(payload?.role || payload?.profile_role || "").trim(),
    user_type: normalizeUserType(payload?.user_type) || "other",
    intent: normalizeIntent(payload?.intent) || "explore",
    country,
    language: String(payload?.language || "").trim() || suggestLanguage(country),
    whatsapp: normalizeWhatsapp(payload?.whatsapp || ""),
    notes: String(payload?.notes || "").trim(),
  };
}

const ORKIO_ENV =
  typeof window !== "undefined" && window.__ORKIO_ENV__ ? window.__ORKIO_ENV__ : {};

function normalizeBase(url) {
  return String(url || "").trim().replace(/\/+$/, "");
}

function resolveApiBase() {
  const envBase = normalizeBase(
    ORKIO_ENV.VITE_API_BASE_URL ||
    ORKIO_ENV.VITE_API_URL ||
    ORKIO_ENV.API_BASE_URL ||
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.VITE_API_URL ||
    ""
  );

  if (envBase) return envBase;

  if (typeof window !== "undefined" && window.location?.origin) {
    return normalizeBase(window.location.origin);
  }
  return "";
}

function buildUrl(path) {
  const base = resolveApiBase();
  const cleanPath = String(path || "").startsWith("/") ? path : `/${path}`;
  return `${base}${cleanPath}`;
}

function buildHeaders(token, org) {
  const headers = {
    "Content-Type": "application/json",
    Accept: "application/json",
  };
  if (token) headers.Authorization = `Bearer ${token}`;
  if (org) headers["X-Org-Slug"] = org;
  return headers;
}

async function readErrorMessage(res) {
  try {
    const data = await res.json();
    return data?.detail || data?.message || JSON.stringify(data);
  } catch {
    try {
      return await res.text();
    } catch {
      return `${res.status} ${res.statusText}`;
    }
  }
}

async function saveOnboarding(payload, token, org) {
  const url = buildUrl("/api/user/onboarding");
  const res = await fetch(url, {
    method: "POST",
    headers: buildHeaders(token, org),
    body: JSON.stringify(payload),
    credentials: "include",
  });

  if (!res.ok) {
    const msg = await readErrorMessage(res);
    throw new Error(msg || `Onboarding failed (${res.status})`);
  }

  try {
    return await res.json();
  } catch {
    return { status: "ok" };
  }
}

const labelStyle = {
  display: "block",
  marginBottom: 8,
  color: "#0f172a",
  fontWeight: 800,
  fontSize: 14,
};

const fieldStyle = {
  width: "100%",
  borderRadius: 14,
  border: "1px solid #cbd5e1",
  background: "#ffffff",
  color: "#0f172a",
  padding: "14px 16px",
  fontSize: 15,
  outline: "none",
  boxSizing: "border-box",
  appearance: "none",
  WebkitAppearance: "none",
  MozAppearance: "none",
  WebkitTextFillColor: "#0f172a",
  boxShadow: "inset 0 1px 2px rgba(15,23,42,0.04)",
};

const optionStyle = {
  backgroundColor: "#ffffff",
  color: "#0f172a",
};

export default function OnboardingModal({ user, onComplete }) {
  const [form, setForm] = useState(() => sanitizeOnboardingPayload(user));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const fullName = useMemo(() => (user?.name || "").trim(), [user]);

  function setField(key, value) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  async function handleSubmit(e) {
    e?.preventDefault?.();

    const payload = {
      company: form.company || null,
      role: form.role || null,
      user_type: form.user_type || "other",
      intent: form.intent || "explore",
      country: form.country || "BR",
      language: form.language || suggestLanguage(form.country || "BR"),
      whatsapp: normalizeWhatsapp(form.whatsapp || ""),
      notes: form.notes || null,
      onboarding_completed: true,
    };

    setBusy(true);
    setError("");

    try {
      const token = getToken();
      const org = user?.org_slug || getTenant() || "public";

      const result = await saveOnboarding(payload, token, org);
      const nextUser = result?.user
        ? { ...user, ...result.user, onboarding_completed: true }
        : {
            ...user,
            company: payload.company,
            profile_role: payload.role,
            user_type: payload.user_type,
            intent: payload.intent,
            country: payload.country,
            language: payload.language,
            whatsapp: payload.whatsapp,
            notes: payload.notes,
            onboarding_completed: true,
          };

      onComplete?.(nextUser);
    } catch (err) {
      setError(err?.message || "Could not save onboarding.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 1000,
        background: "rgba(5,8,18,0.72)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 16,
      }}
    >
      <form
        onSubmit={handleSubmit}
        style={{
          width: "100%",
          maxWidth: 760,
          maxHeight: "92vh",
          overflowY: "auto",
          borderRadius: 24,
          border: "1px solid rgba(15,23,42,0.08)",
          background: "linear-gradient(180deg, #ffffff 0%, #f8fafc 100%)",
          boxShadow: "0 24px 80px rgba(0,0,0,0.28)",
          color: "#0f172a",
          padding: 20,
          boxSizing: "border-box",
        }}
      >
        <div style={{ marginBottom: 16 }}>
          <div
            style={{
              fontSize: 12,
              letterSpacing: "0.14em",
              textTransform: "uppercase",
              color: "#475569",
              fontWeight: 800,
            }}
          >
            Orkio
          </div>
          <h2 style={{ margin: "8px 0 6px", fontSize: 30, lineHeight: 1.1 }}>
            Complete your onboarding
          </h2>
          <p style={{ margin: 0, color: "#475569", lineHeight: 1.55 }}>
            Tell us a bit about your context so we can personalize your console experience from the first session.
          </p>
        </div>

        <div
          style={{
            marginBottom: 18,
            borderRadius: 18,
            border: "1px solid #dbeafe",
            background: "linear-gradient(135deg, rgba(37,99,235,0.08), rgba(124,92,255,0.08))",
            padding: "14px 16px",
            color: "#334155",
            fontSize: 14,
            lineHeight: 1.5,
          }}
        >
          This step appears only once. You can adjust the language later inside the console if needed.
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 14 }}>
          <div>
            <label style={labelStyle}>Full name</label>
            <input value={fullName} readOnly style={{ ...fieldStyle, opacity: 0.85 }} />
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 14 }}>
            <div>
              <label style={labelStyle}>Company</label>
              <input
                value={form.company}
                onChange={(e) => setField("company", e.target.value)}
                placeholder="Your company"
                style={fieldStyle}
              />
            </div>

            <div>
              <label style={labelStyle}>Role / title</label>
              <input
                value={form.role}
                onChange={(e) => setField("role", e.target.value)}
                placeholder="Founder, Partner, CTO..."
                style={fieldStyle}
              />
            </div>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 14 }}>
            <div>
              <label style={labelStyle}>Profile</label>
              <select
                value={form.user_type}
                onChange={(e) => setField("user_type", e.target.value || "other")}
                style={fieldStyle}
              >
                {USER_TYPES.map((opt) => (
                  <option key={opt.value} value={opt.value} style={optionStyle}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label style={labelStyle}>Primary goal</label>
              <select
                value={form.intent}
                onChange={(e) => setField("intent", e.target.value || "explore")}
                style={fieldStyle}
              >
                {INTENTS.map((opt) => (
                  <option key={opt.value} value={opt.value} style={optionStyle}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 14 }}>
            <div>
              <label style={labelStyle}>Country</label>
              <select
                value={form.country}
                onChange={(e) => {
                  const nextCountry = e.target.value || "US";
                  setForm((prev) => ({
                    ...prev,
                    country: nextCountry,
                    language:
                      !prev.language || prev.language === suggestLanguage(prev.country || "US")
                        ? suggestLanguage(nextCountry)
                        : prev.language,
                  }));
                }}
                style={fieldStyle}
              >
                {COUNTRIES.map((opt) => (
                  <option key={opt.value} value={opt.value} style={optionStyle}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label style={labelStyle}>Language</label>
              <select
                value={form.language}
                onChange={(e) => setField("language", e.target.value || suggestLanguage(form.country))}
                style={fieldStyle}
              >
                {LANGUAGES.map((opt) => (
                  <option key={opt.value} value={opt.value} style={optionStyle}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div>
            <label style={labelStyle}>WhatsApp</label>
            <input
              value={form.whatsapp}
              onChange={(e) => setField("whatsapp", normalizeWhatsapp(e.target.value))}
              placeholder="+1 555 000 0000"
              style={fieldStyle}
              inputMode="tel"
            />
          </div>

          <div>
            <label style={labelStyle}>Additional context</label>
            <textarea
              value={form.notes}
              onChange={(e) => setField("notes", e.target.value)}
              placeholder="In one sentence, tell us what you want to solve or explore."
              style={{ ...fieldStyle, minHeight: 120, resize: "vertical" }}
            />
          </div>
        </div>

        {error ? (
          <div
            style={{
              marginTop: 16,
              color: "#0f172a",
              background: "#eff6ff",
              border: "1px solid #bfdbfe",
              borderRadius: 14,
              padding: "12px 14px",
              fontSize: 14,
            }}
          >
            {error}
          </div>
        ) : null}

        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: 12,
            alignItems: "center",
            marginTop: 18,
            flexWrap: "wrap",
          }}
        >
          <div style={{ color: "#64748b", fontSize: 13 }}>
            Your preferred language can be updated later inside the console settings.
          </div>

          <button
            type="submit"
            disabled={busy}
            style={{
              border: 0,
              borderRadius: 16,
              padding: "14px 18px",
              minWidth: 220,
              cursor: "pointer",
              background: "linear-gradient(135deg, #2563eb, #7c3aed)",
              color: "#ffffff",
              fontWeight: 800,
              fontSize: 15,
              boxShadow: "0 14px 30px rgba(37,99,235,0.24)",
              opacity: busy ? 0.75 : 1,
            }}
          >
            {busy ? "Saving..." : "Continue to console"}
          </button>
        </div>
      </form>
    </div>
  );
}
