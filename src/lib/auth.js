// src/lib/auth.js

const TOKEN_KEY = "orkio_token";
const USER_KEY = "orkio_user";
const TENANT_KEY = "orkio_tenant";
const OTP_CTX_KEY = "orkio_pending_otp_context";

const TERMS_PENDING_KEY = "orkio_terms_pending_acceptance";
const TERMS_VERSION_KEY = "orkio_terms_version";

/**
 * =========================
 * TOKEN
 * =========================
 */

export function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token) {
  if (!token) return;
  localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
}

/**
 * =========================
 * TENANT
 * =========================
 */

export function getTenant() {
  return localStorage.getItem(TENANT_KEY);
}

export function setTenant(tenant) {
  if (!tenant) return;
  localStorage.setItem(TENANT_KEY, tenant);
}

export function clearTenant() {
  localStorage.removeItem(TENANT_KEY);
}

/**
 * =========================
 * USER STORAGE
 * =========================
 */

export function getUser() {
  try {
    const raw = localStorage.getItem(USER_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function setUser(user) {
  if (!user) return;
  localStorage.setItem(USER_KEY, JSON.stringify(normalizeUser(user)));
}

export function clearUser() {
  localStorage.removeItem(USER_KEY);
}

/**
 * =========================
 * NORMALIZE USER
 * =========================
 */

function normalizeUser(user) {
  if (!user) return null;

  const role =
    user.role ||
    (user.is_admin === true ? "admin" : null) ||
    (user.admin === true ? "admin" : null) ||
    "user";

  const adminAccess =
    role === "admin" ||
    role === "owner" ||
    role === "superadmin" ||
    user.is_admin === true ||
    user.admin === true;

  return {
    ...user,
    role,
    is_admin: adminAccess,
    admin: adminAccess,
  };
}

/**
 * =========================
 * OTP CONTEXT
 * =========================
 */

export function savePendingOtpContext(ctx) {
  if (!ctx) return;
  localStorage.setItem(OTP_CTX_KEY, JSON.stringify(ctx));
}

export function getPendingOtpContext() {
  try {
    const raw = localStorage.getItem(OTP_CTX_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function clearPendingOtpContext() {
  localStorage.removeItem(OTP_CTX_KEY);
}

/**
 * =========================
 * SESSION STORAGE
 * =========================
 */

export function setSession({ token, user, tenant }) {
  if (token) setToken(token);

  const existingUser = getUser();
  const mergedUser = user
    ? normalizeUser({
        ...(existingUser || {}),
        ...user,
        role:
          user?.role ||
          existingUser?.role ||
          (user?.is_admin === true ? "admin" : null) ||
          (user?.admin === true ? "admin" : null) ||
          "user",
        is_admin:
          user?.is_admin === true ||
          user?.admin === true ||
          existingUser?.is_admin === true ||
          existingUser?.admin === true ||
          String(user?.role || existingUser?.role || "").trim().toLowerCase() === "admin" ||
          String(user?.role || existingUser?.role || "").trim().toLowerCase() === "owner" ||
          String(user?.role || existingUser?.role || "").trim().toLowerCase() === "superadmin",
        admin:
          user?.admin === true ||
          user?.is_admin === true ||
          existingUser?.admin === true ||
          existingUser?.is_admin === true ||
          String(user?.role || existingUser?.role || "").trim().toLowerCase() === "admin" ||
          String(user?.role || existingUser?.role || "").trim().toLowerCase() === "owner" ||
          String(user?.role || existingUser?.role || "").trim().toLowerCase() === "superadmin",
      })
    : existingUser;

  const resolvedTenant =
    tenant ||
    user?.org_slug ||
    user?.tenant ||
    existingUser?.org_slug ||
    existingUser?.tenant ||
    getTenant() ||
    "public";

  if (resolvedTenant) {
    setTenant(resolvedTenant);
  }

  if (mergedUser) {
    setUser(mergedUser);
  }
}

export const storeSession = setSession;

/**
 * =========================
 * COMPLETE OTP LOGIN
 * =========================
 */

export function completeOtpLogin(data) {
  if (!data?.access_token || !data?.user) {
    throw new Error("Invalid OTP login response");
  }

  const pending = getPendingOtpContext();

  const tenant =
    data.user?.org_slug ||
    data.user?.tenant ||
    data.tenant ||
    pending?.tenant ||
    pending?.org_slug ||
    getTenant() ||
    "public";

  setSession({
    token: data.access_token,
    user: data.user,
    tenant,
  });

  clearPendingOtpContext();
}

/**
 * =========================
 * AUTH STATE
 * =========================
 */

export function isAuthenticated() {
  return Boolean(getToken());
}

/**
 * =========================
 * APPROVAL CHECK
 * =========================
 */

export function isApproved(user) {
  if (!user) return false;

  return Boolean(
    user.approved_at ||
      (typeof user.usage_tier === "string" &&
        user.usage_tier.startsWith("summit")) ||
      user.signup_source === "investor" ||
      user.signup_code_label === "efata777"
  );
}

/**
 * =========================
 * ADMIN ACCESS CHECK
 * =========================
 */

export function isAdmin(user) {
  if (!user) return false;

  return Boolean(
    user.role === "admin" ||
      user.role === "owner" ||
      user.role === "superadmin" ||
      user.is_admin === true ||
      user.admin === true
  );
}

/**
 * =========================
 * MERGE USER FROM /api/me
 * =========================
 */

export function mergeUserFromApiMe(apiUser) {
  if (!apiUser) return;

  const existing = getUser();

  const merged = normalizeUser({
    ...existing,
    ...apiUser,
  });

  setUser(merged);

  const tenant =
    merged?.org_slug ||
    merged?.tenant ||
    getTenant() ||
    "public";

  if (tenant) {
    setTenant(tenant);
  }
}


/**
 * =========================
 * TERMS ACCEPTANCE PENDING
 * =========================
 */

export function markPendingTermsAccepted(termsVersion = "2026-03-01") {
  const payload = {
    accepted: true,
    accepted_at: Date.now(),
    terms_version: String(termsVersion || "2026-03-01"),
  };
  localStorage.setItem(TERMS_PENDING_KEY, JSON.stringify(payload));
  localStorage.setItem(TERMS_VERSION_KEY, payload.terms_version);
  return payload;
}

export function getPendingTermsAccepted() {
  try {
    const raw = localStorage.getItem(TERMS_PENDING_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function clearPendingTermsAccepted() {
  localStorage.removeItem(TERMS_PENDING_KEY);
}

export function getAcceptedTermsVersion() {
  return localStorage.getItem(TERMS_VERSION_KEY) || "2026-03-01";
}

/**
 * =========================
 * CLEAR SESSION
 * =========================
 */

export function clearSession() {
  clearToken();
  clearUser();
  clearTenant();
  clearPendingOtpContext();
}

/**
 * =========================
 * LOGOUT
 * =========================
 */

export function logout() {
  clearSession();
  window.location.href = "/auth";
}

const TERMS_VERSION_ENDPOINT = "/api/public/legal/terms-version";
let termsVersionCache = null;
let termsVersionInFlight = null;

export async function fetchCurrentTermsVersion() {
  if (termsVersionCache) return termsVersionCache;
  if (termsVersionInFlight) return termsVersionInFlight;

  termsVersionInFlight = (async () => {
    try {
      const res = await fetch(TERMS_VERSION_ENDPOINT, {
        method: "GET",
        credentials: "include",
        headers: { "Cache-Control": "no-cache" },
      });
      if (!res.ok) throw new Error(`terms version request failed: ${res.status}`);
      const data = await res.json();
      const version = String(data?.version || "").trim() || getAcceptedTermsVersion();
      termsVersionCache = version;
      try { localStorage.setItem(TERMS_VERSION_KEY, version); } catch {}
      return version;
    } catch {
      return getAcceptedTermsVersion();
    } finally {
      termsVersionInFlight = null;
    }
  })();

  return termsVersionInFlight;
}
