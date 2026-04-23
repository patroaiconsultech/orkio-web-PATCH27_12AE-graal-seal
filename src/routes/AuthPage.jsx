import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { apiFetch, createPublicCheckout } from "../ui/api.js";
import OrkioSphereMark from "../ui/OrkioSphereMark.jsx";
import {
  setTenant,
  savePendingOtpContext,
  getPendingOtpContext,
  completeOtpLogin,
  getToken,
  getUser,
  isApproved,
  isAdmin,
  getPendingTermsAccepted,
  clearPendingTermsAccepted,
  getAcceptedTermsVersion,
} from "../lib/auth.js";

const shell = {
  minHeight: "100vh",
  display: "grid",
  placeItems: "center",
  padding: 20,
  background:
    "radial-gradient(circle at 12% 0%, rgba(34,211,238,0.14) 0%, rgba(6,10,20,0) 32%), radial-gradient(circle at 88% 0%, rgba(124,58,237,0.18) 0%, rgba(6,10,20,0) 34%), linear-gradient(180deg, #060812 0%, #040712 100%)",
};

const card = {
  width: "100%",
  maxWidth: 560,
  borderRadius: 32,
  border: "1px solid rgba(255,255,255,0.10)",
  background: "rgba(255,255,255,0.96)",
  color: "#0f172a",
  boxShadow: "0 30px 90px rgba(2,6,23,0.45)",
  padding: 28,
};

const label = { display: "block", marginBottom: 8, fontSize: 13, fontWeight: 700, color: "#334155" };
const input = {
  width: "100%",
  padding: "14px 16px",
  borderRadius: 16,
  border: "1px solid #cbd5e1",
  background: "#ffffff",
  color: "#0f172a",
  outline: "none",
  fontSize: 15,
  boxSizing: "border-box",
};
const btn = {
  width: "100%",
  border: 0,
  borderRadius: 18,
  padding: "15px 18px",
  fontWeight: 800,
  fontSize: 15,
  cursor: "pointer",
  background: "linear-gradient(135deg, #2563eb, #0f172a)",
  color: "#fff",
};
const secondaryBtn = {
  width: "100%",
  border: "1px solid #cbd5e1",
  borderRadius: 18,
  padding: "15px 18px",
  fontWeight: 700,
  fontSize: 15,
  cursor: "pointer",
  background: "#ffffff",
  color: "#0f172a",
};
const linkBtn = {
  border: 0,
  background: "transparent",
  padding: 0,
  margin: 0,
  color: "#2563eb",
  fontWeight: 700,
  cursor: "pointer",
  textAlign: "left",
};
const muted = { color: "#64748b", fontSize: 14, lineHeight: 1.5 };

const adminChip = {
  border: "1px solid rgba(15,23,42,0.10)",
  background: "rgba(255,255,255,0.75)",
  color: "#475569",
  fontSize: 12,
  padding: "6px 10px",
  borderRadius: 999,
  cursor: "pointer",
};

const eyeBtn = {
  position: "absolute",
  right: 10,
  top: "50%",
  transform: "translateY(-50%)",
  border: 0,
  background: "transparent",
  color: "#475569",
  cursor: "pointer",
  fontWeight: 700,
};

const sidePanel = {
  width: "100%",
  borderRadius: 32,
  border: "1px solid rgba(255,255,255,0.10)",
  background: "linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03))",
  color: "#ffffff",
  boxShadow: "0 30px 90px rgba(2,6,23,0.35)",
  padding: 28,
  backdropFilter: "blur(18px)",
};

const sideChip = {
  display: "inline-flex",
  alignItems: "center",
  gap: 10,
  padding: "10px 14px",
  borderRadius: 999,
  border: "1px solid rgba(255,255,255,0.10)",
  background: "rgba(255,255,255,0.04)",
  fontSize: 11,
  fontWeight: 800,
  letterSpacing: "0.18em",
  textTransform: "uppercase",
  color: "rgba(255,255,255,0.62)",
};

const planPill = {
  borderRadius: 20,
  border: "1px solid rgba(255,255,255,0.10)",
  background: "rgba(255,255,255,0.04)",
  padding: "14px 16px",
};


function PasswordField({ labelText, placeholder, value, onChange, show, onToggle }) {
  return (
    <div>
      <label style={label}>{labelText}</label>
      <div style={{ position: "relative" }}>
        <input
          style={{ ...input, paddingRight: 64 }}
          type={show ? "text" : "password"}
          placeholder={placeholder}
          value={value}
          onChange={onChange}
        />
        <button type="button" onClick={onToggle} style={eyeBtn}>
          {show ? "Hide" : "Show"}
        </button>
      </div>
    </div>
  );
}

export default function AuthPage() {
  const nav = useNavigate();
  const [tenant] = useState("public");

  const [mode, setMode] = useState("register"); // register | login | forgot | reset
  const [otpMode, setOtpMode] = useState(false);

  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [passwordConfirm, setPasswordConfirm] = useState("");
  const [accessCode, setAccessCode] = useState("");
  const [acceptTerms, setAcceptTerms] = useState(false);
  const [selectedPlan, setSelectedPlan] = useState("founder_access");

  const [otpCode, setOtpCode] = useState("");
  const [pendingEmail, setPendingEmail] = useState("");
  const [resetToken, setResetToken] = useState("");

  const [showPassword, setShowPassword] = useState(false);
  const [showPasswordConfirm, setShowPasswordConfirm] = useState(false);
  const [showLoginPassword, setShowLoginPassword] = useState(false);
  const [showResetPassword, setShowResetPassword] = useState(false);
  const [showResetPasswordConfirm, setShowResetPasswordConfirm] = useState(false);

  const [status, setStatus] = useState("");
  const [busy, setBusy] = useState(false);

  const token = getToken();
  const currentUser = getUser();
  const showAdminShortcut =
    !!token &&
    !!currentUser &&
    isApproved(currentUser) &&
    isAdmin(currentUser);

  useEffect(() => {
    const token = getToken();
    const user = getUser();
    if (token && user && isApproved(user)) {
      const redirect = sessionStorage.getItem("post_auth_redirect");
      const next = isAdmin(user) ? "/admin" : (redirect || "/app");
      sessionStorage.removeItem("post_auth_redirect");
      nav(next, { replace: true });
    }
  }, [nav]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const requestedMode = (params.get("mode") || "").toLowerCase();
    const requestedPlan = (params.get("plan") || "").toLowerCase();
    const tokenFromUrl = params.get("token") || "";
    if (requestedMode === "login" || requestedMode === "signin") {
      setMode("login");
    } else if (requestedMode === "forgot") {
      setMode("forgot");
    } else if (requestedMode === "reset") {
      setMode("reset");
      setResetToken(tokenFromUrl);
    }

    if (["founder_access","pro_access","team_access","business_monthly","professional_monthly"].includes(requestedPlan || "")) {
      setSelectedPlan(requestedPlan);
      setMode("register");
    }

    if (params.get("accepted_terms") === "1" || getPendingTermsAccepted()) {
      setAcceptTerms(true);
      setStatus("Terms accepted. Please sign in to continue.");
    }

    const ctx = getPendingOtpContext();
    if (ctx?.email) {
      setOtpMode(true);
      setPendingEmail(ctx.email);
      setEmail(ctx.email);
    }

    const checkoutStatus = params.get("checkout");
    const checkoutId = params.get("checkout_id");
    const pendingCheckout = readCheckoutCtx();

    if (checkoutStatus === "success" && checkoutId && pendingCheckout?.email) {
      setStatus("Checking payment confirmation...");
      setMode("register");
      setName(pendingCheckout.name || "");
      setEmail(pendingCheckout.email || "");
      setPassword(pendingCheckout.password || "");
      setPasswordConfirm(pendingCheckout.password || "");
      setSelectedPlan(pendingCheckout.selectedPlan || "founder_access");
      setAcceptTerms(!!pendingCheckout.acceptTerms);

      apiFetch(`/api/billing/public/checkout-status?checkout_id=${encodeURIComponent(checkoutId)}&email=${encodeURIComponent(pendingCheckout.email)}`, {
        method: "GET",
        org: pendingCheckout.tenant || tenant,
        skipAuthRedirect: true,
      })
        .then(async ({ data }) => {
          if (data?.entitlement_active) {
            setStatus("Payment confirmed. Creating your account...");
            await completeRegistration({
              nameValue: pendingCheckout.name || "",
              emailValue: normalizeEmail(pendingCheckout.email),
              passwordValue: pendingCheckout.password || "",
              accessCodeValue: "",
            });
            clearCheckoutCtx();
            return;
          }
          setStatus("Payment received, but confirmation is still pending. Refresh this page in a few seconds.");
        })
        .catch((err) => {
          setStatus(err?.message || "Could not confirm payment yet.");
        });
    }
  }, []);

  const title = useMemo(() => {
    if (otpMode) return "Verify your access code";
    if (mode === "login") return "Sign in to your account";
    if (mode === "forgot") return "Recover your password";
    if (mode === "reset") return "Create a new password";
    return "Create your account";
  }, [otpMode, mode]);

  const subtitle = useMemo(() => {
    if (otpMode) {
      return "Use the one-time code sent to your email to enter the console.";
    }
    if (mode === "login") {
      return "Sign in with your email and password. If required, we will send a one-time code to complete access.";
    }
    if (mode === "forgot") {
      return "Enter your email and we will send a password reset link if the account exists.";
    }
    if (mode === "reset") {
      return "Set your new password to recover access to the console.";
    }
    return "Use a private access code or continue to secure checkout, then verify your email with OTP to enter the console.";
  }, [otpMode, mode]);

  function normalizeEmail(v) {
    return String(v || "").trim().toLowerCase();
  }

  const PENDING_CHECKOUT_CTX_KEY = "orkio_pending_checkout_ctx";

  function normalizeAccessCode(v) {
    return String(v || "").trim().toUpperCase();
  }

  function saveCheckoutCtx(ctx) {
    try { localStorage.setItem(PENDING_CHECKOUT_CTX_KEY, JSON.stringify(ctx)); } catch {}
  }

  function readCheckoutCtx() {
    try {
      const raw = localStorage.getItem(PENDING_CHECKOUT_CTX_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  }

  function clearCheckoutCtx() {
    try { localStorage.removeItem(PENDING_CHECKOUT_CTX_KEY); } catch {}
  }

  function setAuthMode(nextMode) {
    setMode(nextMode);
    setOtpMode(false);
    setOtpCode("");
    setStatus("");
    const url = new URL(window.location.href);
    url.searchParams.set("mode", nextMode);
    if (nextMode !== "reset") {
      url.searchParams.delete("token");
      setResetToken("");
    }
    window.history.replaceState({}, "", `${url.pathname}${url.search}`);
  }

  function goToAdminDirect() {
    nav("/admin");
  }

  async function fetchCurrentTermsVersion() {
    try {
      const { data } = await apiFetch("/api/public/legal/terms-version", {
        method: "GET",
        org: tenant,
        skipAuthRedirect: true,
      });
      return data?.terms_version || data?.version || getAcceptedTermsVersion() || null;
    } catch {
      return getAcceptedTermsVersion() || null;
    }
  }

  async function finalizeSession(data, resolvedTenant) {
    const nextTenant = resolvedTenant || tenant || "public";
    setTenant(nextTenant);

    if (!data?.access_token || !data?.user) {
      throw new Error("Invalid session payload.");
    }

    completeOtpLogin({ ...data, tenant: nextTenant });

    const pendingTerms = getPendingTermsAccepted();
    if (pendingTerms?.accepted) {
      try {
        const currentTermsVersion = await fetchCurrentTermsVersion();
        await apiFetch("/api/me/accept-terms", {
          method: "POST",
          token: getToken(),
          org: nextTenant,
          skipAuthRedirect: true,
          body: {
            accepted: true,
            terms_version: pendingTerms.terms_version || currentTermsVersion || getAcceptedTermsVersion(),
          },
        });
        clearPendingTermsAccepted();
      } catch (err) {
        console.warn("terms acceptance sync failed", err);
      }
    }

    const storedUser = getUser();
    const redirect = sessionStorage.getItem("post_auth_redirect");
    const next = isAdmin(storedUser) ? "/admin" : (redirect || "/app");

    sessionStorage.removeItem("post_auth_redirect");
    nav(next, { replace: true });
  }


  async function completeRegistration({ nameValue, emailValue, passwordValue, accessCodeValue = "" }) {
    const registerPayload = {
      tenant,
      email: emailValue,
      name: nameValue,
      password: passwordValue,
      access_code: accessCodeValue || undefined,
      accept_terms: acceptTerms,
      marketing_consent: false,
    };

    await apiFetch("/api/auth/register", {
      method: "POST",
      org: tenant,
      body: registerPayload,
    });

    setStatus("Account created. Sending OTP...");
    savePendingOtpContext({
      email: emailValue,
      tenant,
      name: nameValue,
      accessCode: accessCodeValue || "",
    });
    setPendingEmail(emailValue);
    setOtpMode(true);

    const { data: loginData } = await apiFetch("/api/auth/login", {
      method: "POST",
      org: tenant,
      body: { tenant, email: emailValue, password: passwordValue },
    });

    if (loginData?.pending_otp) {
      savePendingOtpContext({
        email: loginData.email || emailValue,
        tenant,
        name: nameValue,
        accessCode: accessCodeValue || "",
      });
      setPendingEmail(loginData.email || emailValue);
      setStatus(loginData.message || "OTP sent. Verify it to enter the console.");
      return;
    }

    if (loginData?.access_token && loginData?.user) {
      await finalizeSession(loginData, tenant);
      return;
    }

    setStatus(loginData?.message || "Account created, but OTP was not issued correctly.");
  }

  async function startPaidCheckout({ nameValue, emailValue }) {
    saveCheckoutCtx({
      tenant,
      name: nameValue,
      email: emailValue,
      password,
      selectedPlan,
      acceptTerms,
    });

    const { data } = await createPublicCheckout({
      org: tenant,
      item_code: selectedPlan,
      full_name: nameValue,
      email: emailValue,
    });

    if (data?.already_active) {
      await completeRegistration({
        nameValue,
        emailValue,
        passwordValue: password,
        accessCodeValue: "",
      });
      return;
    }

    if (!data?.checkout_url) {
      throw new Error("Checkout link not available.");
    }

    window.location.href = data.checkout_url;
  }


  async function doRegister() {
    if (busy) return;

    if (password !== passwordConfirm) {
      setStatus("Passwords do not match.");
      return;
    }
    if (!acceptTerms) {
      setStatus("You must accept the terms to continue.");
      return;
    }

    const nameNormalized = String(name || "").trim();
    const emailNormalized = normalizeEmail(email);
    const normalizedAccessCode = normalizeAccessCode(accessCode);

    if (!nameNormalized) {
      setStatus("Please enter your full name.");
      return;
    }

    if (!emailNormalized || !password) {
      setStatus("Please complete name, email, and password.");
      return;
    }

    setBusy(true);
    setStatus(normalizedAccessCode ? "Creating your account..." : "Preparing secure checkout...");

    try {
      if (normalizedAccessCode) {
        await completeRegistration({
          nameValue: nameNormalized,
          emailValue: emailNormalized,
          passwordValue: password,
          accessCodeValue: normalizedAccessCode,
        });
      } else {
        await startPaidCheckout({
          nameValue: nameNormalized,
          emailValue: emailNormalized,
        });
      }
    } catch (err) {
      setStatus(err?.message || "Registration failed.");
    } finally {
      setBusy(false);
    }
  }

  async function doLogin() {
    if (busy) return;

    const emailNormalized = normalizeEmail(email);

    if (!emailNormalized || !password) {
      setStatus("Please enter your email and password.");
      return;
    }

    setBusy(true);
    setStatus("Signing you in...");

    try {
      const { data } = await apiFetch("/api/auth/login", {
        method: "POST",
        org: tenant,
        body: {
          tenant,
          email: emailNormalized,
          password,
        },
      });

      if (data?.pending_otp) {
        savePendingOtpContext({
          email: data.email || emailNormalized,
          tenant,
        });
        setPendingEmail(data.email || emailNormalized);
        setOtpMode(true);
        setStatus(data?.message || "OTP sent. Check your email.");
        return;
      }

      if (data?.access_token && data?.user) {
        await finalizeSession(data, tenant);
        return;
      }

      setStatus(data?.message || "Unable to complete sign in.");
    } catch (err) {
      setStatus(err?.message || "Sign in failed.");
    } finally {
      setBusy(false);
    }
  }

  async function doForgotPassword() {
    if (busy) return;
    const emailNormalized = normalizeEmail(email);
    if (!emailNormalized) {
      setStatus("Please enter your email.");
      return;
    }
    setBusy(true);
    setStatus("Sending reset link...");
    try {
      const { data } = await apiFetch("/api/auth/forgot-password", {
        method: "POST",
        org: tenant,
        body: {
          tenant,
          email: emailNormalized,
        },
      });
      setStatus(data?.message || "If the account exists, a reset link has been sent.");
    } catch (err) {
      setStatus(err?.message || "Unable to request password reset.");
    } finally {
      setBusy(false);
    }
  }

  async function doResetPassword() {
    if (busy) return;

    if (!resetToken) {
      setStatus("Missing reset token.");
      return;
    }
    if (password !== passwordConfirm) {
      setStatus("Passwords do not match.");
      return;
    }
    if (!password || !passwordConfirm) {
      setStatus("Please fill both password fields.");
      return;
    }

    setBusy(true);
    setStatus("Updating your password...");
    try {
      const { data } = await apiFetch("/api/auth/reset-password", {
        method: "POST",
        org: tenant,
        body: {
          tenant,
          token: resetToken,
          password,
          password_confirm: passwordConfirm,
        },
      });
      setStatus(data?.message || "Password updated. You can sign in now.");
      setPassword("");
      setPasswordConfirm("");
      setAuthMode("login");
    } catch (err) {
      setStatus(err?.message || "Unable to reset password.");
    } finally {
      setBusy(false);
    }
  }

  async function doVerifyOtp() {
    if (busy) return;

    const ctx = getPendingOtpContext();
    const resolvedTenant = ctx?.tenant || tenant;
    const emailNormalized = normalizeEmail(ctx?.email || pendingEmail || email);
    const code = String(otpCode || "").trim();

    if (!emailNormalized || !code) {
      setStatus("Please enter the OTP sent by email.");
      return;
    }

    setBusy(true);
    setStatus("Verifying code...");

    try {
      const { data } = await apiFetch("/api/auth/login/verify-otp", {
        method: "POST",
        org: resolvedTenant,
        body: {
          tenant: resolvedTenant,
          email: emailNormalized,
          code,
        },
      });

      if (!data?.access_token || !data?.user) {
        setStatus(data?.message || "Invalid code or session not finalized.");
        return;
      }

      await finalizeSession(data, resolvedTenant);
    } catch (err) {
      setStatus(err?.message || "OTP validation failed.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={shell}>
      <div
        style={{
          width: "100%",
          maxWidth: 1180,
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit,minmax(320px,1fr))",
          gap: 24,
          alignItems: "start",
        }}
      >
        <div style={sidePanel}>
          <div style={sideChip}>
            <OrkioSphereMark size={18} glow={false} ring={false} />
            private access • pwa launch
          </div>

          <div style={{ marginTop: 24, display: "flex", alignItems: "center", gap: 14 }}>
            <OrkioSphereMark size={64} badge />
            <div>
              <div style={{ fontSize: 12, textTransform: "uppercase", letterSpacing: "0.18em", color: "rgba(255,255,255,0.5)", fontWeight: 900 }}>
                Orkio
              </div>
              <div style={{ marginTop: 6, fontSize: 28, lineHeight: 1.02, fontWeight: 900 }}>
                Beautiful entry.<br />Serious product.
              </div>
            </div>
          </div>

          <p style={{ marginTop: 20, color: "rgba(255,255,255,0.72)", lineHeight: 1.8, fontSize: 15 }}>
            Entre no PWA por um fluxo mais bonito, direto e comercial. Checkout, códigos privados e aprovação seguem governados.
          </p>

          <div style={{ marginTop: 24, display: "grid", gap: 14 }}>
            {[
              ["Professional", "R$ 197/mês", "For founders and consultants who need speed with structure."],
              ["Business", "R$ 497/mês", "For operators who need more capacity, control and commercial leverage."],
              ["Private code", "Efata777", "Selected users can bypass checkout with a governed invite code."],
            ].map(([title, value, desc]) => (
              <div key={title} style={planPill}>
                <div style={{ fontSize: 12, textTransform: "uppercase", letterSpacing: "0.14em", color: "rgba(255,255,255,0.42)", fontWeight: 800 }}>{title}</div>
                <div style={{ marginTop: 6, fontSize: 24, lineHeight: 1, fontWeight: 900 }}>{value}</div>
                <div style={{ marginTop: 8, color: "rgba(255,255,255,0.68)", fontSize: 14, lineHeight: 1.7 }}>{desc}</div>
              </div>
            ))}
          </div>
        </div>

      <div style={card}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <OrkioSphereMark size={28} badge={false} glow={false} />
            <div style={{ fontSize: 12, textTransform: "uppercase", letterSpacing: "0.14em", color: "#64748b", fontWeight: 800 }}>
              Orkio access
            </div>
          </div>
          {showAdminShortcut ? (
            <button type="button" onClick={goToAdminDirect} style={adminChip} title="Admin Console">
              admin
            </button>
          ) : null}
        </div>

        <h1 style={{ margin: "10px 0 8px", fontSize: 32, lineHeight: 1.05 }}>{title}</h1>
        <p style={{ ...muted, marginTop: 0 }}>{subtitle}</p>

        {!otpMode ? (
          <>
            {mode !== "forgot" && mode !== "reset" ? (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 18 }}>
                <button
                  type="button"
                  style={mode === "register" ? btn : secondaryBtn}
                  onClick={() => setAuthMode("register")}
                  disabled={busy}
                >
                  Create account
                </button>
                <button
                  type="button"
                  style={mode === "login" ? btn : secondaryBtn}
                  onClick={() => setAuthMode("login")}
                  disabled={busy}
                >
                  Sign in
                </button>
              </div>
            ) : null}

            {mode === "register" ? (
              <div style={{ display: "grid", gap: 14 }}>
                <div>
                  <label style={label}>Full name</label>
                  <input style={input} placeholder="Your name" value={name} onChange={(e) => setName(e.target.value)} />
                </div>

                <div>
                  <label style={label}>Email</label>
                  <input style={input} placeholder="you@company.com" value={email} onChange={(e) => setEmail(e.target.value)} />
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
                  <PasswordField
                    labelText="Password"
                    placeholder="Your password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    show={showPassword}
                    onToggle={() => setShowPassword((v) => !v)}
                  />
                  <PasswordField
                    labelText="Confirm password"
                    placeholder="Repeat your password"
                    value={passwordConfirm}
                    onChange={(e) => setPasswordConfirm(e.target.value)}
                    show={showPasswordConfirm}
                    onToggle={() => setShowPasswordConfirm((v) => !v)}
                  />
                </div>

                <div style={{ display: "grid", gap: 8 }}>
                  <label style={label}>Plan</label>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                    <button
                      type="button"
                      onClick={() => setSelectedPlan("founder_access")}
                      style={{
                        ...secondaryBtn,
                        borderColor: selectedPlan === "founder_access" ? "#2563eb" : "#cbd5e1",
                        background: selectedPlan === "founder_access" ? "rgba(37,99,235,0.06)" : "#fff",
                      }}
                    >
                      Founder Access<br /><span style={{ fontSize: 12, color: "#64748b" }}>US$ 20/mês + US$ 20 créditos</span>
                    </button>
                    <button
                      type="button"
                      onClick={() => setSelectedPlan("pro_access")}
                      style={{
                        ...secondaryBtn,
                        borderColor: selectedPlan === "pro_access" ? "#2563eb" : "#cbd5e1",
                        background: selectedPlan === "pro_access" ? "rgba(37,99,235,0.06)" : "#fff",
                      }}
                    >
                      Pro Access<br /><span style={{ fontSize: 12, color: "#64748b" }}>US$ 49/mês + US$ 60 créditos</span>
                    </button>
                  </div>
                </div>

                <div>
                  <label style={label}>Private access code (optional)</label>
                  <input
                    style={input}
                    placeholder="Use Efata777 or another invite code"
                    value={accessCode}
                    onChange={(e) => setAccessCode(e.target.value.toUpperCase())}
                  />
                  <div style={{ ...muted, marginTop: 8 }}>
                    Selected users can enter directly with a private code. Everyone else continues to secure checkout.
                  </div>
                </div>

                <label style={{ display: "flex", gap: 10, alignItems: "flex-start", color: "#334155", fontSize: 14 }}>
                  <input type="checkbox" checked={acceptTerms} onChange={(e) => setAcceptTerms(e.target.checked)} />
                  <span>I agree to the terms and privacy policy.</span>
                </label>

                <button style={btn} disabled={busy} onClick={doRegister}>
                  {busy ? "Processing..." : (accessCode.trim() ? "Create account and receive OTP" : "Continue to secure checkout")}
                </button>
              </div>
            ) : null}

            {mode === "login" ? (
              <div style={{ display: "grid", gap: 14 }}>
                <div>
                  <label style={label}>Email</label>
                  <input style={input} placeholder="you@company.com" value={email} onChange={(e) => setEmail(e.target.value)} />
                </div>

                <PasswordField
                  labelText="Password"
                  placeholder="Your password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  show={showLoginPassword}
                  onToggle={() => setShowLoginPassword((v) => !v)}
                />

                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
                  <button type="button" style={linkBtn} onClick={() => setAuthMode("forgot")}>
                    Forgot password?
                  </button>
                </div>

                <button style={btn} disabled={busy} onClick={doLogin}>
                  {busy ? "Processing..." : "Sign in"}
                </button>
              </div>
            ) : null}

            {mode === "forgot" ? (
              <div style={{ display: "grid", gap: 14 }}>
                <div>
                  <label style={label}>Email</label>
                  <input style={input} placeholder="you@company.com" value={email} onChange={(e) => setEmail(e.target.value)} />
                </div>
                <button style={btn} disabled={busy} onClick={doForgotPassword}>
                  {busy ? "Processing..." : "Send reset link"}
                </button>
                <button type="button" style={secondaryBtn} disabled={busy} onClick={() => setAuthMode("login")}>
                  Back to sign in
                </button>
              </div>
            ) : null}

            {mode === "reset" ? (
              <div style={{ display: "grid", gap: 14 }}>
                <div>
                  <label style={label}>Reset token</label>
                  <input style={input} placeholder="Paste your reset token" value={resetToken} onChange={(e) => setResetToken(e.target.value)} />
                </div>
                <PasswordField
                  labelText="New password"
                  placeholder="Your new password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  show={showResetPassword}
                  onToggle={() => setShowResetPassword((v) => !v)}
                />
                <PasswordField
                  labelText="Confirm new password"
                  placeholder="Repeat your new password"
                  value={passwordConfirm}
                  onChange={(e) => setPasswordConfirm(e.target.value)}
                  show={showResetPasswordConfirm}
                  onToggle={() => setShowResetPasswordConfirm((v) => !v)}
                />
                <button style={btn} disabled={busy} onClick={doResetPassword}>
                  {busy ? "Processing..." : "Update password"}
                </button>
                <button type="button" style={secondaryBtn} disabled={busy} onClick={() => setAuthMode("login")}>
                  Back to sign in
                </button>
              </div>
            ) : null}
          </>
        ) : (
          <div style={{ display: "grid", gap: 14 }}>
            <div>
              <label style={label}>Email</label>
              <input style={{ ...input, opacity: 0.85 }} readOnly value={pendingEmail || email} />
            </div>
            <div>
              <label style={label}>OTP code</label>
              <input style={input} placeholder="Enter the code you received" value={otpCode} onChange={(e) => setOtpCode(e.target.value)} />
            </div>
            <button style={btn} disabled={busy} onClick={doVerifyOtp}>
              {busy ? "Verifying..." : "Enter console"}
            </button>
            <button
              type="button"
              style={secondaryBtn}
              disabled={busy}
              onClick={() => {
                setOtpMode(false);
                setOtpCode("");
                setStatus("");
                setAuthMode("login");
              }}
            >
              Back
            </button>
          </div>
        )}

        {!!status && (
          <div
            style={{
              marginTop: 16,
              borderRadius: 16,
              padding: "12px 14px",
              fontSize: 14,
              background: status.toLowerCase().includes("failed") || status.toLowerCase().includes("invalid") || status.toLowerCase().includes("unable")
                ? "rgba(239,68,68,0.10)"
                : "rgba(37,99,235,0.10)",
              color: status.toLowerCase().includes("failed") || status.toLowerCase().includes("invalid") || status.toLowerCase().includes("unable")
                ? "#991b1b"
                : "#1d4ed8",
            }}
          >
            {status}
          </div>
        )}
      </div>
      </div>
    </div>
  );
}
