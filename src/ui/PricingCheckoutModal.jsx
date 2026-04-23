import React, { useEffect, useMemo, useState } from "react";
import {
  createPublicCheckout,
  getPublicPlans,
  getPublicTopups,
  getPublicUsageRates,
} from "./api.js";
import OrkioSphereMark from "./OrkioSphereMark.jsx";

const overlayStyle = {
  position: "fixed",
  inset: 0,
  zIndex: 80,
  background: "rgba(2,6,23,0.72)",
  backdropFilter: "blur(16px)",
  display: "grid",
  placeItems: "center",
  padding: 16,
};

const panelStyle = {
  width: "100%",
  maxWidth: 1140,
  borderRadius: 34,
  overflow: "hidden",
  border: "1px solid rgba(255,255,255,0.10)",
  background:
    "radial-gradient(circle at 14% 0%, rgba(34,211,238,0.14), rgba(7,12,24,0) 34%), radial-gradient(circle at 88% 0%, rgba(124,58,237,0.16), rgba(7,12,24,0) 34%), #070c18",
  boxShadow: "0 40px 120px rgba(0,0,0,0.58)",
};

const fieldStyle = {
  width: "100%",
  padding: "15px 16px",
  borderRadius: 18,
  border: "1px solid rgba(255,255,255,0.10)",
  background: "rgba(255,255,255,0.05)",
  color: "#fff",
  outline: "none",
  fontSize: 15,
};

const primaryButton = {
  width: "100%",
  border: 0,
  borderRadius: 20,
  padding: "16px 18px",
  fontWeight: 900,
  fontSize: 15,
  cursor: "pointer",
  background: "linear-gradient(135deg, #67e8f9, #38bdf8 42%, #8b5cf6)",
  color: "#020617",
  boxShadow: "0 18px 50px rgba(56,189,248,0.22)",
};

const secondaryButton = {
  width: "100%",
  borderRadius: 18,
  padding: "14px 18px",
  fontWeight: 700,
  fontSize: 15,
  cursor: "pointer",
  border: "1px solid rgba(255,255,255,0.12)",
  background: "rgba(255,255,255,0.05)",
  color: "#fff",
};

const TAB = {
  borderRadius: 999,
  padding: "10px 14px",
  fontWeight: 800,
  fontSize: 12,
  letterSpacing: "0.14em",
  textTransform: "uppercase",
  border: "1px solid rgba(255,255,255,0.08)",
};

function formatMoney(amount, currency = "USD") {
  const value = Number(amount || 0);
  try {
    return new Intl.NumberFormat(currency === "BRL" ? "pt-BR" : "en-US", {
      style: "currency",
      currency,
      maximumFractionDigits: value % 1 === 0 ? 0 : 2,
    }).format(value);
  } catch {
    return `${currency} ${value}`;
  }
}

function normalizePlans(raw) {
  const payload = raw?.data?.plans || raw?.plans || [];
  if (!Array.isArray(payload) || !payload.length) {
    return [
      {
        code: "founder_access",
        name: "Founder Access",
        billing_model: "subscription_wallet_hybrid",
        price_amount: 20,
        display_currency: "USD",
        included_credit_usd: 20,
        badge: "Best entry",
        description: "Assinatura leve com cobrança adicional por uso.",
        features: ["US$ 20 em créditos", "Wallet acumulativa", "Ideal para founders"],
      },
      {
        code: "pro_access",
        name: "Pro Access",
        billing_model: "subscription_wallet_hybrid",
        price_amount: 49,
        display_currency: "USD",
        included_credit_usd: 60,
        badge: "Power users",
        description: "Mais throughput e mais créditos para uso real.",
        features: ["US$ 60 em créditos", "Mais volume", "Melhor margem por heavy user"],
      },
      {
        code: "team_access",
        name: "Team Access",
        billing_model: "subscription_wallet_hybrid",
        price_amount: 149,
        display_currency: "USD",
        included_credit_usd: 180,
        badge: "Shared wallet",
        description: "Base mensal com pool compartilhado de créditos.",
        features: ["US$ 180 em créditos", "Pool compartilhado", "US$ 12 por seat extra"],
      },
    ];
  }
  return payload.map((item) => ({
    ...item,
    price_amount: Number(item.price_amount ?? item.price_usd ?? item.price ?? 0),
    display_currency: item.display_currency || item.currency || "USD",
    features: Array.isArray(item.features) ? item.features : [],
  }));
}

function normalizeTopups(raw) {
  const payload = raw?.data?.topups || raw?.topups || [];
  if (!Array.isArray(payload) || !payload.length) {
    return [
      { code: "topup_25", name: "Top-up 25", pay_usd: 25, credit_usd: 26, bonus_label: "+US$ 1 bonus" },
      { code: "topup_50", name: "Top-up 50", pay_usd: 50, credit_usd: 53, bonus_label: "+US$ 3 bonus" },
      { code: "topup_100", name: "Top-up 100", pay_usd: 100, credit_usd: 110, bonus_label: "+US$ 10 bonus" },
    ];
  }
  return payload;
}

function normalizeRates(raw) {
  const payload = raw?.data?.rates || raw?.rates || [];
  return Array.isArray(payload) ? payload : [];
}

function PriceCard({ item, selected, onSelect, kind = "plan" }) {
  const activeBorder = selected ? "2px solid rgba(103,232,249,0.92)" : "1px solid rgba(255,255,255,0.08)";
  return (
    <button
      type="button"
      onClick={() => onSelect(item)}
      style={{
        textAlign: "left",
        width: "100%",
        borderRadius: 26,
        border: activeBorder,
        background: selected ? "rgba(103,232,249,0.08)" : "rgba(255,255,255,0.04)",
        color: "#fff",
        padding: 20,
        cursor: "pointer",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start" }}>
        <div>
          <div style={{ fontSize: 12, fontWeight: 900, letterSpacing: "0.14em", textTransform: "uppercase", color: selected ? "#67e8f9" : "rgba(255,255,255,0.54)" }}>
            {item.badge || (kind === "topup" ? "Wallet top-up" : "Hybrid access")}
          </div>
          <div style={{ fontSize: 24, fontWeight: 900, marginTop: 8 }}>{item.name}</div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ fontSize: 24, fontWeight: 900 }}>
            {formatMoney(kind === "topup" ? item.pay_usd : item.price_amount, item.display_currency || "USD")}
          </div>
          <div style={{ fontSize: 12, color: "rgba(255,255,255,0.60)" }}>
            {kind === "topup" ? "one-time top-up" : "/month"}
          </div>
        </div>
      </div>
      <div style={{ marginTop: 14, color: "rgba(255,255,255,0.78)", lineHeight: 1.55 }}>
        {item.description || `${item.included_credit_usd ? `US$ ${item.included_credit_usd} em créditos inclusos.` : ""}`}
      </div>
      {kind === "plan" ? (
        <div style={{ marginTop: 14, display: "grid", gap: 8 }}>
          <div style={{ display: "inline-flex", width: "fit-content", borderRadius: 999, padding: "8px 12px", background: "rgba(255,255,255,0.06)", fontSize: 12, fontWeight: 800 }}>
            Includes {formatMoney(item.included_credit_usd || 0, "USD")} in wallet credits
          </div>
          {item.features?.map((feature) => (
            <div key={feature} style={{ fontSize: 14, color: "rgba(255,255,255,0.82)" }}>• {feature}</div>
          ))}
        </div>
      ) : (
        <div style={{ marginTop: 14, display: "grid", gap: 8 }}>
          <div style={{ display: "inline-flex", width: "fit-content", borderRadius: 999, padding: "8px 12px", background: "rgba(255,255,255,0.06)", fontSize: 12, fontWeight: 800 }}>
            Credit delivered: {formatMoney(item.credit_usd || 0, "USD")}
          </div>
          <div style={{ fontSize: 14, color: "rgba(255,255,255,0.82)" }}>
            {item.bonus_label || "Wallet recharge for extra usage"}
          </div>
        </div>
      )}
    </button>
  );
}

export default function PricingCheckoutModal({ open, onClose, defaultPlanCode = null }) {
  const [mode, setMode] = useState("plans");
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState("");
  const [plans, setPlans] = useState([]);
  const [topups, setTopups] = useState([]);
  const [rates, setRates] = useState([]);
  const [selectedPlan, setSelectedPlan] = useState(null);
  const [selectedTopup, setSelectedTopup] = useState(null);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [company, setCompany] = useState("");

  useEffect(() => {
    if (!open) return;
    let active = true;
    Promise.all([getPublicPlans(), getPublicTopups(), getPublicUsageRates()])
      .then(([plansRes, topupRes, ratesRes]) => {
        if (!active) return;
        const normalizedPlans = normalizePlans(plansRes);
        const normalizedTopups = normalizeTopups(topupRes);
        setPlans(normalizedPlans);
        setTopups(normalizedTopups);
        setRates(normalizeRates(ratesRes));
        const preferredPlan =
          normalizedPlans.find((p) => p.code === defaultPlanCode) ||
          normalizedPlans[0] ||
          null;
        setSelectedPlan(preferredPlan);
        setSelectedTopup(normalizedTopups[0] || null);
      })
      .catch(() => {
        const normalizedPlans = normalizePlans({});
        const normalizedTopups = normalizeTopups({});
        setPlans(normalizedPlans);
        setTopups(normalizedTopups);
        setSelectedPlan(normalizedPlans[0] || null);
        setSelectedTopup(normalizedTopups[0] || null);
      });
    return () => {
      active = false;
    };
  }, [open, defaultPlanCode]);

  useEffect(() => {
    if (defaultPlanCode && plans.length) {
      const p = plans.find((item) => item.code === defaultPlanCode);
      if (p) setSelectedPlan(p);
    }
  }, [defaultPlanCode, plans]);

  const currentItem = mode === "plans" ? selectedPlan : selectedTopup;

  const estimatedUsage = useMemo(() => {
    return rates.slice(0, 5);
  }, [rates]);

  if (!open) return null;

  async function handleCheckout() {
    const normalizedName = String(name || "").trim();
    const normalizedEmail = String(email || "").trim();
    const normalizedCompany = String(company || "").trim();

    if (!normalizedName || !normalizedEmail || !currentItem?.code) {
      setStatus("Informe nome, e-mail e selecione uma opção.");
      return;
    }

    setBusy(true);
    setStatus(mode === "plans" ? "Preparando acesso híbrido..." : "Preparando top-up seguro...");
    try {
      const response = await createPublicCheckout({
        item_code: currentItem.code,
        checkout_kind: mode === "plans" ? "plan" : "topup",
        full_name: normalizedName,
        email: normalizedEmail,
        company: normalizedCompany || undefined,
        currency: currentItem.display_currency || "USD",
      });

      const data = response?.data || response;
      const checkoutUrl = data?.checkout_url || data?.url || data?.payment_url;

      localStorage.setItem(
        "orkio_pending_checkout_ctx",
        JSON.stringify({
          name: normalizedName,
          email: normalizedEmail,
          company: normalizedCompany,
          selectedItem: currentItem.code,
          checkoutKind: mode === "plans" ? "plan" : "topup",
          tenant: "public",
        })
      );

      if (!checkoutUrl) {
        throw new Error("Link de checkout indisponível.");
      }

      window.location.href = checkoutUrl;
    } catch (error) {
      setStatus(error?.message || "Não foi possível abrir o checkout agora.");
      setBusy(false);
    }
  }

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={panelStyle} onClick={(ev) => ev.stopPropagation()}>
        <div style={{ display: "grid", gridTemplateColumns: "1.15fr 0.85fr", gap: 0 }}>
          <div style={{ padding: 28, borderRight: "1px solid rgba(255,255,255,0.08)" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
              <OrkioSphereMark size={54} />
              <div>
                <div style={{ fontSize: 12, fontWeight: 900, letterSpacing: "0.16em", textTransform: "uppercase", color: "rgba(103,232,249,0.78)" }}>
                  Usage-based billing
                </div>
                <div style={{ fontSize: 30, fontWeight: 900, color: "#fff", marginTop: 6 }}>
                  Subscription + wallet
                </div>
              </div>
            </div>

            <div style={{ display: "flex", gap: 10, marginTop: 22 }}>
              <button
                type="button"
                onClick={() => setMode("plans")}
                style={{
                  ...TAB,
                  background: mode === "plans" ? "rgba(103,232,249,0.14)" : "rgba(255,255,255,0.04)",
                  color: mode === "plans" ? "#67e8f9" : "rgba(255,255,255,0.72)",
                }}
              >
                Access plans
              </button>
              <button
                type="button"
                onClick={() => setMode("topups")}
                style={{
                  ...TAB,
                  background: mode === "topups" ? "rgba(103,232,249,0.14)" : "rgba(255,255,255,0.04)",
                  color: mode === "topups" ? "#67e8f9" : "rgba(255,255,255,0.72)",
                }}
              >
                Wallet top-ups
              </button>
            </div>

            <div style={{ marginTop: 22, display: "grid", gap: 14 }}>
              {(mode === "plans" ? plans : topups).map((item) => (
                <PriceCard
                  key={item.code}
                  item={item}
                  kind={mode === "plans" ? "plan" : "topup"}
                  selected={(mode === "plans" ? selectedPlan?.code : selectedTopup?.code) === item.code}
                  onSelect={mode === "plans" ? setSelectedPlan : setSelectedTopup}
                />
              ))}
            </div>

            <div style={{ marginTop: 22, borderRadius: 24, border: "1px solid rgba(255,255,255,0.08)", background: "rgba(255,255,255,0.04)", padding: 18 }}>
              <div style={{ fontSize: 12, fontWeight: 900, letterSpacing: "0.14em", textTransform: "uppercase", color: "rgba(255,255,255,0.58)" }}>
                Example action pricing
              </div>
              <div style={{ display: "grid", gap: 10, marginTop: 14 }}>
                {estimatedUsage.map((rate) => (
                  <div key={rate.action_key} style={{ display: "flex", justifyContent: "space-between", gap: 12, color: "#fff" }}>
                    <div>
                      <div style={{ fontWeight: 700 }}>{rate.label || rate.action_key}</div>
                      <div style={{ color: "rgba(255,255,255,0.56)", fontSize: 13 }}>{rate.description}</div>
                    </div>
                    <div style={{ fontWeight: 800 }}>{formatMoney(rate.price_usd, "USD")}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div style={{ padding: 28, display: "grid", alignContent: "start", gap: 18 }}>
            <div style={{ display: "grid", gap: 8 }}>
              <div style={{ fontSize: 12, fontWeight: 900, letterSpacing: "0.14em", textTransform: "uppercase", color: "rgba(255,255,255,0.58)" }}>
                {mode === "plans" ? "Selected access" : "Selected top-up"}
              </div>
              <div style={{ fontSize: 28, fontWeight: 900, color: "#fff" }}>
                {currentItem?.name || "Choose an option"}
              </div>
              <div style={{ color: "rgba(255,255,255,0.76)", lineHeight: 1.6 }}>
                {mode === "plans"
                  ? "Entry subscription gives you a monthly wallet. Heavy usage is billed by consumption, not by forcing everyone into oversized plans."
                  : "Top-ups extend your wallet immediately for extra usage without changing your base subscription."}
              </div>
            </div>

            <div style={{ display: "grid", gap: 12 }}>
              <input style={fieldStyle} value={name} onChange={(e) => setName(e.target.value)} placeholder="Seu nome" />
              <input style={fieldStyle} value={email} onChange={(e) => setEmail(e.target.value)} placeholder="Seu e-mail" type="email" />
              <input style={fieldStyle} value={company} onChange={(e) => setCompany(e.target.value)} placeholder="Empresa (opcional)" />
            </div>

            <div style={{ borderRadius: 24, border: "1px solid rgba(255,255,255,0.08)", background: "rgba(255,255,255,0.04)", padding: 18, color: "#fff" }}>
              {mode === "plans" ? (
                <>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                    <div style={{ color: "rgba(255,255,255,0.56)" }}>Subscription</div>
                    <div style={{ fontWeight: 800 }}>{formatMoney(currentItem?.price_amount || 0, currentItem?.display_currency || "USD")}/month</div>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginTop: 10 }}>
                    <div style={{ color: "rgba(255,255,255,0.56)" }}>Included wallet credits</div>
                    <div style={{ fontWeight: 800 }}>{formatMoney(currentItem?.included_credit_usd || 0, "USD")}</div>
                  </div>
                  {currentItem?.seat_price_usd ? (
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginTop: 10 }}>
                      <div style={{ color: "rgba(255,255,255,0.56)" }}>Seat expansion</div>
                      <div style={{ fontWeight: 800 }}>{formatMoney(currentItem?.seat_price_usd || 0, "USD")} / seat</div>
                    </div>
                  ) : null}
                </>
              ) : (
                <>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                    <div style={{ color: "rgba(255,255,255,0.56)" }}>Top-up payment</div>
                    <div style={{ fontWeight: 800 }}>{formatMoney(currentItem?.pay_usd || 0, "USD")}</div>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginTop: 10 }}>
                    <div style={{ color: "rgba(255,255,255,0.56)" }}>Wallet credits delivered</div>
                    <div style={{ fontWeight: 800 }}>{formatMoney(currentItem?.credit_usd || 0, "USD")}</div>
                  </div>
                  <div style={{ marginTop: 10, color: "rgba(255,255,255,0.68)" }}>{currentItem?.bonus_label}</div>
                </>
              )}
            </div>

            {status ? <div style={{ color: "rgba(255,255,255,0.78)", fontSize: 14 }}>{status}</div> : null}

            <button type="button" style={primaryButton} disabled={busy} onClick={handleCheckout}>
              {busy ? "Abrindo checkout..." : mode === "plans" ? "Start hybrid access" : "Recharge wallet"}
            </button>
            <button type="button" style={secondaryButton} disabled={busy} onClick={onClose}>
              Voltar
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
