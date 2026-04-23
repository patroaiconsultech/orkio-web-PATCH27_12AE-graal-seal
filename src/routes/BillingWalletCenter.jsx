import React, { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { getToken, getTenant, getUser } from "../lib/auth.js";
import { createPublicCheckout, getPublicTopups, getWalletLedger, getWalletSummary, setWalletAutoRecharge } from "../ui/api.js";
import OrkioSphereMark from "../ui/OrkioSphereMark.jsx";

function fmtUsd(v) {
  const n = Number(v || 0);
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: n % 1 === 0 ? 0 : 2 }).format(n);
}
function fmtTs(v) {
  if (!v) return "-";
  try { return new Date(Number(v) * 1000).toLocaleString(); } catch { return "-"; }
}
const shell = {
  minHeight: "100vh",
  padding: 24,
  background:
    "radial-gradient(circle at 12% 0%, rgba(34,211,238,0.14) 0%, rgba(6,10,20,0) 32%), radial-gradient(circle at 88% 0%, rgba(124,58,237,0.18) 0%, rgba(6,10,20,0) 34%), linear-gradient(180deg, #060812 0%, #040712 100%)",
  color: "#fff",
};
const card = {
  borderRadius: 28,
  border: "1px solid rgba(255,255,255,0.10)",
  background: "rgba(255,255,255,0.05)",
  backdropFilter: "blur(18px)",
  padding: 22,
};

export default function BillingWalletCenter() {
  const nav = useNavigate();
  const token = getToken();
  const tenant = getTenant() || "public";
  const user = getUser();
  const [data, setData] = useState(null);
  const [ledger, setLedger] = useState([]);
  const [topups, setTopups] = useState([]);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("");
  const [autoRecharge, setAutoRecharge] = useState({ enabled: false, pack_code: "topup_25", threshold_usd: 3 });

  useEffect(() => {
    if (!token) nav("/auth");
  }, [token, nav]);

  async function load() {
    if (!token) return;
    setBusy(true);
    setMsg("");
    try {
      const [summaryRes, ledgerRes, topupsRes] = await Promise.all([
        getWalletSummary({ token, org: tenant }),
        getWalletLedger({ token, org: tenant, limit: 50 }),
        getPublicTopups({ org: tenant }),
      ]);
      const summary = summaryRes?.data || summaryRes;
      setData(summary);
      setLedger((ledgerRes?.data?.items || ledgerRes?.items || []));
      const packs = topupsRes?.data?.topups || topupsRes?.topups || [];
      setTopups(packs);
      setAutoRecharge((prev) => ({
        enabled: !!summary?.wallet?.auto_recharge_enabled,
        pack_code: summary?.wallet?.auto_recharge_pack_code || prev.pack_code,
        threshold_usd: summary?.wallet?.auto_recharge_threshold_usd || prev.threshold_usd,
      }));
    } catch (e) {
      setMsg(e?.message || "Não foi possível carregar a wallet.");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => { load(); }, [token, tenant]);

  async function handleTopup(pack) {
    try {
      setMsg("Preparando top-up...");
      const res = await createPublicCheckout({
        item_code: pack.code,
        checkout_kind: "topup",
        full_name: user?.name || user?.full_name || "Orkio user",
        email: user?.email,
        company: user?.company,
        org: tenant,
      });
      const data = res?.data || res;
      if (data?.checkout_url) {
        window.location.href = data.checkout_url;
        return;
      }
      setMsg("Checkout indisponível no momento.");
    } catch (e) {
      setMsg(e?.message || "Falha ao abrir top-up.");
    }
  }

  async function saveAutoRecharge() {
    try {
      setMsg("Salvando auto-recharge...");
      await setWalletAutoRecharge({
        enabled: !!autoRecharge.enabled,
        pack_code: autoRecharge.pack_code,
        threshold_usd: Number(autoRecharge.threshold_usd || 3),
        token,
        org: tenant,
      });
      setMsg("Auto-recharge atualizado.");
      await load();
    } catch (e) {
      setMsg(e?.message || "Falha ao salvar auto-recharge.");
    }
  }

  const wallet = data?.wallet || {};
  const activePlan = data?.active_plan;

  return (
    <div style={shell}>
      <div style={{ maxWidth: 1180, margin: "0 auto", display: "grid", gap: 18 }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
            <OrkioSphereMark size={54} />
            <div>
              <div style={{ fontSize: 12, fontWeight: 900, letterSpacing: "0.15em", textTransform: "uppercase", color: "rgba(103,232,249,0.78)" }}>
                Hybrid billing
              </div>
              <div style={{ fontSize: 30, fontWeight: 900 }}>Wallet & usage</div>
            </div>
          </div>
          <button onClick={() => nav("/app")} style={{ borderRadius: 16, padding: "12px 16px", border: "1px solid rgba(255,255,255,0.1)", background: "rgba(255,255,255,0.05)", color: "#fff", cursor: "pointer" }}>
            Voltar ao app
          </button>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 14 }}>
          <div style={card}>
            <div style={{ color: "rgba(255,255,255,0.55)", fontSize: 12, textTransform: "uppercase", letterSpacing: "0.14em" }}>Current balance</div>
            <div style={{ marginTop: 8, fontSize: 32, fontWeight: 900 }}>{fmtUsd(wallet.balance_usd || 0)}</div>
          </div>
          <div style={card}>
            <div style={{ color: "rgba(255,255,255,0.55)", fontSize: 12, textTransform: "uppercase", letterSpacing: "0.14em" }}>Lifetime credited</div>
            <div style={{ marginTop: 8, fontSize: 32, fontWeight: 900 }}>{fmtUsd(wallet.lifetime_credited_usd || 0)}</div>
          </div>
          <div style={card}>
            <div style={{ color: "rgba(255,255,255,0.55)", fontSize: 12, textTransform: "uppercase", letterSpacing: "0.14em" }}>Lifetime debited</div>
            <div style={{ marginTop: 8, fontSize: 32, fontWeight: 900 }}>{fmtUsd(wallet.lifetime_debited_usd || 0)}</div>
          </div>
          <div style={card}>
            <div style={{ color: "rgba(255,255,255,0.55)", fontSize: 12, textTransform: "uppercase", letterSpacing: "0.14em" }}>Active plan</div>
            <div style={{ marginTop: 8, fontSize: 24, fontWeight: 900 }}>{activePlan?.name || "No active plan"}</div>
            <div style={{ marginTop: 6, color: "rgba(255,255,255,0.60)" }}>{activePlan?.expires_at ? `Valid until ${fmtTs(activePlan.expires_at)}` : "Wallet-only access"}</div>
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1.1fr 0.9fr", gap: 16 }}>
          <div style={card}>
            <div style={{ fontSize: 18, fontWeight: 900 }}>Top-up wallet</div>
            <div style={{ color: "rgba(255,255,255,0.65)", marginTop: 6 }}>Recharge your credits without changing your subscription.</div>
            <div style={{ marginTop: 18, display: "grid", gap: 12 }}>
              {topups.map((pack) => (
                <div key={pack.code} style={{ borderRadius: 20, border: "1px solid rgba(255,255,255,0.08)", padding: 16, display: "flex", justifyContent: "space-between", gap: 14, alignItems: "center" }}>
                  <div>
                    <div style={{ fontWeight: 800 }}>{pack.name}</div>
                    <div style={{ color: "rgba(255,255,255,0.64)", marginTop: 4 }}>
                      Pay {fmtUsd(pack.pay_usd)} → receive {fmtUsd(pack.credit_usd)} {pack.bonus_label ? `(${pack.bonus_label})` : ""}
                    </div>
                  </div>
                  <button onClick={() => handleTopup(pack)} style={{ borderRadius: 16, padding: "12px 16px", border: 0, background: "linear-gradient(135deg, #67e8f9, #38bdf8 42%, #8b5cf6)", color: "#020617", fontWeight: 900, cursor: "pointer" }}>
                    Recharge
                  </button>
                </div>
              ))}
            </div>
          </div>

          <div style={card}>
            <div style={{ fontSize: 18, fontWeight: 900 }}>Auto-recharge</div>
            <div style={{ color: "rgba(255,255,255,0.65)", marginTop: 6 }}>Keep the wallet alive when the balance gets low.</div>
            <div style={{ display: "grid", gap: 12, marginTop: 18 }}>
              <label style={{ display: "grid", gap: 8 }}>
                <span>Enable auto-recharge</span>
                <input type="checkbox" checked={!!autoRecharge.enabled} onChange={(e) => setAutoRecharge((prev) => ({ ...prev, enabled: e.target.checked }))} />
              </label>
              <label style={{ display: "grid", gap: 8 }}>
                <span>Pack</span>
                <select value={autoRecharge.pack_code} onChange={(e) => setAutoRecharge((prev) => ({ ...prev, pack_code: e.target.value }))} style={{ borderRadius: 14, padding: "12px 14px", background: "rgba(255,255,255,0.05)", color: "#fff", border: "1px solid rgba(255,255,255,0.1)" }}>
                  {topups.map((pack) => <option key={pack.code} value={pack.code}>{pack.name}</option>)}
                </select>
              </label>
              <label style={{ display: "grid", gap: 8 }}>
                <span>Threshold</span>
                <input value={autoRecharge.threshold_usd} onChange={(e) => setAutoRecharge((prev) => ({ ...prev, threshold_usd: e.target.value }))} style={{ borderRadius: 14, padding: "12px 14px", background: "rgba(255,255,255,0.05)", color: "#fff", border: "1px solid rgba(255,255,255,0.1)" }} />
              </label>
              <button onClick={saveAutoRecharge} style={{ borderRadius: 16, padding: "13px 16px", border: "1px solid rgba(255,255,255,0.1)", background: "rgba(255,255,255,0.08)", color: "#fff", fontWeight: 800, cursor: "pointer" }}>
                Save settings
              </button>
            </div>
          </div>
        </div>

        <div style={card}>
          <div style={{ fontSize: 18, fontWeight: 900 }}>Recent wallet ledger</div>
          <div style={{ color: "rgba(255,255,255,0.65)", marginTop: 6 }}>Credits in, usage out, balance after each operation.</div>
          <div style={{ marginTop: 18, display: "grid", gap: 10 }}>
            {ledger.length ? ledger.map((item) => (
              <div key={item.id} style={{ display: "grid", gridTemplateColumns: "1.2fr .8fr .6fr .8fr", gap: 12, alignItems: "center", borderBottom: "1px solid rgba(255,255,255,0.06)", paddingBottom: 10 }}>
                <div>
                  <div style={{ fontWeight: 800 }}>{item.source}</div>
                  <div style={{ color: "rgba(255,255,255,0.56)", fontSize: 13 }}>{item.action_key || item.external_ref || "wallet event"}</div>
                </div>
                <div style={{ color: "rgba(255,255,255,0.68)" }}>{fmtTs(item.created_at)}</div>
                <div style={{ color: item.direction === "credit" ? "#67e8f9" : "#fda4af", fontWeight: 800 }}>
                  {item.direction === "credit" ? "+" : "-"}{fmtUsd(item.amount_usd)}
                </div>
                <div style={{ textAlign: "right", fontWeight: 800 }}>{fmtUsd(item.balance_after_usd)}</div>
              </div>
            )) : <div style={{ color: "rgba(255,255,255,0.58)" }}>No wallet activity yet.</div>}
          </div>
        </div>

        {msg ? <div style={{ ...card, color: "#cbd5e1" }}>{msg}</div> : null}
        {busy ? <div style={{ color: "rgba(255,255,255,0.64)" }}>Loading wallet...</div> : null}
      </div>
    </div>
  );
}
