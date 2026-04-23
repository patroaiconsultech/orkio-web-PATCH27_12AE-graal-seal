
import React, { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { apiFetch } from "../ui/api.js";
import { getTenant, getToken, getUser, isAdmin } from "../lib/auth.js";

const CARD = "rounded-3xl border border-white/10 bg-white/5 backdrop-blur p-5";
const INPUT = "w-full rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-sm text-white outline-none placeholder:text-white/35";
const BTN = "rounded-2xl px-4 py-3 text-sm font-semibold";
const SMALL = "rounded-xl px-3 py-2 text-xs font-semibold";

function fmtUsd(v) {
  const n = Number(v || 0);
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(n);
}

function fmtPct(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
  return `${Number(v).toFixed(1)}%`;
}

function fmtTs(v) {
  if (!v) return "-";
  try { return new Date(Number(v) * 1000).toLocaleString(); } catch { return "-"; }
}

function Stat({ label, value, tone = "default" }) {
  const toneCls =
    tone === "warn"
      ? "border-amber-400/20 bg-amber-400/10 text-amber-100"
      : tone === "good"
      ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-100"
      : "border-white/10 bg-white/5 text-white";
  return (
    <div className={`rounded-2xl border p-4 ${toneCls}`}>
      <div className="text-xs uppercase tracking-[0.18em] text-white/50">{label}</div>
      <div className="mt-2 text-2xl font-black">{value}</div>
    </div>
  );
}

function SectionTitle({ title, subtitle, right }) {
  return (
    <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
      <div>
        <h2 className="text-xl font-black tracking-tight">{title}</h2>
        {subtitle ? <p className="mt-1 text-sm text-white/65">{subtitle}</p> : null}
      </div>
      {right}
    </div>
  );
}

export default function AdminValuationCenter() {
  const nav = useNavigate();
  const tenant = getTenant() || "public";
  const token = getToken();
  const user = getUser();

  const [days, setDays] = useState(30);
  const [data, setData] = useState(null);
  const [form, setForm] = useState(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [note, setNote] = useState("");
  const [billingItems, setBillingItems] = useState([]);
  const [billingBusy, setBillingBusy] = useState(false);
  const [billingForm, setBillingForm] = useState({ payer_email: "", plan_code: "individual", charge_kind: "recurring", amount_usd: "20", normalized_mrr_usd: "20", status: "confirmed", provider: "manual", notes: "" });

  useEffect(() => {
    if (!token) nav("/auth");
    else if (!isAdmin(user)) nav("/app");
  }, [token, user, nav]);

  async function load() {
    setBusy(true);
    setErr("");
    try {
      const [valRes, txRes] = await Promise.all([
        apiFetch(`/api/admin/valuation?days=${encodeURIComponent(days)}`, { org: tenant, token }),
        apiFetch(`/api/admin/billing/transactions?limit=12`, { org: tenant, token }),
      ]);
      const payload = valRes?.data ?? valRes;
      const txPayload = txRes?.data ?? txRes;
      setData(payload);
      setForm(payload?.config || null);
      setBillingItems(Array.isArray(txPayload?.items) ? txPayload.items : []);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    if (token) load();
  }, [days, token]);

  async function save() {
    setBusy(true);
    setErr("");
    setNote("");
    try {
      const payload = { ...form };
      Object.keys(payload).forEach((k) => {
        if (payload[k] === "") payload[k] = null;
      });
      const res = await apiFetch("/api/admin/valuation/config", {
        method: "PUT",
        org: tenant,
        token,
        body: payload,
      });
      setForm(res?.config || payload);
      setNote("Assumptions updated.");
      await load();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }


  async function createBillingTransaction() {
    setBillingBusy(true);
    setErr("");
    setNote("");
    try {
      const body = {
        ...billingForm,
        amount_usd: Number(billingForm.amount_usd || 0),
        normalized_mrr_usd: billingForm.normalized_mrr_usd === "" ? null : Number(billingForm.normalized_mrr_usd || 0),
      };
      await apiFetch("/api/admin/billing/transactions", {
        method: "POST",
        org: tenant,
        token,
        body,
      });
      setBillingForm({ payer_email: "", plan_code: "individual", charge_kind: "recurring", amount_usd: "20", normalized_mrr_usd: "20", status: "confirmed", provider: "manual", notes: "" });
      setNote("Billing event recorded.");
      await load();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBillingBusy(false);
    }
  }

  const current = data?.current;
  const modeledCurrent = data?.modeled_current;
  const billingCurrent = data?.billing_current;
  const billing = data?.billing || {};
  const scenarios = data?.scenarios || {};
  const actuals = data?.actuals || {};
  const mixTotal = useMemo(() => {
    if (!form) return 0;
    return Number(form.individual_share_pct || 0) + Number(form.pro_share_pct || 0) + Number(form.team_share_pct || 0);
  }, [form]);

  return (
    <div className="min-h-screen bg-[#050816] text-white">
      <div className="mx-auto max-w-7xl px-4 py-8 md:px-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="text-xs uppercase tracking-[0.24em] text-cyan-300/80">ORKIO · Valuation Center</div>
            <h1 className="mt-2 text-3xl font-black tracking-tight">Realtime valuation cockpit</h1>
            <p className="mt-2 max-w-3xl text-sm text-white/65">
              Live enterprise valuation model linked to current users, leads and AI cost run-rate. Multiples and pricing mix remain under your control.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <button onClick={() => nav("/admin")} className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-semibold hover:bg-white/10">
              ← Admin
            </button>
            <button onClick={load} className="rounded-2xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-3 text-sm font-semibold text-cyan-100 hover:bg-cyan-400/20">
              Refresh
            </button>
          </div>
        </div>

        {err ? <div className="mt-5 rounded-2xl border border-rose-400/20 bg-rose-400/10 p-4 text-sm text-rose-100">⚠️ {err}</div> : null}
        {note ? <div className="mt-5 rounded-2xl border border-emerald-400/20 bg-emerald-400/10 p-4 text-sm text-emerald-100">✓ {note}</div> : null}

        <div className="mt-6 grid gap-4 md:grid-cols-8">
          <Stat label="Approved users" value={actuals.approved_users ?? "-"} />
          <Stat label="Total users" value={actuals.total_users ?? "-"} />
          <Stat label="Leads 30d" value={actuals.leads_window ?? "-"} />
          <Stat label="Messages window" value={actuals.messages_window ?? "-"} />
          <Stat label="AI cost / 30d" value={fmtUsd(actuals.monthly_ai_cost_usd)} tone="warn" />
          <Stat label="Confirmed rev / 30d" value={fmtUsd(actuals.billing_confirmed_30d_usd)} tone="good" />
          <Stat label="Current billing MRR" value={fmtUsd(actuals.billing_current_mrr_usd)} tone={Number(actuals.billing_current_mrr_usd || 0) > 0 ? "good" : "default"} />
          <Stat label="Window" value={`${data?.window_days || days}d`} />
        </div>

        <div className="mt-8 grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
          <div className={CARD}>
            <SectionTitle
              title="Current valuation"
              subtitle="Calculated from your active assumptions and current paid-user base."
              right={
                <div className="flex items-center gap-2">
                  <span className="text-xs text-white/50">Window</span>
                  <select className={INPUT + " max-w-[110px] py-2"} value={days} onChange={(e) => setDays(Number(e.target.value))}>
                    <option value={7}>7 days</option>
                    <option value={30}>30 days</option>
                    <option value={90}>90 days</option>
                    <option value={180}>180 days</option>
                  </select>
                </div>
              }
            />

            <div className="mt-5 grid gap-4 md:grid-cols-3">
              <Stat label="MRR" value={fmtUsd(current?.mrr?.total_usd)} tone="good" />
              <Stat label="ARR" value={fmtUsd(current?.arr?.total_usd)} tone="good" />
              <Stat label="ARPPU" value={fmtUsd(current?.unit_economics?.arppu_usd)} />
            </div>

            <div className="mt-4 grid gap-4 md:grid-cols-3">
              <Stat label="Valuation · low" value={fmtUsd(current?.valuation?.low_usd)} />
              <Stat label="Valuation · base" value={fmtUsd(current?.valuation?.base_usd)} tone="good" />
              <Stat label="Valuation · high" value={fmtUsd(current?.valuation?.high_usd)} />
            </div>

            <div className="mt-5 grid gap-4 md:grid-cols-4">
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Paid users in model</div>
                <div className="mt-2 text-2xl font-black">{current?.paid_users ?? 0}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Gross margin</div>
                <div className="mt-2 text-2xl font-black">{fmtPct(current?.unit_economics?.gross_margin_pct)}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">AI cost ratio</div>
                <div className="mt-2 text-2xl font-black">{fmtPct(current?.unit_economics?.cost_ratio_pct)}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Team accounts</div>
                <div className="mt-2 text-2xl font-black">{current?.composition?.team_accounts ?? 0}</div>
              </div>
            </div>

            <div className="mt-5 grid gap-4 md:grid-cols-5">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs text-white/45">Individual users</div>
                <div className="mt-1 text-xl font-black">{current?.composition?.individual_users ?? 0}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs text-white/45">Pro users</div>
                <div className="mt-1 text-xl font-black">{current?.composition?.pro_users ?? 0}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs text-white/45">Team seats</div>
                <div className="mt-1 text-xl font-black">{current?.composition?.team_seats ?? 0}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs text-white/45">Setup MRR</div>
                <div className="mt-1 text-xl font-black">{fmtUsd(current?.mrr?.setup_usd)}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs text-white/45">Enterprise MRR</div>
                <div className="mt-1 text-xl font-black">{fmtUsd(current?.mrr?.enterprise_usd)}</div>
              </div>
            </div>
          </div>

          <div className={CARD}>
            <SectionTitle title="Assumptions" subtitle="Saved per tenant and used by the live model." />
            <div className="mt-5 grid gap-3 md:grid-cols-2">
              {form ? (
                <>
                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Paid users override</div>
                    <input className={INPUT} value={form.paid_users_override ?? ""} onChange={(e) => setForm({ ...form, paid_users_override: e.target.value })} placeholder="default = approved users" />
                  </label>
                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Average team size</div>
                    <input className={INPUT} value={form.avg_team_size ?? ""} onChange={(e) => setForm({ ...form, avg_team_size: e.target.value })} />
                  </label>

                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Individual price (USD)</div>
                    <input className={INPUT} value={form.individual_price_usd ?? ""} onChange={(e) => setForm({ ...form, individual_price_usd: e.target.value })} />
                  </label>
                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Pro price (USD)</div>
                    <input className={INPUT} value={form.pro_price_usd ?? ""} onChange={(e) => setForm({ ...form, pro_price_usd: e.target.value })} />
                  </label>
                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Team base (USD)</div>
                    <input className={INPUT} value={form.team_base_price_usd ?? ""} onChange={(e) => setForm({ ...form, team_base_price_usd: e.target.value })} />
                  </label>
                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Team seat (USD)</div>
                    <input className={INPUT} value={form.team_seat_price_usd ?? ""} onChange={(e) => setForm({ ...form, team_seat_price_usd: e.target.value })} />
                  </label>

                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Individual share %</div>
                    <input className={INPUT} value={form.individual_share_pct ?? ""} onChange={(e) => setForm({ ...form, individual_share_pct: e.target.value })} />
                  </label>
                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Pro share %</div>
                    <input className={INPUT} value={form.pro_share_pct ?? ""} onChange={(e) => setForm({ ...form, pro_share_pct: e.target.value })} />
                  </label>
                  <label className="block md:col-span-2">
                    <div className="mb-2 text-xs text-white/55">Team share %</div>
                    <input className={INPUT} value={form.team_share_pct ?? ""} onChange={(e) => setForm({ ...form, team_share_pct: e.target.value })} />
                  </label>

                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Setup MRR (USD)</div>
                    <input className={INPUT} value={form.monthly_setup_revenue_usd ?? ""} onChange={(e) => setForm({ ...form, monthly_setup_revenue_usd: e.target.value })} />
                  </label>
                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Enterprise MRR (USD)</div>
                    <input className={INPUT} value={form.monthly_enterprise_mrr_usd ?? ""} onChange={(e) => setForm({ ...form, monthly_enterprise_mrr_usd: e.target.value })} />
                  </label>

                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Low ARR multiple</div>
                    <input className={INPUT} value={form.low_arr_multiple ?? ""} onChange={(e) => setForm({ ...form, low_arr_multiple: e.target.value })} />
                  </label>
                  <label className="block">
                    <div className="mb-2 text-xs text-white/55">Base ARR multiple</div>
                    <input className={INPUT} value={form.base_arr_multiple ?? ""} onChange={(e) => setForm({ ...form, base_arr_multiple: e.target.value })} />
                  </label>
                  <label className="block md:col-span-2">
                    <div className="mb-2 text-xs text-white/55">High ARR multiple</div>
                    <input className={INPUT} value={form.high_arr_multiple ?? ""} onChange={(e) => setForm({ ...form, high_arr_multiple: e.target.value })} />
                  </label>

                  <label className="block md:col-span-2">
                    <div className="mb-2 text-xs text-white/55">Notes</div>
                    <textarea className={INPUT + " min-h-[110px]"} value={form.notes ?? ""} onChange={(e) => setForm({ ...form, notes: e.target.value })} />
                  </label>
                </>
              ) : null}
            </div>

            <div className="mt-4 flex flex-wrap items-center gap-2">
              <span className={`inline-flex rounded-full border px-3 py-1 text-xs ${Math.abs(mixTotal - 100) < 0.01 ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-100" : "border-amber-400/20 bg-amber-400/10 text-amber-100"}`}>
                Mix total: {mixTotal.toFixed(1)}%
              </span>
              <span className="inline-flex rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white/70">
                Default paid users = approved users when override is empty
              </span>
            </div>

            <div className="mt-5 flex gap-3">
              <button disabled={busy || !form} onClick={save} className={`${BTN} bg-cyan-400 text-black disabled:opacity-50`}>
                {busy ? "Saving..." : "Save assumptions"}
              </button>
              <button disabled={busy} onClick={load} className={`${BTN} border border-white/10 bg-white/5 text-white disabled:opacity-50`}>
                Reset
              </button>
            </div>
          </div>
        </div>


        <div className="mt-8 grid gap-6 xl:grid-cols-[1fr_1fr]">
          <div className={CARD}>
            <SectionTitle title="Billing-backed signal" subtitle="Confirmed revenue, normalized MRR and valuation from real billing events." />
            <div className="mt-5 grid gap-4 md:grid-cols-3">
              <Stat label="Actual MRR" value={fmtUsd(billingCurrent?.mrr?.total_usd)} tone={Number(billingCurrent?.mrr?.total_usd || 0) > 0 ? "good" : "default"} />
              <Stat label="Actual ARR" value={fmtUsd(billingCurrent?.arr?.total_usd)} tone={Number(billingCurrent?.arr?.total_usd || 0) > 0 ? "good" : "default"} />
              <Stat label="Billing base valuation" value={fmtUsd(billingCurrent?.valuation?.base_usd)} tone={Number(billingCurrent?.valuation?.base_usd || 0) > 0 ? "good" : "default"} />
            </div>
            <div className="mt-4 grid gap-4 md:grid-cols-4">
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Confirmed revenue · 30d</div>
                <div className="mt-2 text-2xl font-black">{fmtUsd(billing.confirmed_revenue_30d_usd)}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Recurring cash · 30d</div>
                <div className="mt-2 text-2xl font-black">{fmtUsd(billing.recurring_revenue_30d_usd)}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Setup cash · 30d</div>
                <div className="mt-2 text-2xl font-black">{fmtUsd(billing.setup_revenue_30d_usd)}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Active subscriptions</div>
                <div className="mt-2 text-2xl font-black">{billing.active_subscription_count ?? 0}</div>
              </div>
            </div>
            <div className="mt-4 rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-white/70">
              Current basis: <span className="font-bold text-white">{data?.current_basis === "billing_actuals" ? "billing actuals" : "modeled assumptions"}</span><br />
              Last confirmed payment: <span className="font-bold text-white">{fmtTs(billing.last_payment_at)}</span><br />
              Paid accounts estimate: <span className="font-bold text-white">{billing.paid_accounts_estimate ?? 0}</span>
            </div>
          </div>

          <div className={CARD}>
            <SectionTitle title="Record billing event" subtitle="Use this until provider webhooks are fully wired. The valuation engine reads these confirmed events immediately." />
            <div className="mt-5 grid gap-3 md:grid-cols-2">
              <label className="block">
                <div className="mb-2 text-xs text-white/55">Payer email</div>
                <input className={INPUT} value={billingForm.payer_email} onChange={(e) => setBillingForm({ ...billingForm, payer_email: e.target.value })} placeholder="founder@company.com" />
              </label>
              <label className="block">
                <div className="mb-2 text-xs text-white/55">Plan code</div>
                <input className={INPUT} value={billingForm.plan_code} onChange={(e) => setBillingForm({ ...billingForm, plan_code: e.target.value })} placeholder="individual / pro / team" />
              </label>
              <label className="block">
                <div className="mb-2 text-xs text-white/55">Charge kind</div>
                <select className={INPUT} value={billingForm.charge_kind} onChange={(e) => setBillingForm({ ...billingForm, charge_kind: e.target.value })}>
                  <option value="recurring">Recurring</option>
                  <option value="setup">Setup</option>
                  <option value="enterprise">Enterprise</option>
                  <option value="addon">Add-on</option>
                  <option value="refund">Refund</option>
                </select>
              </label>
              <label className="block">
                <div className="mb-2 text-xs text-white/55">Provider</div>
                <input className={INPUT} value={billingForm.provider} onChange={(e) => setBillingForm({ ...billingForm, provider: e.target.value })} placeholder="manual / asaas / stripe" />
              </label>
              <label className="block">
                <div className="mb-2 text-xs text-white/55">Amount USD</div>
                <input className={INPUT} value={billingForm.amount_usd} onChange={(e) => setBillingForm({ ...billingForm, amount_usd: e.target.value })} />
              </label>
              <label className="block">
                <div className="mb-2 text-xs text-white/55">Normalized MRR USD</div>
                <input className={INPUT} value={billingForm.normalized_mrr_usd} onChange={(e) => setBillingForm({ ...billingForm, normalized_mrr_usd: e.target.value })} placeholder="Use 0 for setup / one-off" />
              </label>
              <label className="block md:col-span-2">
                <div className="mb-2 text-xs text-white/55">Notes</div>
                <textarea className={INPUT + " min-h-[96px]"} value={billingForm.notes} onChange={(e) => setBillingForm({ ...billingForm, notes: e.target.value })} placeholder="Manual import, confirmed checkout, setup package, etc." />
              </label>
            </div>
            <div className="mt-5 flex gap-3">
              <button disabled={billingBusy} onClick={createBillingTransaction} className={`${BTN} bg-emerald-400 text-black disabled:opacity-50`}>
                {billingBusy ? "Recording..." : "Record confirmed event"}
              </button>
            </div>
            <div className="mt-5 space-y-3">
              {billingItems.map((item) => (
                <div key={item.id} className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                    <div>
                      <div className="text-sm font-bold">{item.plan_code || item.charge_kind} · {item.payer_email || "no email"}</div>
                      <div className="text-xs text-white/50">{item.provider} · {item.status} · {fmtTs(item.confirmed_at || item.occurred_at || item.created_at)}</div>
                    </div>
                    <div className="text-right">
                      <div className="text-lg font-black">{fmtUsd(item.amount_usd)}</div>
                      <div className="text-xs text-white/50">MRR {fmtUsd(item.normalized_mrr_usd)}</div>
                    </div>
                  </div>
                </div>
              ))}
              {!billingItems.length ? <div className="rounded-2xl border border-dashed border-white/10 p-4 text-sm text-white/50">No billing events yet.</div> : null}
            </div>
          </div>
        </div>

        <div className="mt-8 grid gap-6 xl:grid-cols-[1fr_1fr]">
          <div className={CARD}>
            <SectionTitle title="Scenario ladder" subtitle="Same valuation model applied to 100, 1,000 and 10,000 paid users." />
            <div className="mt-5 space-y-4">
              {Object.entries(scenarios).map(([key, sc]) => (
                <div key={key} className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                    <div>
                      <div className="text-sm font-bold">{key} paid users</div>
                      <div className="text-xs text-white/50">
                        MRR {fmtUsd(sc?.mrr?.total_usd)} · ARR {fmtUsd(sc?.arr?.total_usd)} · ARPPU {fmtUsd(sc?.unit_economics?.arppu_usd)}
                      </div>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <span className="inline-flex rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white/80">Low {fmtUsd(sc?.valuation?.low_usd)}</span>
                      <span className="inline-flex rounded-full border border-emerald-400/20 bg-emerald-400/10 px-3 py-1 text-xs text-emerald-100">Base {fmtUsd(sc?.valuation?.base_usd)}</span>
                      <span className="inline-flex rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white/80">High {fmtUsd(sc?.valuation?.high_usd)}</span>
                    </div>
                  </div>
                  <div className="mt-3 grid gap-3 md:grid-cols-4">
                    <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-white/45">Individual</div>
                      <div className="mt-1 text-lg font-black">{sc?.composition?.individual_users ?? 0}</div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-white/45">Pro</div>
                      <div className="mt-1 text-lg font-black">{sc?.composition?.pro_users ?? 0}</div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-white/45">Team seats</div>
                      <div className="mt-1 text-lg font-black">{sc?.composition?.team_seats ?? 0}</div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-white/45">Team accounts</div>
                      <div className="mt-1 text-lg font-black">{sc?.composition?.team_accounts ?? 0}</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className={CARD}>
            <SectionTitle title="Realtime rationale" subtitle="What the valuation model is reading from the platform right now." />
            <div className="mt-5 grid gap-4 md:grid-cols-2">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Live user base</div>
                <div className="mt-3 text-sm text-white/70">
                  Approved users: <span className="font-bold text-white">{actuals.approved_users ?? 0}</span><br />
                  Total users: <span className="font-bold text-white">{actuals.total_users ?? 0}</span>
                </div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Demand signal</div>
                <div className="mt-3 text-sm text-white/70">
                  Leads in window: <span className="font-bold text-white">{actuals.leads_window ?? 0}</span><br />
                  Leads total: <span className="font-bold text-white">{actuals.leads_total ?? 0}</span>
                </div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Usage signal</div>
                <div className="mt-3 text-sm text-white/70">
                  Threads in window: <span className="font-bold text-white">{actuals.threads_window ?? 0}</span><br />
                  Messages in window: <span className="font-bold text-white">{actuals.messages_window ?? 0}</span>
                </div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Cost signal</div>
                <div className="mt-3 text-sm text-white/70">
                  Monthly AI cost run-rate: <span className="font-bold text-white">{fmtUsd(actuals.monthly_ai_cost_usd)}</span><br />
                  Margin impact on current MRR: <span className="font-bold text-white">{fmtPct(current?.unit_economics?.gross_margin_pct)}</span><br />
                  Billing-backed MRR: <span className="font-bold text-white">{fmtUsd(billingCurrent?.mrr?.total_usd)}</span>
                </div>
              </div>
            </div>

            <div className="mt-5 rounded-2xl border border-cyan-400/20 bg-cyan-400/10 p-4 text-sm text-cyan-100">
              This dashboard is intentionally founder-controlled. It shows a live valuation model, not a market quote. Multiples, pricing mix and paid-user assumptions stay under your authorization.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
