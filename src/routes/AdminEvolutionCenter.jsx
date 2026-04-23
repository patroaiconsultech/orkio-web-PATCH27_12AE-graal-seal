import React, { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { apiFetch } from "../ui/api.js";
import { getTenant, getToken, getUser, isAdmin } from "../lib/auth.js";

const CARD = "rounded-3xl border border-white/10 bg-white/5 backdrop-blur p-5";
const INPUT = "w-full rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-sm text-white outline-none placeholder:text-white/35";
const SELECT = "rounded-2xl border border-white/10 bg-black/20 px-3 py-2 text-sm text-white outline-none";
const BTN = "rounded-2xl px-4 py-3 text-sm font-semibold";
const TEXTAREA = "w-full min-h-[120px] rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-sm text-white outline-none placeholder:text-white/35";

function fmtTs(v) {
  if (!v) return "-";
  try {
    const n = Number(v);
    return new Date((n > 10_000_000_000 ? n : n * 1000)).toLocaleString("pt-BR");
  } catch {
    return "-";
  }
}

function extractError(e) {
  const detail = e?.data?.detail;
  if (typeof detail === "string") return detail;
  if (detail && typeof detail === "object") {
    return detail.reason || detail.message || JSON.stringify(detail);
  }
  return e?.message || "Falha na operação.";
}

function toneForStatus(status) {
  const s = String(status || "").toLowerCase();
  if (["approved", "completed", "executed"].includes(s)) return "good";
  if (["failed", "rejected", "rolled_back"].includes(s)) return "bad";
  if (["running", "awaiting_master_approval"].includes(s)) return "warn";
  return "default";
}

function toneForBand(band) {
  const b = String(band || "").toLowerCase();
  if (["critical", "high"].includes(b)) return "bad";
  if (["medium"].includes(b)) return "warn";
  if (["low"].includes(b)) return "good";
  return "default";
}

function scoreTone(v) {
  const n = Number(v || 0);
  if (n >= 85) return "bad";
  if (n >= 70) return "warn";
  if (n > 0) return "good";
  return "default";
}

function recommendationLabel(value) {
  const key = String(value || "").toLowerCase();
  return {
    review_now: "Revisar agora",
    review_soon: "Revisar em seguida",
    observe_with_guard: "Observar com guarda",
    informational_only: "Informacional",
  }[key] || (value || "Sem recomendação");
}


function adminRecommendationLabel(value) {
  const key = String(value || "").toLowerCase();
  return {
    execute_candidate: "Candidato a executar",
    review_then_execute: "Revisar e executar",
    review_only: "Somente revisão",
    observe_only: "Somente observação",
    blocked_sensitive_domain: "Bloqueado por domínio sensível",
  }[key] || (value || "Sem recomendação admin");
}

function adminRecommendationTone(value) {
  const key = String(value || "").toLowerCase();
  if (key === "execute_candidate") return "good";
  if (key === "review_then_execute") return "warn";
  if (key === "blocked_sensitive_domain") return "bad";
  return "default";
}

function trendLabel(value) {
  const key = String(value || "").toLowerCase();
  return {
    rising: "Tendência em alta",
    stable: "Tendência estável",
    cooling: "Tendência em queda",
    new: "Sinal novo",
  }[key] || (value || "Sem tendência");
}

function trendTone(value) {
  const key = String(value || "").toLowerCase();
  if (key === "rising") return "bad";
  if (key === "stable") return "warn";
  if (key === "cooling") return "good";
  return "default";
}

function slaLabel(value) {
  const key = String(value || "").toLowerCase();
  return {
    breached: "SLA vencido",
    due_soon: "SLA próximo",
    on_track: "Dentro do SLA",
  }[key] || (value || "SLA");
}

function slaTone(value) {
  const key = String(value || "").toLowerCase();
  if (key === "breached") return "bad";
  if (key === "due_soon") return "warn";
  return "good";
}

function ageBandLabel(value) {
  const key = String(value || "").toLowerCase();
  return {
    fresh: "Recente",
    warm: "Aquecendo",
    stale: "Envelhecida",
    aged_out: "Muito envelhecida",
  }[key] || (value || "Idade");
}

function ageBandTone(value) {
  const key = String(value || "").toLowerCase();
  if (key === "aged_out") return "bad";
  if (key === "stale") return "warn";
  if (key === "warm") return "default";
  return "good";
}

function backlogPressureLabel(value) {
  const key = String(value || "").toLowerCase();
  return {
    critical: "Pressão crítica",
    high: "Pressão alta",
    medium: "Pressão média",
    low: "Pressão baixa",
  }[key] || (value || "Pressão");
}

function capacityHintLabel(value) {
  const key = String(value || "").toLowerCase();
  return {
    burst_now: "Burst agora",
    clear_next: "Limpar em seguida",
    steady_review: "Revisão contínua",
    defer_observe: "Adiar e observar",
  }[key] || (value || "Capacidade");
}


function durationLabel(seconds) {
  const n = Number(seconds || 0);
  if (!n) return "-";
  if (n < 3600) return `${Math.round(n / 60)} min`;
  if (n < 86400) return `${Math.round((n / 3600) * 10) / 10} h`;
  return `${Math.round((n / 86400) * 10) / 10} d`;
}

function workWindowModeLabel(value) {
  const key = String(value || "").toLowerCase();
  return {
    triage_intensive: "Triagem intensiva",
    burst_review: "Burst governado",
    steady_window: "Janela contínua",
    light_observe: "Observação leve",
  }[key] || (value || "Janela");
}

function agendaActionLabel(value) {
  const key = String(value || "").toLowerCase();
  return {
    approve_and_execute: "Aprovar + executar",
    approve_only: "Aprovar",
    hold: "Segurar",
    reject: "Rejeitar",
  }[key] || (value || "Ação");
}

function focusDomainTone(value) {
  const key = String(value || "").toLowerCase();
  if (["security", "billing", "auth"].includes(key)) return "bad";
  if (["runtime", "realtime", "schema"].includes(key)) return "warn";
  return "default";
}

function Tag({ children, tone = "default" }) {
  const toneCls =
    tone === "warn"
      ? "border-amber-400/30 bg-amber-400/10 text-amber-100"
      : tone === "good"
      ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-100"
      : tone === "bad"
      ? "border-rose-400/30 bg-rose-400/10 text-rose-100"
      : "border-white/10 bg-white/5 text-white/80";
  return <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs ${toneCls}`}>{children}</span>;
}

function Stat({ label, value, tone = "default", subtitle = "" }) {
  const toneCls =
    tone === "warn"
      ? "border-amber-400/20 bg-amber-400/10 text-amber-100"
      : tone === "good"
      ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-100"
      : tone === "bad"
      ? "border-rose-400/20 bg-rose-400/10 text-rose-100"
      : "border-white/10 bg-white/5 text-white";
  return (
    <div className={`rounded-2xl border p-4 ${toneCls}`}>
      <div className="text-xs uppercase tracking-[0.18em] text-white/50">{label}</div>
      <div className="mt-2 text-2xl font-black">{value}</div>
      {subtitle ? <div className="mt-2 text-xs text-white/60">{subtitle}</div> : null}
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

function scoreValue(item, key) {
  return Number(item?.scores?.[key] || 0);
}

function isMasterAdminUser(user) {
  if (!user) return false;
  const role = String(user.role || "").toLowerCase();
  return role === "owner" || role === "superadmin" || user.master_admin === true;
}

export default function AdminEvolutionCenter() {
  const nav = useNavigate();
  const tenant = getTenant() || "public";
  const token = getToken();
  const user = getUser();

  const [health, setHealth] = useState(null);
  const [policyOverview, setPolicyOverview] = useState(null);
  const [actionsCatalog, setActionsCatalog] = useState([]);
  const [items, setItems] = useState([]);
  const [selectedId, setSelectedId] = useState("");
  const [detail, setDetail] = useState(null);
  const [executions, setExecutions] = useState([]);
  const [signalHistory, setSignalHistory] = useState([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [actionFilter, setActionFilter] = useState("");
  const [severityFilter, setSeverityFilter] = useState("");
  const [sortMode, setSortMode] = useState("priority");
  const [queryText, setQueryText] = useState("");
  const [scanResult, setScanResult] = useState(null);
  const [decisionNote, setDecisionNote] = useState("");
  const [executionForm, setExecutionForm] = useState({
    source_branch: "",
    auto_pr: true,
    path: "app/db.py",
    validate_after_commit: true,
    override_hard_gate: false,
  });
  const [batchPlan, setBatchPlan] = useState(null);

  const selected = useMemo(() => items.find((item) => item.id === selectedId) || detail, [items, selectedId, detail]);
  const latestExecution = useMemo(() => (executions && executions.length ? executions[0] : null), [executions]);
  const topQueue = useMemo(() => (Array.isArray(health?.top_queue) ? health.top_queue : []), [health]);
  const recentCycles = useMemo(() => (Array.isArray(health?.recent_cycles) ? health.recent_cycles : []), [health]);
  const adminRecommendationBuckets = useMemo(() => (health?.admin_recommendation_buckets || {}), [health]);
  const trendBuckets = useMemo(() => (health?.trend_buckets || {}), [health]);
  const workWindow = useMemo(() => (health?.work_window || {}), [health]);
  const dailyAgenda = useMemo(() => (health?.daily_agenda || {}), [health]);
  const domainSlaPolicy = useMemo(() => (health?.domain_sla_policy || {}), [health]);

  async function loadHealth() {
    const data = await apiFetch("/api/internal/evolution/health", { token, org: tenant });
    setHealth(data);
  }

  async function loadPolicy() {
    const data = await apiFetch("/api/internal/evolution/policy", { token, org: tenant });
    setPolicyOverview(data?.policy || null);
  }

  async function loadActions() {
    const data = await apiFetch("/api/internal/evolution/actions", { token, org: tenant });
    setActionsCatalog(Array.isArray(data?.items) ? data.items : []);
  }

  async function loadProposals(nextSelectedId = null) {
    const qs = new URLSearchParams();
    qs.set("limit", "100");
    qs.set("sort", sortMode || "priority");
    if (statusFilter) qs.set("status", statusFilter);
    if (actionFilter) qs.set("action", actionFilter);
    if (severityFilter) qs.set("severity", severityFilter);
    if (queryText.trim()) qs.set("q", queryText.trim());
    const data = await apiFetch(`/api/internal/evolution/proposals?${qs.toString()}`, { token, org: tenant });
    const nextItems = Array.isArray(data?.items) ? data.items : [];
    setItems(nextItems);
    const candidateId = nextSelectedId || selectedId;
    if (candidateId && nextItems.some((item) => item.id === candidateId)) {
      setSelectedId(candidateId);
      return candidateId;
    }
    if (nextItems.length) {
      setSelectedId(nextItems[0].id);
      return nextItems[0].id;
    }
    setSelectedId("");
    setDetail(null);
    setExecutions([]);
    setSignalHistory([]);
    return "";
  }

  async function loadDetail(id) {
    if (!id) {
      setDetail(null);
      setExecutions([]);
      setSignalHistory([]);
      return;
    }
    const data = await apiFetch(`/api/internal/evolution/proposals/${id}`, { token, org: tenant });
    setDetail(data?.item || null);
    setExecutions(Array.isArray(data?.executions) ? data.executions : []);
    setSignalHistory(Array.isArray(data?.signal_history) ? data.signal_history : []);
  }

  async function refreshAll(preferredId = null) {
    setBusy(true);
    setErr("");
    try {
      await Promise.all([loadHealth(), loadPolicy(), loadActions()]);
      const resolvedId = await loadProposals(preferredId);
      await loadDetail(resolvedId);
    } catch (e) {
      setErr(extractError(e));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    if (!token || !user) {
      nav("/auth");
      return;
    }
    if (!isAdmin(user)) {
      nav("/app");
      return;
    }
    refreshAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, user, nav]);

  useEffect(() => {
    if (token && user && isAdmin(user)) {
      refreshAll(selectedId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statusFilter, actionFilter, severityFilter, sortMode]);

  useEffect(() => {
    if (!token || !user || !isAdmin(user)) return;
    const h = setTimeout(() => {
      refreshAll(selectedId);
    }, 250);
    return () => clearTimeout(h);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryText]);

  useEffect(() => {
    if (selectedId) loadDetail(selectedId).catch((e) => setErr(extractError(e)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedId]);

  async function runScanNow() {
    setBusy(true);
    setErr("");
    try {
      const data = await apiFetch("/api/internal/evolution/scan-now", {
        method: "POST",
        token,
        org: tenant,
      });
      setScanResult(data || null);
      await refreshAll(selectedId);
    } catch (e) {
      setErr(extractError(e));
    } finally {
      setBusy(false);
    }
  }

  async function decide(kind, { executeNow = false } = {}) {
    if (!selectedId) return;
    setBusy(true);
    setErr("");
    try {
      if (kind === "approve") {
        await apiFetch(`/api/internal/evolution/proposals/${selectedId}/approve`, {
          method: "POST",
          token,
          org: tenant,
          body: {
            note: decisionNote || null,
            execute_now: executeNow,
            source_branch: executionForm.source_branch || null,
            auto_pr: executionForm.auto_pr,
            path: executionForm.path || "app/db.py",
            validate_after_commit: executionForm.validate_after_commit,
            override_hard_gate: executionForm.override_hard_gate,
          },
        });
      } else if (kind === "reject") {
        await apiFetch(`/api/internal/evolution/proposals/${selectedId}/reject`, {
          method: "POST",
          token,
          org: tenant,
          body: {
            note: decisionNote || null,
          },
        });
      }
      setDecisionNote("");
      await refreshAll(selectedId);
    } catch (e) {
      setErr(extractError(e));
    } finally {
      setBusy(false);
    }
  }

  async function executeProposal() {
    if (!selectedId) return;
    setBusy(true);
    setErr("");
    try {
      await apiFetch(`/api/internal/evolution/proposals/${selectedId}/execute`, {
        method: "POST",
        token,
        org: tenant,
        body: {
          note: decisionNote || null,
          source_branch: executionForm.source_branch || null,
          auto_pr: executionForm.auto_pr,
          path: executionForm.path || "app/db.py",
          validate_after_commit: executionForm.validate_after_commit,
          override_hard_gate: executionForm.override_hard_gate,
        },
      });
      await refreshAll(selectedId);
    } catch (e) {
      setErr(extractError(e));
    } finally {
      setBusy(false);
    }
  }

  async function retryProposal() {
    if (!selectedId) return;
    setBusy(true);
    setErr("");
    try {
      await apiFetch(`/api/internal/evolution/proposals/${selectedId}/retry`, {
        method: "POST",
        token,
        org: tenant,
        body: {
          note: decisionNote || null,
          source_branch: executionForm.source_branch || null,
          auto_pr: executionForm.auto_pr,
          path: executionForm.path || "app/db.py",
          validate_after_commit: executionForm.validate_after_commit,
          override_hard_gate: executionForm.override_hard_gate,
        },
      });
      await refreshAll(selectedId);
    } catch (e) {
      setErr(extractError(e));
    } finally {
      setBusy(false);
    }
  }

  async function rollbackExecution(executionId) {
    if (!executionId) return;
    setBusy(true);
    setErr("");
    try {
      await apiFetch(`/api/internal/evolution/executions/${executionId}/rollback`, {
        method: "POST",
        token,
        org: tenant,
        body: {
          note: decisionNote || "Rollback governado pelo Admin Master",
        },
      });
      await refreshAll(selectedId);
    } catch (e) {
      setErr(extractError(e));
    } finally {
      setBusy(false);
    }
  }

  async function runBatchPlan() {
    setBusy(true);
    setErr("");
    try {
      const data = await apiFetch("/api/internal/evolution/batch/plan", {
        method: "POST",
        token,
        org: tenant,
        body: {
          limit: 5,
          confidence_min: 75,
          include_statuses: ["awaiting_master_approval", "approved"],
          allow_sensitive_domains: false,
        },
      });
      setBatchPlan(data);
    } catch (e) {
      setErr(extractError(e));
    } finally {
      setBusy(false);
    }
  }

  async function runBatchExecute() {
    setBusy(true);
    setErr("");
    try {
      const data = await apiFetch("/api/internal/evolution/batch/execute", {
        method: "POST",
        token,
        org: tenant,
        body: {
          limit: 5,
          confidence_min: 75,
          include_statuses: ["awaiting_master_approval", "approved"],
          allow_sensitive_domains: false,
          note: decisionNote || null,
          auto_pr: executionForm.auto_pr,
          path: executionForm.path || "app/db.py",
          validate_after_commit: executionForm.validate_after_commit,
          override_hard_gate: executionForm.override_hard_gate,
        },
      });
      setBatchPlan(data);
      await refreshAll(selectedId);
    } catch (e) {
      setErr(extractError(e));
    } finally {
      setBusy(false);
    }
  }

  const stats = useMemo(() => ([
    {
      label: "Pendentes",
      value: health?.pending_master_approval ?? "-",
      tone: (health?.pending_master_approval || 0) > 0 ? "warn" : "default",
    },
    {
      label: "Aprovadas",
      value: health?.approved_pending_execution ?? "-",
      tone: (health?.approved_pending_execution || 0) > 0 ? "warn" : "default",
    },
    {
      label: "SLA vencido",
      value: health?.overdue_count ?? "-",
      tone: (health?.overdue_count || 0) > 0 ? "bad" : "good",
      subtitle: `Due soon ${health?.due_soon_count ?? 0}`,
    },
    {
      label: "Pressão da fila",
      value: backlogPressureLabel(health?.backlog_pressure),
      tone: health?.backlog_pressure === "critical" ? "bad" : health?.backlog_pressure === "high" ? "warn" : "good",
      subtitle: `Score ${health?.backlog_pressure_score ?? 0}`,
    },
    {
      label: "Burst recomendado",
      value: health?.review_burst_size ?? "-",
      tone: (health?.review_burst_size || 0) >= 5 ? "warn" : "default",
      subtitle: health?.review_window_seconds ? `janela ${Math.round(Number(health.review_window_seconds || 0) / 60)} min` : "",
    },
    {
      label: "Execute candidates",
      value: health?.execute_candidates ?? "-",
      tone: (health?.execute_candidates || 0) > 0 ? "good" : "default",
      subtitle: `Threshold ${health?.signature_stability_threshold || "-"}x`,
    },
    {
      label: "Prioridade média",
      value: health?.avg_priority ?? "-",
      tone: scoreTone(health?.avg_priority ?? 0),
      subtitle: `Policy ${health?.policy_version || policyOverview?.version || "-"}`,
    },
    {
      label: "Peso médio da fila",
      value: health?.avg_backlog_weight ?? "-",
      tone: scoreTone(health?.avg_backlog_weight ?? 0),
      subtitle: "Envelhecimento + prioridade + SLA",
    },
    {
      label: "Confiança operacional",
      value: health?.avg_operator_confidence ?? "-",
      tone: scoreTone(health?.avg_operator_confidence ?? 0),
      subtitle: "Média da fila semiassistida",
    },
    {
      label: "Próximo loop sugerido",
      value: health?.next_interval_suggested_seconds ? `${health.next_interval_suggested_seconds}s` : "-",
      tone: scoreTone((topQueue?.[0]?.scores?.priority) || 0),
      subtitle: `Cooldown ${health?.touch_cooldown_seconds || "-"}s`,
    },
    {
      label: "Janela do dia",
      value: workWindowModeLabel(workWindow?.mode),
      tone: workWindow?.mode === "triage_intensive" ? "bad" : workWindow?.mode === "burst_review" ? "warn" : "good",
      subtitle: durationLabel(workWindow?.window_seconds),
    },
    {
      label: "Domínio foco",
      value: health?.today_focus_domain || dailyAgenda?.dominant_domain || "-",
      tone: focusDomainTone(health?.today_focus_domain || dailyAgenda?.dominant_domain),
      subtitle: `${dailyAgenda?.focus_count ?? 0} itens na agenda`,
    },
  ]), [health, policyOverview, topQueue, workWindow, dailyAgenda]);

  return (
    <div className="min-h-screen bg-[#060816] text-white">
      <div className="mx-auto max-w-7xl px-4 py-6 md:px-6 lg:px-8">
        <header className="rounded-[28px] border border-white/10 bg-[radial-gradient(circle_at_top,rgba(56,189,248,0.15),transparent_35%),rgba(255,255,255,0.03)] p-6 shadow-2xl shadow-black/20">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <div className="inline-flex items-center gap-2 rounded-full border border-cyan-400/20 bg-cyan-400/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-cyan-100">
                Admin Master
              </div>
              <h1 className="mt-4 text-3xl font-black tracking-tight md:text-4xl">Governed Evolution Center</h1>
              <p className="mt-2 max-w-3xl text-sm text-white/70 md:text-base">
                Mesa de decisão da autoevolução do Orkio. Aqui a fila deixa de ser só lista e passa a ter
                prioridade, risco, impacto, lane de política e execução governada.
              </p>
              <div className="mt-3 flex flex-wrap gap-2 text-xs text-white/60">
                <Tag tone="good">Tenant: {tenant}</Tag>
                <Tag>{user?.email || "admin"}</Tag>
                <Tag>Modo: {health?.mode || "governed"}</Tag>
                <Tag>{policyOverview?.version || health?.policy_version || "policy-pending"}</Tag>
                <Tag>{health?.master_admin_emails_configured ? "Master gate configurado" : "Master gate sem whitelist"}</Tag>
              </div>
            </div>

            <div className="flex flex-wrap gap-3">
              <button
                onClick={() => nav("/admin")}
                className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-semibold text-white/80 hover:bg-white/10"
              >
                ← Voltar ao Admin
              </button>
              <button
                onClick={runScanNow}
                disabled={busy}
                className="rounded-2xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-3 text-sm font-semibold text-cyan-100 hover:bg-cyan-400/20 disabled:opacity-50"
              >
                {busy ? "Processando..." : "Rodar scan governado"}
              </button>
              <button
                onClick={() => refreshAll(selectedId)}
                disabled={busy}
                className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-semibold text-white/80 hover:bg-white/10 disabled:opacity-50"
              >
                Atualizar
              </button>
            </div>
          </div>
        </header>

        <section className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-6">
          {stats.map((item) => (
            <Stat key={item.label} label={item.label} value={item.value} tone={item.tone} subtitle={item.subtitle} />
          ))}
        </section>

        <section className="mt-6 grid gap-4 xl:grid-cols-[1.2fr,0.8fr]">
          <div className={`${CARD}`}>
            <SectionTitle
              title="Policy ativa"
              subtitle="Regras operacionais que governam a fila e a permissão de execução."
            />
            <div className="mt-4 flex flex-wrap gap-2">
              {Array.isArray(policyOverview?.lanes) ? policyOverview.lanes.map((lane) => (
                <div key={lane.lane} className="rounded-2xl border border-white/10 bg-black/20 px-4 py-3">
                  <div className="text-sm font-semibold capitalize">{String(lane.lane || "").replaceAll("_", " ")}</div>
                  <div className="mt-1 text-xs text-white/60">{lane.notes}</div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {(lane.actions || []).map((action) => <Tag key={action}>{action}</Tag>)}
                  </div>
                </div>
              )) : <div className="text-sm text-white/55">Policy overview indisponível.</div>}
            </div>
          </div>

          <div className={`${CARD}`}>
            <SectionTitle
              title="Fila prioritária"
              subtitle="Top 5 proposals ordenadas por prioridade de política."
            />
            <div className="mt-4 space-y-3">
              {topQueue.length ? topQueue.map((item) => (
                <button
                  key={item.id}
                  onClick={() => setSelectedId(item.id)}
                  className="w-full rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-left hover:bg-white/5"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="truncate text-sm font-semibold">{item.title || item.code || item.id}</div>
                      <div className="mt-1 text-xs text-white/55">{item.priority_label} · {item.action} · {item.domain_scope || "general"} · {recommendationLabel(item.recommendation)}</div>
                      <div className="mt-1 flex flex-wrap gap-2 text-[11px] text-white/55">
                        <Tag tone={operatorSuggestionTone(item?.operator_guidance?.suggested_action)}>{operatorSuggestionLabel(item?.operator_guidance?.suggested_action)}</Tag>
                        <Tag>{item?.operator_guidance?.confidence_score ?? 0}%</Tag>
                        <Tag tone={slaTone(item?.sla?.state)}>{slaLabel(item?.sla?.state)}</Tag>
                        <Tag tone={focusDomainTone(item?.domain_scope)}>{item?.domain_scope || "general"}</Tag>
                        <Tag tone={ageBandTone(item?.age?.band)}>{ageBandLabel(item?.age?.band)}</Tag>
                      </div>
                    </div>
                    <Tag tone={toneForBand(item.priority_band)}>{item.scores?.priority ?? 0}</Tag>
                    <Tag>{item?.cadence?.seconds || "-"}s</Tag>
                  </div>
                </button>
              )) : <div className="text-sm text-white/55">Sem proposals priorizadas no momento.</div>}
            </div>
          </div>
        </section>

<section className="mt-6 grid gap-4 xl:grid-cols-[1fr,1fr,1fr]">
  <div className={`${CARD}`}>
    <SectionTitle
      title="Agenda operacional do dia"
      subtitle="Resumo da janela de trabalho recomendada para a tua mesa de decisão."
    />
    <div className="mt-4 grid gap-3">
      <Stat
        label="Modo"
        value={workWindowModeLabel(workWindow?.mode)}
        tone={workWindow?.mode === "triage_intensive" ? "bad" : workWindow?.mode === "burst_review" ? "warn" : "good"}
        subtitle={workWindow?.summary || ""}
      />
      <div className="grid gap-3 md:grid-cols-2">
        <Stat
          label="Janela"
          value={durationLabel(workWindow?.window_seconds)}
          tone="default"
          subtitle={`slots ${workWindow?.operator_slots ?? 0}`}
        />
        <Stat
          label="Começar por"
          value={String(workWindow?.recommended_start_with || "-").replaceAll("_", " ")}
          tone={workWindow?.recommended_start_with === "overdue" ? "bad" : workWindow?.recommended_start_with === "due_soon" ? "warn" : "good"}
          subtitle={`pendentes ${workWindow?.pending_queue_count ?? 0}`}
        />
      </div>
      <div className="rounded-2xl border border-white/10 bg-black/20 p-4 text-sm text-white/75">
        {dailyAgenda?.summary || "Sem agenda operacional calculada ainda."}
      </div>
      <div className="flex flex-wrap gap-2">
        <Tag tone={focusDomainTone(dailyAgenda?.dominant_domain)}>Domínio foco: {dailyAgenda?.dominant_domain || "general"}</Tag>
        <Tag tone="good">Execute now: {dailyAgenda?.execute_now_count ?? 0}</Tag>
        {Object.entries(dailyAgenda?.action_buckets || {}).map(([key, value]) => (
          <Tag key={`agenda-${key}`} tone={operatorSuggestionTone(key)}>
            {agendaActionLabel(key)}: {value}
          </Tag>
        ))}
      </div>
      <div className="mt-4 space-y-2">
        {(dailyAgenda?.focus_items || []).length ? dailyAgenda.focus_items.map((item) => (
          <div key={item.id} className="rounded-2xl border border-white/10 bg-black/20 px-4 py-3">
            <div className="flex items-start justify-between gap-3">
              <button className="text-left" onClick={() => setSelectedId(item.id)}>
                <div className="text-sm font-semibold">{item.title}</div>
                <div className="mt-1 text-xs text-white/55">{item.domain_scope || "general"} · {item.priority_score || 0}</div>
              </button>
              <div className="flex flex-wrap gap-2">
                <Tag tone={operatorSuggestionTone(item.suggested_action)}>{agendaActionLabel(item.suggested_action)}</Tag>
                <Tag tone={slaTone(item.sla_state)}>{slaLabel(item.sla_state)}</Tag>
              </div>
            </div>
          </div>
        )) : (
          <div className="rounded-2xl border border-dashed border-white/10 px-4 py-6 text-sm text-white/55">
            Sem itens focais na agenda do dia.
          </div>
        )}
      </div>
    </div>
  </div>

  <div className={`${CARD}`}>
    <SectionTitle
      title="Janela de trabalho"
      subtitle="Capacidade operacional real sugerida pela pressão da fila."
    />
    <div className="mt-4 grid gap-3">
      <div className="grid gap-3 md:grid-cols-2">
        <Stat label="Burst" value={workWindow?.review_burst_size ?? "-"} tone={(workWindow?.review_burst_size || 0) >= 5 ? "warn" : "default"} subtitle={`em ${durationLabel(workWindow?.window_seconds)}`} />
        <Stat label="Slots" value={workWindow?.operator_slots ?? "-"} tone="default" subtitle={workWindow?.lane ? String(workWindow.lane).replaceAll("_", " ") : ""} />
      </div>
      <div className="flex flex-wrap gap-2">
        <Tag tone="bad">Overdue: {workWindow?.overdue_count ?? 0}</Tag>
        <Tag tone="warn">Due soon: {workWindow?.due_soon_count ?? 0}</Tag>
        <Tag>Pressure score: {workWindow?.backlog_pressure_score ?? 0}</Tag>
      </div>
      <div className="rounded-2xl border border-white/10 bg-black/20 p-4 text-sm text-white/70">
        {workWindow?.summary || "Sem janela calculada."}
      </div>
    </div>
  </div>

  <div className={`${CARD}`}>
    <SectionTitle
      title="SLA automático por domínio"
      subtitle="Regras que encurtam o alvo de revisão quando o domínio é mais sensível."
    />
    <div className="mt-4 space-y-3">
      {Object.entries(domainSlaPolicy || {}).length ? Object.entries(domainSlaPolicy).map(([domain, policy]) => (
        <div key={domain} className="rounded-2xl border border-white/10 bg-black/20 p-4">
          <div className="flex items-center justify-between gap-3">
            <div className="text-sm font-semibold capitalize">{domain}</div>
            <Tag tone={focusDomainTone(domain)}>{domain}</Tag>
          </div>
          <div className="mt-3 grid grid-cols-2 gap-2 text-xs text-white/70">
            {Object.entries(policy || {}).map(([sev, seconds]) => (
              <div key={`${domain}-${sev}`} className="rounded-xl border border-white/10 bg-white/5 px-3 py-2">
                <span className="text-white/45 uppercase">{sev}</span>: {durationLabel(seconds)}
              </div>
            ))}
          </div>
        </div>
      )) : (
        <div className="rounded-2xl border border-dashed border-white/10 px-4 py-8 text-sm text-white/55">
          SLA por domínio indisponível.
        </div>
      )}
    </div>
  </div>
</section>


        {err ? (
          <div className="mt-6 rounded-3xl border border-rose-400/20 bg-rose-400/10 px-5 py-4 text-sm text-rose-100">
            {err}
          </div>
        ) : null}

        <section className="mt-6 grid gap-4 xl:grid-cols-[1fr,1fr]">
          <div className={`${CARD}`}>
            <SectionTitle
              title="Memória de ciclos"
              subtitle="Histórico recente da cadência e das recomendações da autoevolução."
            />
            <div className="mt-4 space-y-3">
              {recentCycles.length ? recentCycles.map((cycle) => (
                <div key={cycle.id} className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                    <div>
                      <div className="text-sm font-semibold">{fmtTs(cycle.created_at)}</div>
                      <div className="mt-1 text-xs text-white/50">trace_id: {cycle.trace_id || "-"}</div>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <Tag tone={scoreTone(cycle.max_priority_score)}>{cycle.max_priority_score}</Tag>
                      <Tag>{cycle.next_interval_suggested_seconds || "-"}s</Tag>
                      <Tag>{cycle.findings || 0} findings</Tag>
                    </div>
                  </div>
                  <div className="mt-3 grid gap-2 text-sm text-white/75 md:grid-cols-2">
                    <div><span className="text-white/45">Classified:</span> {cycle.classified || 0}</div>
                    <div><span className="text-white/45">Touched:</span> {cycle.proposals_touched || 0}</div>
                    <div><span className="text-white/45">Created:</span> {cycle.proposals_created || 0}</div>
                    <div><span className="text-white/45">Suppressed:</span> {cycle.proposals_suppressed || 0}</div>
                  </div>
                </div>
              )) : (
                <div className="rounded-2xl border border-dashed border-white/10 px-4 py-8 text-sm text-white/55">
                  Ainda não há memória de ciclos persistida.
                </div>
              )}
            </div>
          </div>

          <div className={`${CARD}`}>
            <SectionTitle
              title="Domínios observados"
              subtitle="Distribuição atual dos problemas por domínio afetado."
            />
            <div className="mt-4 flex flex-wrap gap-2">
              {Object.entries(health?.domain_buckets || {}).length ? Object.entries(health?.domain_buckets || {}).map(([key, value]) => (
                <Tag key={key} tone={["security","billing","auth"].includes(String(key)) ? "warn" : "default"}>
                  {key}: {value}
                </Tag>
              )) : (
                <div className="text-sm text-white/55">Sem buckets de domínio no momento.</div>
              )}
            </div>
          </div>

          <div className={`${CARD}`}>
            <SectionTitle
              title="Admin recommendations"
              subtitle="Distribuição atual da recomendação operacional de execução versus revisão."
            />
            <div className="mt-4 flex flex-wrap gap-2">
              {Object.entries(adminRecommendationBuckets || {}).length ? Object.entries(adminRecommendationBuckets || {}).map(([key, value]) => (
                <Tag key={key} tone={adminRecommendationTone(key)}>
                  {adminRecommendationLabel(key)}: {value}
                </Tag>
              )) : (
                <div className="text-sm text-white/55">Sem buckets de recomendação admin no momento.</div>
              )}
            </div>
          </div>

          <div className={`${CARD}`}>
            <SectionTitle
              title="Sugestões operacionais"
              subtitle="Próxima ação sugerida pelo sistema para a tua mesa de decisão."
            />
            <div className="mt-4 flex flex-wrap gap-2">
              {Object.entries(operatorSuggestionBuckets || {}).length ? Object.entries(operatorSuggestionBuckets || {}).map(([key, value]) => (
                <Tag key={key} tone={operatorSuggestionTone(key)}>
                  {operatorSuggestionLabel(key)}: {value}
                </Tag>
              )) : (
                <div className="text-sm text-white/55">Sem buckets de sugestão operacional no momento.</div>
              )}
            </div>
          </div>
          <div className={`${CARD}`}>
            <SectionTitle
              title="Capacidade operacional"
              subtitle="SLA, envelhecimento da fila e cadência recomendada para tua mesa de decisão."
            />
            <div className="mt-4 grid gap-3 md:grid-cols-2">
              <Stat
                label="Pressão da fila"
                value={backlogPressureLabel(health?.backlog_pressure)}
                tone={health?.backlog_pressure === "critical" ? "bad" : health?.backlog_pressure === "high" ? "warn" : "good"}
                subtitle={`score ${health?.backlog_pressure_score ?? 0}`}
              />
              <Stat
                label="Burst recomendado"
                value={health?.review_burst_size ?? "-"}
                tone={(health?.review_burst_size || 0) >= 5 ? "warn" : "default"}
                subtitle={health?.review_window_seconds ? `${Math.round(Number(health.review_window_seconds || 0) / 60)} min` : ""}
              />
            </div>
            <div className="mt-4 flex flex-wrap gap-2">
              {Object.entries(health?.sla_buckets || {}).length ? Object.entries(health?.sla_buckets || {}).map(([key, value]) => (
                <Tag key={key} tone={slaTone(key)}>
                  {slaLabel(key)}: {value}
                </Tag>
              )) : null}
              {Object.entries(health?.age_buckets || {}).length ? Object.entries(health?.age_buckets || {}).map(([key, value]) => (
                <Tag key={`age-${key}`} tone={ageBandTone(key)}>
                  {ageBandLabel(key)}: {value}
                </Tag>
              )) : null}
              {Object.entries(health?.capacity_hint_buckets || {}).length ? Object.entries(health?.capacity_hint_buckets || {}).map(([key, value]) => (
                <Tag key={`cap-${key}`}>
                  {capacityHintLabel(key)}: {value}
                </Tag>
              )) : null}
            </div>
          </div>

          <div className={`${CARD}`}>
            <SectionTitle
              title="Tendências observadas"
              subtitle="Escalonamento por tendência recorrente da assinatura do problema."
            />
            <div className="mt-4 flex flex-wrap gap-2">
              {Object.entries(trendBuckets || {}).length ? Object.entries(trendBuckets || {}).map(([key, value]) => (
                <Tag key={key} tone={trendTone(key)}>
                  {trendLabel(key)}: {value}
                </Tag>
              )) : (
                <div className="text-sm text-white/55">Sem buckets de tendência no momento.</div>
              )}
            </div>
          </div>

        </section>

        {scanResult ? (
          <div className="mt-6 rounded-3xl border border-cyan-400/20 bg-cyan-400/10 p-5 text-sm text-cyan-50">
            <div className="font-semibold">Último scan governado</div>
            <div className="mt-2 flex flex-wrap gap-2">
              <Tag tone="good">trace_id: {scanResult.trace_id || "-"}</Tag>
              <Tag>findings: {scanResult.findings ?? "-"}</Tag>
              <Tag>created: {scanResult.proposals_created ?? "-"}</Tag>
              <Tag>touched: {scanResult.proposals_touched ?? "-"}</Tag>
              <Tag>suppressed: {scanResult.proposals_suppressed ?? "-"}</Tag>
            </div>
          </div>
        ) : null}

        <div className="mt-6 grid gap-6 xl:grid-cols-[380px,minmax(0,1fr)]">
          <aside className={`${CARD} h-fit`}>
            <SectionTitle title="Fila de proposals" subtitle="Decisão governada por prioridade, risco e impacto." />
            <div className="mt-4 space-y-3">
              <input
                className={INPUT}
                placeholder="Buscar por título, resumo, código..."
                value={queryText}
                onChange={(e) => setQueryText(e.target.value)}
              />
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-1">
                <select className={SELECT} value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
                  <option value="">Todos os status</option>
                  <option value="awaiting_master_approval">Aguardando aprovação</option>
                  <option value="approved">Aprovadas</option>
                  <option value="on_hold">Em hold</option>
                  <option value="executed">Executadas</option>
                  <option value="rejected">Rejeitadas</option>
                  <option value="rolled_back">Rollback</option>
                </select>
                <select className={SELECT} value={actionFilter} onChange={(e) => setActionFilter(e.target.value)}>
                  <option value="">Todas as actions</option>
                  {actionsCatalog.map((item) => (
                    <option key={item.action} value={item.action}>{item.action}</option>
                  ))}
                </select>
                <select className={SELECT} value={severityFilter} onChange={(e) => setSeverityFilter(e.target.value)}>
                  <option value="">Todas as severidades</option>
                  <option value="CRITICAL">CRITICAL</option>
                  <option value="HIGH">HIGH</option>
                  <option value="MEDIUM">MEDIUM</option>
                  <option value="LOW">LOW</option>
                </select>
                <select className={SELECT} value={sortMode} onChange={(e) => setSortMode(e.target.value)}>
                  <option value="priority">Ordenar por prioridade</option>
                  <option value="sla">Ordenar por SLA / envelhecimento</option>
                  <option value="recent">Ordenar por atualização</option>
                  <option value="severity">Ordenar por severidade</option>
                </select>
              </div>
            </div>

            <div className="mt-4 space-y-3">
              {items.length ? items.map((item) => (
                <button
                  key={item.id}
                  onClick={() => setSelectedId(item.id)}
                  className={`w-full rounded-2xl border p-4 text-left transition ${
                    selectedId === item.id
                      ? "border-cyan-400/40 bg-cyan-400/10"
                      : "border-white/10 bg-black/20 hover:bg-white/5"
                  }`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="truncate text-sm font-semibold text-white">{item.title || item.code || item.id}</div>
                      <div className="mt-1 line-clamp-2 text-xs text-white/50">{item.summary || item.category || "Sem resumo"}</div>
                    </div>
                    <Tag tone={toneForStatus(item.status)}>{item.status || "unknown"}</Tag>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2 text-xs text-white/55">
                    <Tag tone={toneForBand(item.priority_band)}>{item.priority_label || "Priority P4"}</Tag>
                    <Tag>{item.action || "n/a"}</Tag>
                    <Tag>{item.severity || "n/a"}</Tag>
                    <Tag>{item.policy?.lane || "n/a"}</Tag>
                    <Tag tone={trendTone(item.trend?.state)}>{trendLabel(item.trend?.state)}</Tag>
                    <Tag tone={slaTone(item?.sla?.state)}>{slaLabel(item?.sla?.state)}</Tag>
                    <Tag tone={ageBandTone(item?.age?.band)}>{ageBandLabel(item?.age?.band)}</Tag>
                    <Tag tone={adminRecommendationTone(item.admin_recommendation)}>{adminRecommendationLabel(item.admin_recommendation)}</Tag>
                    <Tag tone={operatorSuggestionTone(item?.operator_guidance?.suggested_action)}>{operatorSuggestionLabel(item?.operator_guidance?.suggested_action)}</Tag>
                  </div>
                  <div className="mt-3 grid grid-cols-2 gap-2 text-[11px] text-white/55 md:grid-cols-3">
                    <div>Prioridade <span className="font-semibold text-white">{item.scores?.priority ?? 0}</span></div>
                    <div>Blast <span className="font-semibold text-white">{item.scores?.blast_radius ?? 0}</span></div>
                    <div>Segurança <span className="font-semibold text-white">{item.scores?.security ?? 0}</span></div>
                    <div>Peso fila <span className="font-semibold text-white">{item.backlog?.weight_score ?? 0}</span></div>
                    <div>Capacidade <span className="font-semibold text-white">{capacityHintLabel(item.backlog?.capacity_hint)}</span></div>
                    <div>SLA alvo <span className="font-semibold text-white">{item.sla?.target_seconds ? `${Math.round(Number(item.sla.target_seconds || 0) / 60)} min` : "-"}</span></div>
                  </div>
                  <div className="mt-2 text-[11px] text-white/45">{recommendationLabel(item.recommendation)}</div>
                  <div className="mt-2 text-[11px] text-white/40">Atualizada em {fmtTs(item.updated_at)}{item.cooldown?.active ? ` · cooldown ${item.cooldown.remaining_seconds}s` : ""}{item.sla?.overdue_seconds ? ` · overdue ${item.sla.overdue_seconds}s` : ""}</div>
                </button>
              )) : (
                <div className="rounded-2xl border border-dashed border-white/10 px-4 py-8 text-sm text-white/55">
                  Nenhuma proposal encontrada para o filtro atual.
                </div>
              )}
            </div>
          </aside>

          <main className="space-y-6">
            <section className={CARD}>
              <SectionTitle
                title="Detalhe da proposal"
                subtitle="Governança explícita da autoevolução. Só o Admin Master libera execução."
                right={selected ? <Tag tone={toneForStatus(selected.status)}>{selected.status}</Tag> : null}
              />

              {!selected ? (
                <div className="mt-4 rounded-2xl border border-dashed border-white/10 px-4 py-8 text-sm text-white/55">
                  Selecione uma proposal para inspecionar.
                </div>
              ) : (
                <div className="mt-5 space-y-5">
                  <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-10">
                    <Stat label="Prioridade" value={selected.scores?.priority ?? 0} tone={toneForBand(selected.priority_band)} subtitle={selected.priority_label} />
                    <Stat label="Risco" value={selected.scores?.risk ?? 0} tone={toneForBand(selected.risk_band)} subtitle={selected.code || "-"} />
                    <Stat label="Impacto" value={selected.scores?.impact ?? 0} tone={toneForBand(selected.impact_band)} subtitle={selected.action || "-"} />
                    <Stat label="Blast radius" value={selected.scores?.blast_radius ?? 0} tone={toneForBand(selected.blast_radius_band)} subtitle={selected.category || "-"} />
                    <Stat label="Segurança" value={selected.scores?.security ?? 0} tone={toneForBand(selected.security_band)} subtitle={recommendationLabel(selected.recommendation)} />
                    <Stat label="Confiança" value={selected.scores?.confidence ?? 0} tone={scoreTone(selected.scores?.confidence ?? 0)} subtitle={`Lane: ${selected.policy?.lane || "-"}`} />
                    <Stat label="Tendência" value={selected.trend?.state || "-"} tone={trendTone(selected.trend?.state)} subtitle={`Δ ${selected.trend?.delta ?? 0}`} />
                    <Stat label="SLA" value={slaLabel(selected?.sla?.state)} tone={slaTone(selected?.sla?.state)} subtitle={selected?.sla?.target_seconds ? `${durationLabel(selected?.sla?.target_seconds)} alvo · ${selected?.sla?.domain_scope || "general"}` : "-"} />
                    <Stat label="Idade" value={ageBandLabel(selected?.age?.band)} tone={ageBandTone(selected?.age?.band)} subtitle={selected?.age?.open_seconds ? `${Math.round(Number(selected.age.open_seconds || 0) / 60)} min aberta` : "-"} />
                    <Stat label="Próxima ação" value={operatorSuggestionLabel(selected?.operator_guidance?.suggested_action)} tone={operatorSuggestionTone(selected?.operator_guidance?.suggested_action)} subtitle={`${selected?.operator_guidance?.confidence_score ?? 0}% de confiança`} />
                  </div>

                  <div className="grid gap-5 xl:grid-cols-[1.1fr,0.9fr]">
                    <div className="space-y-4">
                      <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                        <div className="text-xs uppercase tracking-[0.18em] text-white/40">Identidade e policy</div>
                        <div className="mt-3 grid gap-2 text-sm text-white/80">
                          <div><span className="text-white/45">ID:</span> {selected.id}</div>
                          <div><span className="text-white/45">Título:</span> {selected.title || "-"}</div>
                          <div><span className="text-white/45">Resumo:</span> {selected.summary || "-"}</div>
                          <div><span className="text-white/45">Código:</span> {selected.code || "-"}</div>
                          <div><span className="text-white/45">Action:</span> {selected.action || "-"}</div>
                          <div><span className="text-white/45">Domínio:</span> {selected.domain_scope || "-"}</div>
                          <div><span className="text-white/45">SLA domínio:</span> {durationLabel(selected?.sla?.domain_target_seconds)} ({selected?.sla?.policy_rule || "-"})</div>
                          <div><span className="text-white/45">Lane:</span> {selected.policy?.lane || "-"}</div>
                          <div><span className="text-white/45">Owner review:</span> {selected.policy?.owner_review_required ? "sim" : "não"}</div>
                          <div><span className="text-white/45">Execution allowed:</span> {selected.policy?.execution_allowed ? "sim" : "não"}</div>
                          <div><span className="text-white/45">Trust level:</span> {selected.policy?.source_trust_level || selected.decision?.trust?.source_trust_level || "-"}</div>
                          <div><span className="text-white/45">Instruction authority:</span> {selected.policy?.instruction_authority ? "sim" : "não"}</div>
                          <div><span className="text-white/45">Secret exposure risk:</span> {selected.policy?.secret_exposure_risk ?? 0}</div>
                          <div><span className="text-white/45">Required review domains:</span> {Array.isArray(selected.semantic_validation?.required_review_domains) && selected.semantic_validation.required_review_domains.length ? selected.semantic_validation.required_review_domains.join(", ") : "-"}</div>
                          <div><span className="text-white/45">Blocked domains:</span> {Array.isArray(selected.semantic_validation?.blocked_domains) && selected.semantic_validation.blocked_domains.length ? selected.semantic_validation.blocked_domains.join(", ") : "-"}</div>
                          <div><span className="text-white/45">Razão da policy:</span> {selected.policy?.reason || "-"}</div>
                          <div><span className="text-white/45">Recomendação:</span> {recommendationLabel(selected.recommendation)}</div>
                          <div><span className="text-white/45">Admin recommendation:</span> {adminRecommendationLabel(selected.admin_recommendation)}</div>
                          <div><span className="text-white/45">Próxima ação sugerida:</span> {operatorSuggestionLabel(selected?.operator_guidance?.suggested_action)}</div>
                          <div><span className="text-white/45">Confiança operacional:</span> {selected?.operator_guidance?.confidence_score ?? 0}%</div>
                          <div><span className="text-white/45">Execute candidate:</span> {selected.execute_candidate ? "sim" : "não"}</div>
                          <div><span className="text-white/45">Tendência:</span> {trendLabel(selected.trend?.state)} (Δ {selected.trend?.delta ?? 0})</div>
                          <div><span className="text-white/45">Domínio:</span> {selected.domain_scope || "-"}</div>
                          <div><span className="text-white/45">Detectado:</span> {selected.detected_count || 0}x</div>
                          <div><span className="text-white/45">Recorrência na janela:</span> {selected.recurrence?.window_count || 1}</div>
                          <div><span className="text-white/45">Assinatura repetida:</span> {selected.recurrence?.signature_repeat_count || selected.recurrence?.window_count || 1}</div>
                          <div><span className="text-white/45">Cadência sugerida:</span> {selected.cadence?.seconds ? `${selected.cadence.seconds}s` : "-"}</div>
                          <div><span className="text-white/45">SLA:</span> {slaLabel(selected?.sla?.state)} · alvo {selected?.sla?.target_seconds ? `${Math.round(Number(selected.sla.target_seconds || 0) / 60)} min` : "-"}</div>
                          <div><span className="text-white/45">Idade da fila:</span> {ageBandLabel(selected?.age?.band)} · {selected?.age?.open_seconds ? `${Math.round(Number(selected.age.open_seconds || 0) / 60)} min` : "-"}</div>
                          <div><span className="text-white/45">Peso da fila:</span> {selected.backlog?.weight_score ?? 0}</div>
                          <div><span className="text-white/45">Capacidade sugerida:</span> {capacityHintLabel(selected.backlog?.capacity_hint)}</div>
                          <div><span className="text-white/45">Blast acumulado:</span> {selected.accumulated?.blast_radius ?? 0}</div>
                          <div><span className="text-white/45">Segurança acumulada:</span> {selected.accumulated?.security ?? 0}</div>
                          <div><span className="text-white/45">Cooldown:</span> {selected.cooldown?.active ? `${selected.cooldown.remaining_seconds}s restantes` : "livre"}</div>
                          <div><span className="text-white/45">Racional da sugestão:</span> {selected?.operator_guidance?.rationale || "-"}</div>
                          <div><span className="text-white/45">Primeira detecção:</span> {fmtTs(selected.first_detected_at)}</div>
                          <div><span className="text-white/45">Última atualização:</span> {fmtTs(selected.updated_at)}</div>
                        </div>
                        <div className="mt-4 flex flex-wrap gap-2">
                          <Tag tone={toneForBand(selected.priority_band)}>{selected.priority_label}</Tag>
                          <Tag>{selected.category || "n/a"}</Tag>
                          <Tag>{selected.domain_scope || "general"}</Tag>
                          <Tag>{selected.source || "n/a"}</Tag>
                          <Tag>{selected.severity || "n/a"}</Tag>
                          <Tag>{selected.policy?.version || "-"}</Tag>
                          <Tag tone={trendTone(selected.trend?.state)}>{trendLabel(selected.trend?.state)}</Tag>
                          <Tag tone={slaTone(selected?.sla?.state)}>{slaLabel(selected?.sla?.state)}</Tag>
                          <Tag tone={ageBandTone(selected?.age?.band)}>{ageBandLabel(selected?.age?.band)}</Tag>
                          <Tag>{capacityHintLabel(selected?.backlog?.capacity_hint)}</Tag>
                          <Tag tone={adminRecommendationTone(selected.admin_recommendation)}>{adminRecommendationLabel(selected.admin_recommendation)}</Tag>
                          <Tag tone={operatorSuggestionTone(selected?.operator_guidance?.suggested_action)}>{operatorSuggestionLabel(selected?.operator_guidance?.suggested_action)}</Tag>
                          <Tag>{selected?.operator_guidance?.confidence_score ?? 0}%</Tag>
                          <Tag>{selected.policy?.source_trust_level || selected.decision?.trust?.source_trust_level || "internal"}</Tag>
                          {(selected.semantic_validation?.blocked_domains || []).map((domain) => <Tag key={domain} tone="bad">{domain}</Tag>)}
                        </div>
                      </div>

                      <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                        <div className="text-xs uppercase tracking-[0.18em] text-white/40">Notas do Admin Master</div>
                        <div className="mt-3">
                          <textarea
                            className={TEXTAREA}
                            value={decisionNote}
                            onChange={(e) => setDecisionNote(e.target.value)}
                            placeholder="Registre a motivação da decisão, instruções de execução ou observações de rollback."
                          />
                        </div>
                      </div>

                      <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                        <div className="text-xs uppercase tracking-[0.18em] text-white/40">Parâmetros de execução</div>
                        <div className="mt-4 grid gap-3 md:grid-cols-2">
                          <input
                            className={INPUT}
                            value={executionForm.source_branch}
                            onChange={(e) => setExecutionForm((prev) => ({ ...prev, source_branch: e.target.value }))}
                            placeholder="source_branch opcional"
                          />
                          <input
                            className={INPUT}
                            value={executionForm.path}
                            onChange={(e) => setExecutionForm((prev) => ({ ...prev, path: e.target.value }))}
                            placeholder="path alvo"
                          />
                        </div>
                        <div className="mt-4 flex flex-wrap gap-3">
                          <label className="inline-flex items-center gap-2 text-sm text-white/80">
                            <input
                              type="checkbox"
                              checked={!!executionForm.auto_pr}
                              onChange={(e) => setExecutionForm((prev) => ({ ...prev, auto_pr: e.target.checked }))}
                            />
                            auto_pr
                          </label>
                          <label className="inline-flex items-center gap-2 text-sm text-white/80">
                            <input
                              type="checkbox"
                              checked={!!executionForm.validate_after_commit}
                              onChange={(e) => setExecutionForm((prev) => ({ ...prev, validate_after_commit: e.target.checked }))}
                            />
                            validate_after_commit
                          </label>
                          <label className="inline-flex items-center gap-2 text-sm text-white/80">
                            <input
                              type="checkbox"
                              checked={!!executionForm.override_hard_gate}
                              onChange={(e) => setExecutionForm((prev) => ({ ...prev, override_hard_gate: e.target.checked }))}
                            />
                            override_hard_gate (Master Admin)
                          </label>
                        </div>
                      </div>
                    </div>

                    <div className="space-y-4">
                      <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                        <div className="text-xs uppercase tracking-[0.18em] text-white/40">Ações governadas</div>
                        <div className="mt-4 grid gap-3">
                          <button
                            onClick={() => decide("approve")}
                            disabled={busy || !selected}
                            className={`${BTN} border border-emerald-400/30 bg-emerald-400/10 text-emerald-100 hover:bg-emerald-400/20 disabled:opacity-50`}
                          >
                            Aprovar
                          </button>
                          <button
                            onClick={() => decide("approve", { executeNow: true })}
                            disabled={busy || !selected}
                            className={`${BTN} border border-cyan-400/30 bg-cyan-400/10 text-cyan-100 hover:bg-cyan-400/20 disabled:opacity-50`}
                          >
                            Aprovar + executar
                          </button>
                          <button
                            onClick={executeProposal}
                            disabled={busy || !selected}
                            className={`${BTN} border border-white/10 bg-white/5 text-white hover:bg-white/10 disabled:opacity-50`}
                          >
                            Executar proposal aprovada
                          </button>
                          <button
                            onClick={retryProposal}
                            disabled={busy || !selected}
                            className={`${BTN} border border-amber-400/30 bg-amber-400/10 text-amber-100 hover:bg-amber-400/20 disabled:opacity-50`}
                          >
                            Retry execution
                          </button>
                          <button
                            onClick={holdProposal}
                            disabled={busy || !selected}
                            className={`${BTN} border border-amber-400/30 bg-amber-400/10 text-amber-100 hover:bg-amber-400/20 disabled:opacity-50`}
                          >
                            Segurar em hold
                          </button>
                          <button
                            onClick={() => decide("reject")}
                            disabled={busy || !selected}
                            className={`${BTN} border border-rose-400/30 bg-rose-400/10 text-rose-100 hover:bg-rose-400/20 disabled:opacity-50`}
                          >
                            Rejeitar
                          </button>
                          <button
                            onClick={runBatchPlan}
                            disabled={busy}
                            className={`${BTN} border border-fuchsia-400/30 bg-fuchsia-400/10 text-fuchsia-100 hover:bg-fuchsia-400/20 disabled:opacity-50`}
                          >
                            Batch plan
                          </button>
                          <button
                            onClick={runBatchExecute}
                            disabled={busy || !isMasterAdminUser(user)}
                            className={`${BTN} border border-indigo-400/30 bg-indigo-400/10 text-indigo-100 hover:bg-indigo-400/20 disabled:opacity-50`}
                          >
                            Batch execute
                          </button>
                        </div>
                      </div>

                      <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                        <div className="text-xs uppercase tracking-[0.18em] text-white/40">Batch governado</div>
                        {!batchPlan ? (
                          <div className="mt-3 text-sm text-white/55">Nenhum batch plan carregado.</div>
                        ) : (
                          <div className="mt-3 space-y-2 text-sm text-white/80">
                            <div><span className="text-white/45">Elegíveis:</span> {batchPlan.eligible_count ?? batchPlan.executed?.length ?? 0}</div>
                            <div><span className="text-white/45">Bloqueados:</span> {batchPlan.blocked_count ?? batchPlan.skipped?.length ?? 0}</div>
                            <div><span className="text-white/45">Tentados:</span> {batchPlan.attempted ?? "-"}</div>
                          </div>
                        )}
                      </div>

                      <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                        <div className="text-xs uppercase tracking-[0.18em] text-white/40">Última execution</div>
                        {!latestExecution ? (
                          <div className="mt-3 text-sm text-white/55">Nenhuma execution registrada.</div>
                        ) : (
                          <div className="mt-3 space-y-3 text-sm text-white/80">
                            <div className="flex flex-wrap gap-2">
                              <Tag tone={toneForStatus(latestExecution.status)}>{latestExecution.status}</Tag>
                              <Tag>{latestExecution.mode || "-"}</Tag>
                              <Tag>{latestExecution.trace_id || "-"}</Tag>
                            </div>
                            <div><span className="text-white/45">Início:</span> {fmtTs(latestExecution.started_at)}</div>
                            <div><span className="text-white/45">Conclusão:</span> {fmtTs(latestExecution.completed_at)}</div>
                            <div><span className="text-white/45">Actor:</span> {latestExecution.actor_ref || "-"}</div>
                            {latestExecution.error_text ? (
                              <div className="rounded-2xl border border-rose-400/20 bg-rose-400/10 px-3 py-2 text-rose-100">
                                {latestExecution.error_text}
                              </div>
                            ) : null}
                            <button
                              onClick={() => rollbackExecution(latestExecution.id)}
                              disabled={busy || !latestExecution?.id}
                              className={`${BTN} w-full border border-rose-400/30 bg-rose-400/10 text-rose-100 hover:bg-rose-400/20 disabled:opacity-50`}
                            >
                              Rollback desta execution
                            </button>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>

                  <div className="grid gap-5 xl:grid-cols-2">
                    <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                      <div className="text-xs uppercase tracking-[0.18em] text-white/40">Issue JSON</div>
                      <pre className="mt-3 overflow-auto rounded-2xl bg-black/30 p-4 text-xs text-white/75">
                        {JSON.stringify(selected.issue || {}, null, 2)}
                      </pre>
                    </div>
                    <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                      <div className="text-xs uppercase tracking-[0.18em] text-white/40">Decision JSON</div>
                      <pre className="mt-3 overflow-auto rounded-2xl bg-black/30 p-4 text-xs text-white/75">
                        {JSON.stringify(selected.decision || {}, null, 2)}
                      </pre>
                    </div>
                    <div className="rounded-2xl border border-white/10 bg-black/20 p-4 xl:col-span-2">
                      <div className="text-xs uppercase tracking-[0.18em] text-white/40">Finding JSON</div>
                      <pre className="mt-3 overflow-auto rounded-2xl bg-black/30 p-4 text-xs text-white/75">
                        {JSON.stringify(selected.finding || {}, null, 2)}
                      </pre>
                    </div>
                  </div>
                </div>
              )}
            </section>

            <section className={CARD}>
              <SectionTitle title="Histórico de executions" subtitle="Trilha auditável das tentativas de execução governada." />
              <div className="mt-4 space-y-3">
                {executions.length ? executions.map((item) => (
                  <div key={item.id} className="rounded-2xl border border-white/10 bg-black/20 p-4">
                    <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                      <div className="min-w-0">
                        <div className="text-sm font-semibold">{item.id}</div>
                        <div className="mt-1 text-xs text-white/50">trace_id: {item.trace_id || "-"}</div>
                      </div>
                      <div className="flex flex-wrap gap-2">
                        <Tag tone={toneForStatus(item.status)}>{item.status || "-"}</Tag>
                        <Tag>{item.mode || "-"}</Tag>
                        <Tag>{fmtTs(item.created_at)}</Tag>
                      </div>
                    </div>
                    <div className="mt-3 grid gap-2 text-sm text-white/75 md:grid-cols-2">
                      <div><span className="text-white/45">Actor:</span> {item.actor_ref || "-"}</div>
                      <div><span className="text-white/45">Started:</span> {fmtTs(item.started_at)}</div>
                      <div><span className="text-white/45">Completed:</span> {fmtTs(item.completed_at)}</div>
                      <div><span className="text-white/45">Rollback bundle:</span> {item?.result?.rollback_bundle ? "sim" : "não"}</div>
                      <div><span className="text-white/45">Post semantic integrity:</span> {item?.result?.post_validation?.semantic_integrity?.ok ? "ok" : (item?.result?.post_validation?.semantic_integrity ? "atenção" : "-")}</div>
                      <div><span className="text-white/45">Integrity severity:</span> {item?.result?.post_validation?.semantic_integrity?.severity || "-"}</div>
                      <div><span className="text-white/45">Blocked domains:</span> {Array.isArray(item?.result?.post_validation?.semantic_integrity?.blocked_domains) && item.result.post_validation.semantic_integrity.blocked_domains.length ? item.result.post_validation.semantic_integrity.blocked_domains.join(", ") : "-"}</div>
                      <div><span className="text-white/45">Runtime reasons:</span> {Array.isArray(item?.result?.post_validation?.semantic_integrity?.checks?.runtime?.reasons) && item.result.post_validation.semantic_integrity.checks.runtime.reasons.length ? item.result.post_validation.semantic_integrity.checks.runtime.reasons.join(", ") : "-"}</div>
                    </div>
                    {item.error_text ? (
                      <div className="mt-3 rounded-2xl border border-rose-400/20 bg-rose-400/10 px-3 py-2 text-sm text-rose-100">
                        {item.error_text}
                      </div>
                    ) : null}
                    <details className="mt-3 rounded-2xl border border-white/10 bg-black/30 p-3">
                      <summary className="cursor-pointer text-sm font-semibold text-white/80">Result JSON</summary>
                      <pre className="mt-3 overflow-auto text-xs text-white/70">{JSON.stringify(item.result || {}, null, 2)}</pre>
                    </details>
                  </div>
                )) : (
                  <div className="rounded-2xl border border-dashed border-white/10 px-4 py-8 text-sm text-white/55">
                    Nenhuma execution registrada para a proposal atual.
                  </div>
                )}
              </div>
            </section>

            <section className={`${CARD}`}>
              <SectionTitle title="Memória de sinais" subtitle="Snapshots persistidos de recommendation, recorrência e cadência por proposal." />
              <div className="mt-4 space-y-3">
                {signalHistory.length ? signalHistory.map((item) => (
                  <div key={item.id} className="rounded-2xl border border-white/10 bg-black/20 p-4">
                    <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                      <div className="min-w-0">
                        <div className="text-sm font-semibold">{fmtTs(item.created_at)}</div>
                        <div className="mt-1 text-xs text-white/50">trace_id: {item.trace_id || "-"}</div>
                      </div>
                      <div className="flex flex-wrap gap-2">
                        <Tag tone={scoreTone(item.priority_score)}>{item.priority_score}</Tag>
                        <Tag>{item.recommendation || "-"}</Tag>
                        <Tag tone={adminRecommendationTone(item.admin_recommendation)}>{adminRecommendationLabel(item.admin_recommendation)}</Tag>
                        <Tag tone={operatorSuggestionTone(item.operator_suggestion)}>{operatorSuggestionLabel(item.operator_suggestion)}</Tag>
                        <Tag>{item.operator_confidence_score ?? 0}%</Tag>
                        <Tag tone={trendTone(item.trend_state)}>{trendLabel(item.trend_state)}</Tag>
                        <Tag>{item.cadence_seconds || "-"}s</Tag>
                      </div>
                    </div>
                    <div className="mt-3 grid gap-2 text-sm text-white/75 md:grid-cols-2">
                      <div><span className="text-white/45">Domínio:</span> {item.domain_scope || "-"}</div>
                      <div><span className="text-white/45">Recorrência:</span> {item.recurrence_window_count || 1}</div>
                      <div><span className="text-white/45">Blast radius:</span> {item.blast_radius_score || 0}</div>
                      <div><span className="text-white/45">Segurança:</span> {item.security_score || 0}</div>
                      <div><span className="text-white/45">Trend delta:</span> {item.trend_delta ?? 0}</div>
                      <div><span className="text-white/45">Execute candidate:</span> {item.execute_candidate ? "sim" : "não"}</div>
                    </div>
                    <details className="mt-3 rounded-2xl border border-white/10 bg-black/30 p-3">
                      <summary className="cursor-pointer text-sm font-semibold text-white/80">Payload do snapshot</summary>
                      <pre className="mt-3 overflow-auto text-xs text-white/70">{JSON.stringify(item.payload || {}, null, 2)}</pre>
                    </details>
                  </div>
                )) : (
                  <div className="rounded-2xl border border-dashed border-white/10 px-4 py-8 text-sm text-white/55">
                    Ainda não há snapshots persistidos para a proposal atual.
                  </div>
                )}
              </div>
            </section>
          </main>
        </div>
      </div>
    </div>
  );
}
