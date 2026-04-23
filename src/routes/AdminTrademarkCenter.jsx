
import React, { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { apiFetch } from "../ui/api.js";
import { getTenant, getToken, getUser, isAdmin } from "../lib/auth.js";

const CARD = "rounded-3xl border border-white/10 bg-white/5 backdrop-blur p-5";
const INPUT = "w-full rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-sm text-white outline-none placeholder:text-white/35";
const BTN = "rounded-2xl px-4 py-3 text-sm font-semibold";
const SMALL = "rounded-xl px-3 py-2 text-xs font-semibold";

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

function fmt(ts) {
  if (!ts) return "-";
  try {
    return new Date(ts * 1000).toLocaleString("pt-BR");
  } catch {
    return "-";
  }
}

function Tag({ children, tone = "default" }) {
  const toneCls =
    tone === "high"
      ? "border-rose-400/30 bg-rose-400/10 text-rose-100"
      : tone === "medium"
      ? "border-amber-400/30 bg-amber-400/10 text-amber-100"
      : tone === "good"
      ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-100"
      : "border-white/10 bg-white/5 text-white/80";
  return <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs ${toneCls}`}>{children}</span>;
}

export default function AdminTrademarkCenter() {
  const nav = useNavigate();
  const tenant = getTenant() || "public";
  const token = getToken();
  const user = getUser();

  const [summary, setSummary] = useState(null);
  const [items, setItems] = useState([]);
  const [selectedId, setSelectedId] = useState("");
  const [detail, setDetail] = useState(null);
  const [preview, setPreview] = useState(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [note, setNote] = useState("");

  const [form, setForm] = useState({
    mark_name: "",
    applicant_name: "",
    applicant_country: "IT",
    contact_email: "",
    goods_services_text:
      "Software as a service (SaaS), artificial intelligence platforms, multi-agent orchestration, workflow automation and strategic planning.",
    jurisdictions: "BR,EU,UK",
    nice_classes: "42,9,35",
    notes: "",
    filing_mode: "assisted",
  });

  useEffect(() => {
    if (!token) nav("/auth");
    else if (!isAdmin(user)) nav("/app");
  }, [token, user, nav]);

  async function loadSummary() {
    const res = await apiFetch("/api/admin/trademarks/summary", { org: tenant, token });
    setSummary(res?.data ?? res);
  }

  async function loadItems() {
    const res = await apiFetch("/api/admin/trademarks", { org: tenant, token });
    const payload = res?.data ?? res;
    setItems(Array.isArray(payload) ? payload : []);
  }

  async function loadDetail(id) {
    if (!id) return;
    const res = await apiFetch(`/api/admin/trademarks/${id}`, { org: tenant, token });
    setDetail(res?.data ?? res);
    setSelectedId(id);
  }

  async function reloadAll(pickLatest = false) {
    setBusy(true);
    setErr("");
    try {
      await Promise.all([loadSummary(), loadItems()]);
      if (pickLatest) {
        const res = await apiFetch("/api/admin/trademarks", { org: tenant, token });
        const payload = res?.data ?? res;
        const list = Array.isArray(payload) ? payload : [];
        setItems(list);
        if (list[0]?.id) {
          await loadDetail(list[0].id);
        }
      } else if (selectedId) {
        await loadDetail(selectedId);
      }
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    reloadAll();
  }, []);

  async function handlePreview() {
    setBusy(true);
    setErr("");
    try {
      const res = await apiFetch("/api/admin/trademarks/preview", {
        method: "POST",
        org: tenant,
        token,
        body: {
          ...form,
          jurisdictions: form.jurisdictions.split(",").map((v) => v.trim()).filter(Boolean),
          nice_classes: form.nice_classes.split(",").map((v) => v.trim()).filter(Boolean),
          contact_email: form.contact_email || null,
        },
      });
      setPreview(res?.data ?? res);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function handleCreate() {
    setBusy(true);
    setErr("");
    try {
      const res = await apiFetch("/api/admin/trademarks", {
        method: "POST",
        org: tenant,
        token,
        body: {
          ...form,
          jurisdictions: form.jurisdictions.split(",").map((v) => v.trim()).filter(Boolean),
          nice_classes: form.nice_classes.split(",").map((v) => v.trim()).filter(Boolean),
          contact_email: form.contact_email || null,
        },
      });
      const payload = res?.data ?? res;
      setPreview(payload?.preview || null);
      await reloadAll(true);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function handleRescreen() {
    if (!selectedId) return;
    setBusy(true);
    setErr("");
    try {
      const res = await apiFetch(`/api/admin/trademarks/${selectedId}/rescreen`, {
        method: "POST",
        org: tenant,
        token,
      });
      setPreview((res?.data ?? res)?.preview || null);
      await reloadAll();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function handleApproval(approval_status) {
    if (!selectedId) return;
    setBusy(true);
    setErr("");
    try {
      await apiFetch(`/api/admin/trademarks/${selectedId}`, {
        method: "PUT",
        org: tenant,
        token,
        body: { approval_status },
      });
      await reloadAll();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function handleAddNote() {
    if (!selectedId || !note.trim()) return;
    setBusy(true);
    setErr("");
    try {
      await apiFetch(`/api/admin/trademarks/${selectedId}/events`, {
        method: "POST",
        org: tenant,
        token,
        body: {
          event_type: "note.added",
          payload: { note: note.trim() },
        },
      });
      setNote("");
      await reloadAll();
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  const selectedRiskTone = (detail?.risk_level || preview?.risk_level || "").toLowerCase();
  const currentConnectors = preview?.search_connectors || detail?.external_screening?.search_connectors || [];
  const currentLinks = preview?.official_links || detail?.external_screening?.official_links || {};
  const currentConflicts = preview?.internal_conflicts || detail?.internal_conflicts || [];
  const currentDossier = preview?.dossier || detail?.dossier || {};

  const summaryCards = useMemo(() => summary || { total: 0, pending: 0, approved: 0, high_risk: 0, jurisdictions: [] }, [summary]);

  return (
    <div className="min-h-screen bg-[#060816] text-white">
      <div className="mx-auto max-w-7xl px-4 py-8">
        <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
          <div>
            <button onClick={() => nav("/admin")} className="text-sm text-cyan-300 hover:text-cyan-200">
              ← Voltar ao Admin
            </button>
            <h1 className="mt-2 text-3xl font-black">Trademark Center</h1>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-white/65">
              Intake, triagem internacional, classes, jurisdições, dossiê e aprovação humana antes de filing.
            </p>
          </div>

          <div className="flex gap-2">
            <button onClick={handlePreview} disabled={busy} className={`${BTN} border border-white/10 bg-white/5 hover:bg-white/10 disabled:opacity-50`}>
              Pré-analisar
            </button>
            <button onClick={handleCreate} disabled={busy} className={`${BTN} bg-gradient-to-r from-violet-500 to-cyan-400 text-black hover:brightness-110 disabled:opacity-50`}>
              Criar matter
            </button>
          </div>
        </div>

        {err ? (
          <div className="mb-5 rounded-2xl border border-rose-400/20 bg-rose-400/10 p-4 text-sm text-rose-100">
            ⚠️ {err}
          </div>
        ) : null}

        <div className="grid gap-4 md:grid-cols-4">
          <Stat label="Matters" value={summaryCards.total} />
          <Stat label="Pendentes" value={summaryCards.pending} tone={summaryCards.pending ? "warn" : "default"} />
          <Stat label="Aprovadas" value={summaryCards.approved} tone={summaryCards.approved ? "good" : "default"} />
          <Stat label="Risco alto" value={summaryCards.high_risk} tone={summaryCards.high_risk ? "warn" : "default"} />
        </div>

        <div className="mt-6 grid gap-6 xl:grid-cols-[1.15fr_0.85fr]">
          <div className={CARD}>
            <div className="mb-4 flex items-center justify-between">
              <h2 className="text-lg font-bold">Nova marca / screening</h2>
              <Tag>{tenant}</Tag>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-white/50">Marca</label>
                <input className={INPUT} value={form.mark_name} onChange={(e) => setForm((s) => ({ ...s, mark_name: e.target.value }))} placeholder="ORKIO" />
              </div>
              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-white/50">Titular / requerente</label>
                <input className={INPUT} value={form.applicant_name} onChange={(e) => setForm((s) => ({ ...s, applicant_name: e.target.value }))} placeholder="PatroAI Italia SRL" />
              </div>
              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-white/50">País do titular</label>
                <input className={INPUT} value={form.applicant_country} onChange={(e) => setForm((s) => ({ ...s, applicant_country: e.target.value }))} placeholder="IT" />
              </div>
              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-white/50">Contato</label>
                <input className={INPUT} value={form.contact_email} onChange={(e) => setForm((s) => ({ ...s, contact_email: e.target.value }))} placeholder="ip@patroai.com" />
              </div>
              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-white/50">Jurisdições</label>
                <input className={INPUT} value={form.jurisdictions} onChange={(e) => setForm((s) => ({ ...s, jurisdictions: e.target.value }))} placeholder="BR,EU,UK" />
              </div>
              <div>
                <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-white/50">Classes Nice</label>
                <input className={INPUT} value={form.nice_classes} onChange={(e) => setForm((s) => ({ ...s, nice_classes: e.target.value }))} placeholder="42,9,35" />
              </div>
            </div>

            <div className="mt-4">
              <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-white/50">Produtos / serviços</label>
              <textarea className={`${INPUT} min-h-[120px]`} value={form.goods_services_text} onChange={(e) => setForm((s) => ({ ...s, goods_services_text: e.target.value }))} />
            </div>

            <div className="mt-4">
              <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.18em] text-white/50">Notas internas</label>
              <textarea className={`${INPUT} min-h-[90px]`} value={form.notes} onChange={(e) => setForm((s) => ({ ...s, notes: e.target.value }))} placeholder="Prioridade, Europa, holding, Madrid, etc." />
            </div>

            {preview ? (
              <div className="mt-6 space-y-4 rounded-3xl border border-cyan-400/20 bg-cyan-400/5 p-4">
                <div className="flex flex-wrap items-center gap-2">
                  <Tag tone={selectedRiskTone === "high" ? "high" : selectedRiskTone === "medium" ? "medium" : "good"}>
                    risco {preview.risk_level} · {preview.risk_score}
                  </Tag>
                  <Tag>{preview.normalized_mark}</Tag>
                  {(preview.recommended_classes || []).map((c) => <Tag key={c}>classe {c}</Tag>)}
                  {(preview.recommended_jurisdictions || []).map((j) => <Tag key={j}>{j}</Tag>)}
                </div>

                <div>
                  <div className="text-sm font-semibold">Variantes para busca</div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {(preview.variants || []).map((v) => <Tag key={v}>{v}</Tag>)}
                  </div>
                </div>

                <div className="grid gap-4 md:grid-cols-2">
                  <div>
                    <div className="text-sm font-semibold">Conflitos internos</div>
                    <div className="mt-2 space-y-2">
                      {currentConflicts.length ? currentConflicts.map((hit) => (
                        <div key={`${hit.matter_id}-${hit.mark_name}`} className="rounded-2xl border border-white/10 bg-black/20 p-3 text-sm">
                          <div className="flex items-center justify-between gap-2">
                            <div className="font-semibold">{hit.mark_name}</div>
                            <Tag tone={hit.similarity >= 85 ? "high" : hit.similarity >= 70 ? "medium" : "default"}>{hit.similarity}% similar</Tag>
                          </div>
                          <div className="mt-1 text-white/55">matter {hit.matter_id} · {hit.status} · {hit.approval_status}</div>
                        </div>
                      )) : <div className="rounded-2xl border border-white/10 bg-black/20 p-3 text-sm text-white/55">Sem colisões internas relevantes.</div>}
                    </div>
                  </div>

                  <div>
                    <div className="text-sm font-semibold">Conectores oficiais</div>
                    <div className="mt-2 space-y-2">
                      {currentConnectors.map((item) => (
                        <a key={`${item.jurisdiction}-${item.search_url}`} href={item.search_url} target="_blank" rel="noreferrer" className="block rounded-2xl border border-white/10 bg-black/20 p-3 text-sm hover:bg-white/5">
                          <div className="flex items-center justify-between gap-2">
                            <div className="font-semibold">{item.jurisdiction}</div>
                            <Tag>{item.mode}</Tag>
                          </div>
                          <div className="mt-1 break-all text-white/60">{item.search_url}</div>
                          {item.hint ? <div className="mt-2 text-white/45">{item.hint}</div> : null}
                        </a>
                      ))}
                    </div>
                  </div>
                </div>

                <div>
                  <div className="text-sm font-semibold">Dossiê preliminar</div>
                  <div className="mt-2 rounded-2xl border border-white/10 bg-black/20 p-4 text-sm text-white/70">
                    <div>Prioridade: {(currentDossier.filing_priority || []).join(" → ") || "-"}</div>
                    <div className="mt-2">Próximos passos:</div>
                    <ul className="mt-2 list-disc space-y-1 pl-5">
                      {(currentDossier.next_steps || []).map((step, idx) => <li key={idx}>{step}</li>)}
                    </ul>
                  </div>
                </div>
              </div>
            ) : null}
          </div>

          <div className={`${CARD} flex flex-col`}>
            <div className="mb-4 flex items-center justify-between">
              <h2 className="text-lg font-bold">Pipeline de marcas</h2>
              <button onClick={() => reloadAll()} className={`${SMALL} border border-white/10 bg-white/5 hover:bg-white/10`}>Atualizar</button>
            </div>

            <div className="space-y-2 overflow-auto pr-1">
              {items.map((item) => (
                <button
                  key={item.id}
                  onClick={() => loadDetail(item.id)}
                  className={`w-full rounded-2xl border p-4 text-left ${selectedId === item.id ? "border-cyan-400/35 bg-cyan-400/10" : "border-white/10 bg-black/20 hover:bg-white/5"}`}
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="font-semibold">{item.mark_name}</div>
                    <Tag tone={(item.risk_level || "").toLowerCase() === "high" ? "high" : (item.risk_level || "").toLowerCase() === "medium" ? "medium" : "good"}>
                      {item.risk_level || "n/a"} · {item.risk_score ?? "-"}
                    </Tag>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    <Tag>{item.status}</Tag>
                    <Tag>{item.approval_status}</Tag>
                    {(item.jurisdictions || []).slice(0, 4).map((j) => <Tag key={j}>{j}</Tag>)}
                  </div>
                  <div className="mt-2 text-xs text-white/45">{fmt(item.updated_at)}</div>
                </button>
              ))}
              {!items.length ? (
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4 text-sm text-white/55">
                  Nenhum matter ainda.
                </div>
              ) : null}
            </div>
          </div>
        </div>

        {detail ? (
          <div className="mt-6 grid gap-6 xl:grid-cols-[1fr_0.9fr]">
            <div className={CARD}>
              <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                <div>
                  <div className="text-xs uppercase tracking-[0.18em] text-white/45">Matter</div>
                  <h2 className="text-2xl font-black">{detail.mark_name}</h2>
                </div>
                <div className="flex flex-wrap gap-2">
                  <button onClick={handleRescreen} className={`${SMALL} border border-white/10 bg-white/5 hover:bg-white/10`} disabled={busy}>Re-screen</button>
                  <button onClick={() => handleApproval("approved")} className={`${SMALL} bg-emerald-500/20 text-emerald-100 hover:bg-emerald-500/30`} disabled={busy}>Aprovar</button>
                  <button onClick={() => handleApproval("rejected")} className={`${SMALL} bg-rose-500/20 text-rose-100 hover:bg-rose-500/30`} disabled={busy}>Rejeitar</button>
                </div>
              </div>

              <div className="grid gap-4 md:grid-cols-2">
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <div className="text-xs uppercase tracking-[0.18em] text-white/45">Titular</div>
                  <div className="mt-2 text-sm">{detail.applicant_name || "-"}</div>
                  <div className="mt-3 text-xs uppercase tracking-[0.18em] text-white/45">País</div>
                  <div className="mt-2 text-sm">{detail.applicant_country || "-"}</div>
                  <div className="mt-3 text-xs uppercase tracking-[0.18em] text-white/45">Contato</div>
                  <div className="mt-2 text-sm">{detail.contact_email || "-"}</div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                  <div className="flex flex-wrap gap-2">
                    <Tag>{detail.status}</Tag>
                    <Tag>{detail.approval_status}</Tag>
                    <Tag tone={selectedRiskTone === "high" ? "high" : selectedRiskTone === "medium" ? "medium" : "good"}>
                      {detail.risk_level} · {detail.risk_score}
                    </Tag>
                  </div>
                  <div className="mt-4 text-sm text-white/65">Criado: {fmt(detail.created_at)}</div>
                  <div className="mt-1 text-sm text-white/65">Atualizado: {fmt(detail.updated_at)}</div>
                  <div className="mt-1 text-sm text-white/65">Aprovado em: {fmt(detail.approval_at)}</div>
                </div>
              </div>

              <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Classes e jurisdições</div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {(detail.classes || []).map((c) => <Tag key={c}>classe {c}</Tag>)}
                  {(detail.jurisdictions || []).map((j) => <Tag key={j}>{j}</Tag>)}
                </div>
              </div>

              <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Produtos / serviços</div>
                <div className="mt-3 whitespace-pre-wrap text-sm text-white/75">{detail.goods_services_text || "-"}</div>
              </div>

              <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Dossiê</div>
                <pre className="mt-3 overflow-auto text-xs leading-6 text-white/70">{JSON.stringify(detail.dossier || {}, null, 2)}</pre>
              </div>
            </div>

            <div className={CARD}>
              <h2 className="text-lg font-bold">Governança e trilha</h2>

              <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Adicionar nota</div>
                <textarea className={`${INPUT} mt-3 min-h-[100px]`} value={note} onChange={(e) => setNote(e.target.value)} placeholder="Decisão do founder, orientação jurídica, estratégia de filing..." />
                <div className="mt-3 flex justify-end">
                  <button onClick={handleAddNote} disabled={busy || !note.trim()} className={`${SMALL} bg-white/10 hover:bg-white/15 disabled:opacity-50`}>
                    Registrar evento
                  </button>
                </div>
              </div>

              <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Links oficiais</div>
                <div className="mt-3 space-y-2">
                  {Object.entries(currentLinks).map(([key, item]) => (
                    <a key={key} href={item.search_url} target="_blank" rel="noreferrer" className="block rounded-2xl border border-white/10 bg-black/30 p-3 text-sm hover:bg-white/5">
                      <div className="font-semibold">{item.label || key}</div>
                      <div className="mt-1 break-all text-white/55">{item.search_url}</div>
                    </a>
                  ))}
                </div>
              </div>

              <div className="mt-4 rounded-2xl border border-white/10 bg-black/20 p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-white/45">Eventos</div>
                <div className="mt-3 space-y-2">
                  {(detail.events || []).map((ev) => (
                    <div key={ev.id} className="rounded-2xl border border-white/10 bg-black/30 p-3">
                      <div className="flex items-center justify-between gap-2">
                        <div className="text-sm font-semibold">{ev.event_type}</div>
                        <div className="text-xs text-white/45">{fmt(ev.created_at)}</div>
                      </div>
                      <pre className="mt-2 overflow-auto text-xs text-white/65">{JSON.stringify(ev.payload || {}, null, 2)}</pre>
                    </div>
                  ))}
                  {!detail.events?.length ? <div className="text-sm text-white/55">Sem eventos ainda.</div> : null}
                </div>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
