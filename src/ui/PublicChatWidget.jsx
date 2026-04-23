
import React, { useEffect, useMemo, useRef, useState } from "react";
import { publicChat } from "./api.js";
import OrkioSphereMark from "./OrkioSphereMark.jsx";

function readLead() {
  try {
    const raw = localStorage.getItem("orkio_lead");
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

export default function PublicChatWidget({ autoOpen = false }) {
  const ENV =
    typeof window !== "undefined" && window.__ORKIO_ENV__ ? window.__ORKIO_ENV__ : {};
  const WHATSAPP_PHONE_E164 = String(ENV.WHATSAPP_PHONE_E164 || "").replace(/\D/g, "");

  const [open, setOpen] = useState(false);
  const [lead, setLead] = useState(() => readLead());
  const [threadId, setThreadId] = useState(null);
  const [msg, setMsg] = useState("");
  const [busy, setBusy] = useState(false);
  const [items, setItems] = useState([]);
  const boxRef = useRef(null);

  const hasLead = !!lead?.lead_id;

  const greet = useMemo(() => {
    const name = lead?.name || "there";
    const company = lead?.company || "your company";
    const role = lead?.role ? ` as ${lead.role}` : "";
    return `Hi ${name}. I’m Orkio.\n\nI see you’re with ${company}${role}.\nTell me the outcome you want AI to deliver with safety, clarity and measurable value.`;
  }, [lead]);

  useEffect(() => {
    if (autoOpen) setOpen(true);
  }, [autoOpen]);

  useEffect(() => {
    if (!open) return;
    setLead(readLead());
  }, [open]);

  useEffect(() => {
    if (!open) return;
    if (!items.length) setItems([{ role: "assistant", text: greet }]);
  }, [open, items.length, greet]);

  useEffect(() => {
    if (boxRef.current) boxRef.current.scrollTop = boxRef.current.scrollHeight;
  }, [items, open]);

  async function send(text) {
    const clean = String(text || "").trim();
    if (!clean) return;

    if (!hasLead) {
      setItems((x) => [
        ...x,
        {
          role: "assistant",
          text: "To continue, create your access first in the secure signup flow.",
        },
      ]);
      return;
    }

    setItems((x) => [...x, { role: "user", text: clean }]);
    setBusy(true);

    try {
      const r = await publicChat({
        lead_id: lead.lead_id,
        message: clean,
        thread_id: threadId,
      });

      if (r?.ok) {
        setThreadId(r.thread_id || null);
        setItems((x) => [...x, { role: "assistant", text: r.reply || "—" }]);
      } else {
        setItems((x) => [...x, { role: "assistant", text: "Something went wrong. Try again?" }]);
      }
    } catch (err) {
      setItems((x) => [...x, { role: "assistant", text: err?.message || "Network issue. Try again?" }]);
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <button
        onClick={() => setOpen(true)}
        className="fixed bottom-5 right-5 z-40 flex items-center gap-3 rounded-[22px] border border-white/12 bg-[#0c1120]/92 px-4 py-3 font-extrabold shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl transition hover:-translate-y-0.5 hover:bg-[#10172a]"
        aria-label="Talk to Orkio"
      >
        <OrkioSphereMark size={26} badge />
        <div className="text-left">
          <div className="text-sm leading-none">Talk to Orkio</div>
          <div className="mt-1 text-[11px] font-semibold tracking-[0.18em] text-white/45 uppercase">Live concierge</div>
        </div>
      </button>

      {open ? (
        <div className="fixed bottom-5 right-5 z-50 w-[min(470px,calc(100vw-32px))] overflow-hidden rounded-[30px] border border-white/10 bg-[#070c18]/96 shadow-[0_25px_90px_rgba(0,0,0,0.7)] backdrop-blur-2xl">
          <div className="border-b border-white/8 bg-[radial-gradient(420px_120px_at_10%_0%,rgba(34,211,238,0.14),transparent_55%),radial-gradient(420px_160px_at_90%_0%,rgba(124,58,237,0.18),transparent_58%),rgba(255,255,255,0.02)] px-4 py-4">
            <div className="flex items-start justify-between gap-3">
              <div className="flex items-center gap-3">
                <OrkioSphereMark size={44} badge={hasLead} />
                <div>
                  <div className="text-sm font-black">Orkio concierge</div>
                  <div className="mt-1 text-xs text-white/60">
                    {hasLead ? "Qualified lead recognized" : "Secure signup required to continue"}
                  </div>
                </div>
              </div>

              <div className="flex items-center gap-2">
                {hasLead && WHATSAPP_PHONE_E164 ? (
                  <button
                    type="button"
                    onClick={() => {
                      const name = encodeURIComponent(lead?.name || "");
                      const email = encodeURIComponent(lead?.email || "");
                      const company = encodeURIComponent(lead?.company || "");
                      const role = encodeURIComponent(lead?.role || "");
                      const segment = encodeURIComponent(lead?.segment || "");
                      const leadId = encodeURIComponent(lead?.lead_id || "");
                      const message =
                        `New ORKIO lead (ID ${leadId})%0A` +
                        `Name: ${name}%0A` +
                        `Email: ${email}%0A` +
                        `Company: ${company}%0A` +
                        `Role: ${role}%0A` +
                        `Segment: ${segment}%0A%0A` +
                        `Orkio: Please book a demo.`;

                      window.open(`https://wa.me/${WHATSAPP_PHONE_E164}?text=${message}`, "_blank", "noopener,noreferrer");
                    }}
                    className="rounded-xl border border-emerald-400/25 bg-emerald-500/10 px-3 py-2 text-[11px] font-bold uppercase tracking-[0.16em] text-emerald-100 hover:bg-emerald-500/15"
                    title="Send lead to WhatsApp"
                  >
                    WhatsApp
                  </button>
                ) : null}

                <button
                  onClick={() => setOpen(false)}
                  className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-[11px] font-bold uppercase tracking-[0.16em] text-white/80 hover:bg-white/10"
                >
                  Close
                </button>
              </div>
            </div>
          </div>

          <div ref={boxRef} className="max-h-[430px] overflow-auto px-4 py-4">
            {items.map((it, idx) => (
              <div key={idx} className={`my-3 flex ${it.role === "user" ? "justify-end" : "justify-start"}`}>
                <div
                  className={`max-w-[88%] whitespace-pre-wrap rounded-[24px] border px-4 py-3 text-sm leading-relaxed ${
                    it.role === "user"
                      ? "border-cyan-300/20 bg-cyan-300/12 text-white"
                      : "border-white/8 bg-white/[0.04] text-white/92"
                  }`}
                >
                  {it.text}
                </div>
              </div>
            ))}

            {busy ? (
              <div className="my-3 flex justify-start">
                <div className="max-w-[88%] rounded-[24px] border border-white/8 bg-white/[0.04] px-4 py-3 text-sm text-white/75">
                  Orkio is composing the next move…
                </div>
              </div>
            ) : null}
          </div>

          <form
            onSubmit={(e) => {
              e.preventDefault();
              const current = msg;
              setMsg("");
              send(current);
            }}
            className="border-t border-white/8 bg-white/[0.02] px-3 py-3"
          >
            <div className="flex gap-2">
              <input
                value={msg}
                onChange={(e) => setMsg(e.target.value)}
                placeholder={hasLead ? "Describe the result you want…" : "Create your access first"}
                className="flex-1 rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-sm text-white outline-none placeholder:text-white/35 focus:border-cyan-300/20"
              />
              <button
                disabled={busy}
                className="rounded-2xl bg-gradient-to-r from-cyan-300 via-sky-400 to-violet-500 px-4 py-3 text-sm font-black text-slate-950 shadow-[0_18px_40px_rgba(96,165,250,0.18)] transition hover:brightness-110 disabled:opacity-60"
              >
                Send
              </button>
            </div>
            {!hasLead ? (
              <div className="mt-3 text-xs text-white/52">
                The public concierge is available after the secure signup flow.
              </div>
            ) : null}
          </form>
        </div>
      ) : null}
    </>
  );
}
