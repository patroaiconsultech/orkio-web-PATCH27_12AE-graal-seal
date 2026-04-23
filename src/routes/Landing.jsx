
import React, { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { getToken, getUser, isAdmin } from "../lib/auth.js";
import { getPublicPlans } from "../ui/api.js";
import PublicChatWidget from "../ui/PublicChatWidget.jsx";
import Footer from "../ui/Footer.jsx";
import OrkioSphereMark from "../ui/OrkioSphereMark.jsx";
import PricingCheckoutModal from "../ui/PricingCheckoutModal.jsx";

const fallbackPlans = [
  {
    code: "founder_access",
    name: "Founder Access",
    price_amount: 20,
    display_currency: "USD",
    badge: "Best entry",
    summary: "Subscription leve com wallet incluída e cobrança adicional por uso real.",
    included_credit_usd: 20,
    features: [
      "US$ 20 em créditos inclusos por mês",
      "Wallet com saldo acumulativo",
      "Ideal para founders e consultores",
    ],
  },
  {
    code: "pro_access",
    name: "Pro Access",
    price_amount: 49,
    display_currency: "USD",
    badge: "Power users",
    summary: "Mais throughput e créditos maiores para quem usa o Orkio de verdade.",
    included_credit_usd: 60,
    features: [
      "US$ 60 em créditos inclusos por mês",
      "Mais execuções e documentos",
      "Melhor economia para uso intensivo",
    ],
  },
  {
    code: "team_access",
    name: "Team Access",
    price_amount: 149,
    display_currency: "USD",
    badge: "Shared wallet",
    summary: "Base mensal com pool compartilhado de créditos e expansão por seat.",
    included_credit_usd: 180,
    features: [
      "US$ 180 em créditos inclusos por mês",
      "Pool compartilhado de wallet",
      "US$ 12 por assento adicional",
    ],
  },
];


function normalizePlansPayload(raw) {
  const payload =
    raw?.data?.plans ||
    raw?.plans ||
    raw?.data ||
    (Array.isArray(raw) ? raw : null);

  if (!Array.isArray(payload) || !payload.length) return fallbackPlans;

  const normalized = payload.map((item, index) => ({
    code: String(item.code || item.plan_code || item.slug || `plan_${index + 1}`),
    name: String(item.name || item.title || item.label || `Plano ${index + 1}`),
    price_amount: Number(item.price_amount ?? item.price_usd ?? item.amount ?? item.price ?? 0),
    display_currency: String(item.display_currency || item.currency || "USD"),
    currency: String(item.currency || item.display_currency || "USD"),
    period_label: item.period_label || item.period || "/mês",
    badge:
      item.badge ||
      (index === 0 ? "Mais rápido para vender" : index === 1 ? "Mais alavancado" : "Plano"),
    summary:
      item.summary ||
      item.description ||
      "Subscription + wallet + usage billing para operar sem overselling.",
    features: Array.isArray(item.features) && item.features.length
      ? item.features
      : [
          "Acesso governado ao Orkio",
          "Entrada comercial segura",
          "Ativação por checkout",
        ],
  }));

  return normalized.length ? normalized : fallbackPlans;
}

function formatMoney(amount, currency = "BRL") {
  if (!Number(amount)) return "Sob proposta";
  try {
    return new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency,
      maximumFractionDigits: Number(amount) % 1 === 0 ? 0 : 2,
    }).format(Number(amount));
  } catch {
    return `${currency} ${amount}`;
  }
}

function VoiceOrb() {
  const speak = () => {
    if (typeof window === "undefined" || !("speechSynthesis" in window)) return;
    window.speechSynthesis.cancel();
    const u = new SpeechSynthesisUtterance(
      "Bem-vindo ao Orkio. Inteligência executiva, governança e execução com rastreabilidade total."
    );
    u.lang = "pt-BR";
    u.rate = 0.96;
    u.pitch = 1.02;
    const voices = window.speechSynthesis.getVoices();
    const ptVoice = voices.find((v) => String(v.lang || "").toLowerCase().startsWith("pt"));
    if (ptVoice) u.voice = ptVoice;
    window.speechSynthesis.speak(u);
  };

  return (
    <button
      type="button"
      onClick={speak}
      className="group relative inline-flex items-center justify-center rounded-full p-4 transition hover:scale-[1.02]"
      title="Ouvir Orkio"
    >
      <span className="absolute inset-0 rounded-full bg-cyan-300/10 blur-2xl transition group-hover:bg-cyan-300/20" />
      <OrkioSphereMark size={170} ring glow />
      <span className="absolute -bottom-7 left-1/2 -translate-x-1/2 whitespace-nowrap text-xs font-semibold uppercase tracking-[0.22em] text-white/45">
        toque para ouvir
      </span>
    </button>
  );
}

function StatCard({ label, value, caption }) {
  return (
    <div className="rounded-[28px] border border-white/10 bg-white/[0.04] p-5 backdrop-blur">
      <div className="text-[11px] font-black uppercase tracking-[0.24em] text-white/42">{label}</div>
      <div className="mt-3 text-2xl font-black tracking-tight text-white">{value}</div>
      <div className="mt-2 text-sm leading-6 text-white/62">{caption}</div>
    </div>
  );
}

function PriceCard({ plan, tone = "subtle", onChoose }) {
  const toneCls =
    tone === "featured"
      ? "border-cyan-300/25 bg-[linear-gradient(135deg,rgba(56,189,248,0.14),rgba(124,58,237,0.18),rgba(255,255,255,0.06))] shadow-[0_24px_70px_rgba(14,165,233,0.16)]"
      : "border-white/10 bg-white/[0.04]";
  const price = Number(plan.price_amount) ? formatMoney(plan.price_amount, plan.currency) : "Sob proposta";

  return (
    <div className={`rounded-[30px] border p-6 backdrop-blur ${toneCls}`}>
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <OrkioSphereMark size={30} badge={tone === "featured"} />
          <div>
            <div className="text-sm font-black">{plan.name}</div>
            <div className="text-xs uppercase tracking-[0.18em] text-white/48">PWA early access</div>
          </div>
        </div>
        <div className="rounded-full border border-white/10 bg-white/[0.04] px-3 py-1 text-[10px] font-black uppercase tracking-[0.18em] text-white/58">
          {plan.badge}
        </div>
      </div>
      <div className="mt-6 text-4xl font-black tracking-tight">{price}</div>
      <div className="mt-1 text-xs uppercase tracking-[0.18em] text-white/44">{plan.period_label || "custom"}</div>
      <div className="mt-3 text-sm leading-6 text-white/62">{plan.summary}</div>
      <div className="mt-6 space-y-3">
        {(plan.features || []).map((item) => (
          <div key={item} className="flex items-start gap-3 text-sm text-white/82">
            <span className="mt-1 h-2.5 w-2.5 rounded-full bg-gradient-to-r from-cyan-300 to-violet-500" />
            <span>{item}</span>
          </div>
        ))}
      </div>
      <button
        type="button"
        onClick={() => onChoose?.(plan)}
        className={`mt-7 w-full rounded-2xl px-4 py-3 text-sm font-black transition ${
          tone === "featured"
            ? "bg-gradient-to-r from-cyan-300 via-sky-400 to-violet-500 text-slate-950 hover:brightness-110"
            : "border border-white/10 bg-white/5 text-white hover:bg-white/10"
        }`}
      >
        {plan.code === "enterprise" ? "Solicitar proposta" : "Ativar agora"}
      </button>
    </div>
  );
}


function TrustChip({ title, subtitle }) {
  return (
    <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-3 backdrop-blur">
      <div className="text-[11px] font-black uppercase tracking-[0.22em] text-white/40">{title}</div>
      <div className="mt-1 text-sm font-semibold text-white/80">{subtitle}</div>
    </div>
  );
}

function ProofCard({ eyebrow, title, copy, bullets = [] }) {
  return (
    <div className="rounded-[30px] border border-white/10 bg-white/[0.04] p-6 backdrop-blur">
      <div className="text-[11px] font-black uppercase tracking-[0.24em] text-white/42">{eyebrow}</div>
      <div className="mt-4 text-2xl font-black tracking-tight text-white">{title}</div>
      <div className="mt-4 text-sm leading-7 text-white/66">{copy}</div>
      {bullets.length ? (
        <div className="mt-6 space-y-3">
          {bullets.map((item) => (
            <div key={item} className="flex items-start gap-3 rounded-2xl border border-white/8 bg-black/20 p-4 text-sm leading-6 text-white/76">
              <span className="mt-2 h-2.5 w-2.5 rounded-full bg-gradient-to-r from-cyan-300 to-violet-500" />
              <span>{item}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export default function Landing() {
  const nav = useNavigate();
  const token = getToken();
  const user = getUser();

  const isLogged = !!token;
  const userIsAdmin = isLogged && isAdmin(user);

  const [plans, setPlans] = useState(fallbackPlans);
  const [plansLoaded, setPlansLoaded] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [selectedPlanCode, setSelectedPlanCode] = useState("founder_access");

  useEffect(() => {
    let active = true;
    getPublicPlans()
      .then((res) => {
        if (!active) return;
        const normalized = normalizePlansPayload(res);
        setPlans(normalized);
        const preferred = normalized.find((p) => p.code === "pro_access")
          ? "pro_access"
          : normalized[0]?.code || "founder_access";
        setSelectedPlanCode(preferred);
      })
      .catch(() => {})
      .finally(() => {
        if (active) setPlansLoaded(true);
      });
    return () => {
      active = false;
    };
  }, []);

  const pricingPlans = useMemo(() => {
    const output = plans.slice(0, 3);
    return output.length ? output : fallbackPlans;
  }, [plans]);

  function openCheckout(plan) {
    if (plan?.code === "enterprise_contact") {
      nav("/contact?topic=enterprise");
      return;
    }
    setSelectedPlanCode(plan?.code || "founder_access");
    setModalOpen(true);
  }

  return (
    <div className="min-h-screen overflow-x-hidden bg-[#060812] text-white">
      <div className="pointer-events-none fixed inset-0 -z-10">
        <div className="absolute inset-0 bg-[radial-gradient(900px_500px_at_14%_8%,rgba(34,211,238,0.14),transparent_60%),radial-gradient(1000px_560px_at_86%_10%,rgba(124,58,237,0.18),transparent_62%),linear-gradient(180deg,#050812,#060812)]" />
        <div className="absolute inset-0 opacity-30 bg-[linear-gradient(rgba(255,255,255,0.03)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.03)_1px,transparent_1px)] bg-[size:32px_32px]" />
      </div>

      <header className="sticky top-0 z-30 border-b border-white/8 bg-[#060812]/76 backdrop-blur-xl">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-4">
          <div className="flex items-center gap-3">
            <OrkioSphereMark size={34} badge />
            <div>
              <div className="text-[11px] font-black uppercase tracking-[0.28em] text-white/42">Orkio</div>
              <div className="text-sm font-semibold text-white/88">Execution intelligence for serious operators</div>
            </div>
          </div>

          <nav className="hidden items-center gap-2 md:flex">
            <a className="rounded-xl px-3 py-2 text-sm text-white/70 hover:bg-white/5 hover:text-white" href="#outcomes">
              Outcomes
            </a>
            <a className="rounded-xl px-3 py-2 text-sm text-white/70 hover:bg-white/5 hover:text-white" href="#architecture">
              Architecture
            </a>
            <a className="rounded-xl px-3 py-2 text-sm text-white/70 hover:bg-white/5 hover:text-white" href="#pricing">
              Pricing
            </a>
            <a className="rounded-xl px-3 py-2 text-sm text-white/70 hover:bg-white/5 hover:text-white" href="#proof">
              Proof
            </a>
            <a className="rounded-xl px-3 py-2 text-sm text-white/70 hover:bg-white/5 hover:text-white" href="#security">
              Security
            </a>
          </nav>

          <div className="flex items-center gap-2">
            <button
              onClick={() => nav(isLogged ? "/app" : "/auth?mode=login")}
              className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold hover:bg-white/10"
            >
              {isLogged ? "Open app" : "Sign in"}
            </button>
            {userIsAdmin ? (
              <button
                onClick={() => nav("/admin")}
                className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold hover:bg-white/10"
              >
                Admin
              </button>
            ) : null}
            <button
              onClick={() => setModalOpen(true)}
              className="rounded-xl bg-gradient-to-r from-cyan-300 via-sky-400 to-violet-500 px-4 py-2 text-sm font-black text-slate-950 hover:brightness-110"
            >
              Start now
            </button>
          </div>
        </div>
      </header>

      <section className="mx-auto grid max-w-7xl gap-12 px-4 pb-16 pt-14 lg:grid-cols-[minmax(0,1.1fr)_minmax(460px,520px)] lg:items-center">
        <div>
          <div className="inline-flex items-center gap-3 rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-xs font-black uppercase tracking-[0.24em] text-white/56">
            <OrkioSphereMark size={18} />
            PWA launch • governed autonomy • billing-ready
          </div>

          <h1 className="mt-7 max-w-4xl text-5xl font-black leading-[0.96] tracking-tight md:text-7xl">
            A plataforma que transforma
            <span className="bg-gradient-to-r from-cyan-300 via-sky-400 to-violet-500 bg-clip-text text-transparent"> estratégia em execução</span>.
          </h1>

          <p className="mt-6 max-w-2xl text-lg leading-8 text-white/68">
            Orkio organiza intenção, aciona inteligência, coordena agentes e mantém rastreabilidade operacional.
            É a camada de execução da sua empresa — com design premium, governança e velocidade de produto.
          </p>

          <div className="mt-8 flex flex-wrap gap-3">
            <button
              onClick={() => setModalOpen(true)}
              className="rounded-2xl bg-gradient-to-r from-cyan-300 via-sky-400 to-violet-500 px-6 py-4 text-sm font-black text-slate-950 shadow-[0_20px_60px_rgba(96,165,250,0.22)] hover:brightness-110"
            >
              Ativar acesso pago
            </button>
            <a
              href="#pricing"
              className="rounded-2xl border border-white/10 bg-white/5 px-6 py-4 text-sm font-semibold text-white hover:bg-white/10"
            >
              Ver planos
            </a>
            <button
              onClick={() => nav("/auth?mode=register")}
              className="rounded-2xl border border-white/10 bg-white/5 px-6 py-4 text-sm font-semibold text-white hover:bg-white/10"
            >
              Tenho código privado
            </button>
          </div>

          <div className="mt-8 flex flex-wrap gap-3">
            <TrustChip title="ICP" subtitle="Founders, consultores e equipes em crescimento" />
            <TrustChip title="Go-to-market" subtitle="PWA + checkout + early access" />
            <TrustChip title="Operação" subtitle="Billing, governança e valuation já ligados" />
          </div>

          <div className="mt-8 grid gap-4 sm:grid-cols-3">
            <StatCard label="Posicionamento" value="OS de execução" caption="Mais do que chat. Orquestração com propósito, plano e evidência." />
            <StatCard label="Experiência" value="PWA premium" caption="Distribuição imediata, atualizações contínuas e onboarding controlado." />
            <StatCard label="Monetização" value="Billing live" caption="Checkout, valuation e admin conectados em tempo real." />
          </div>
        </div>

        <div className="rounded-[36px] border border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.07),rgba(255,255,255,0.03))] p-6 shadow-[0_24px_80px_rgba(0,0,0,0.48)] backdrop-blur-2xl">
          <div className="flex items-center justify-between gap-4">
            <div>
              <div className="text-[11px] font-black uppercase tracking-[0.26em] text-white/45">Live entrance</div>
              <div className="mt-2 text-2xl font-black">Conheça o Orkio</div>
              <div className="mt-2 max-w-md text-sm leading-6 text-white/65">
                Visual mais limpo, marca concentrada na esfera e uma entrada comercial pronta para converter.
              </div>
            </div>
            <VoiceOrb />
          </div>

          <div className="mt-8 grid gap-4 md:grid-cols-2">
            <div className="rounded-[28px] border border-white/10 bg-black/20 p-5">
              <div className="text-[11px] font-black uppercase tracking-[0.24em] text-white/42">Value loop</div>
              <div className="mt-4 space-y-4">
                {[
                  ["Diagnostica o objetivo", "Capta contexto e define o resultado desejado."],
                  ["Monta o plano", "Estrutura passos, agentes e prioridades."],
                  ["Executa com governança", "Mantém auditoria, trilha e controle humano."],
                ].map(([title, desc]) => (
                  <div key={title} className="rounded-2xl border border-white/8 bg-white/[0.04] p-4">
                    <div className="text-sm font-black">{title}</div>
                    <div className="mt-2 text-sm leading-6 text-white/62">{desc}</div>
                  </div>
                ))}
              </div>
            </div>

            <div className="rounded-[28px] border border-white/10 bg-black/20 p-5">
              <div className="text-[11px] font-black uppercase tracking-[0.24em] text-white/42">What users feel</div>
              <div className="mt-4 space-y-3">
                {[
                  "Menos ruído visual e mais confiança de marca",
                  "Checkout bonito do primeiro clique ao redirecionamento",
                  "Onboarding preparado para converter em plano pago",
                  "Admin com sinais de negócio, infraestrutura e valuation",
                ].map((item) => (
                  <div key={item} className="flex items-start gap-3 rounded-2xl border border-white/8 bg-white/[0.04] p-4 text-sm leading-6 text-white/76">
                    <span className="mt-2 h-2.5 w-2.5 rounded-full bg-gradient-to-r from-cyan-300 to-violet-500" />
                    <span>{item}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="outcomes" className="mx-auto max-w-7xl px-4 py-16">
        <div className="grid gap-5 lg:grid-cols-3">
          {[
            ["Founders", "Estruture business plans, playbooks e priorização sem se perder em ruído operacional."],
            ["Consultores", "Transforme seu método em uma camada de execução com histórico, ativos e recorrência."],
            ["Equipes", "Coordene operação, aprovação e inteligência com uma interface que não parece improvisada."],
          ].map(([title, desc]) => (
            <div key={title} className="rounded-[30px] border border-white/10 bg-white/[0.04] p-6 backdrop-blur">
              <div className="flex items-center gap-3">
                <OrkioSphereMark size={24} />
                <div className="text-lg font-black">{title}</div>
              </div>
              <div className="mt-4 text-sm leading-7 text-white/66">{desc}</div>
            </div>
          ))}
        </div>
      </section>

      <section id="architecture" className="mx-auto max-w-7xl px-4 py-16">
        <div className="rounded-[36px] border border-white/10 bg-white/[0.04] p-6 backdrop-blur md:p-8">
          <div className="max-w-2xl">
            <div className="text-[11px] font-black uppercase tracking-[0.28em] text-white/42">Architecture</div>
            <h2 className="mt-4 text-3xl font-black tracking-tight md:text-4xl">
              Design revisto para parecer plataforma, não experimento.
            </h2>
            <p className="mt-4 text-sm leading-7 text-white/66">
              O visual agora trabalha com massa, profundidade, hierarquia clara e marca centralizada na esfera.
              Isso aproxima o produto do nível enterprise que a proposta comercial exige.
            </p>
          </div>

          <div className="mt-8 grid gap-4 md:grid-cols-4">
            {[
              ["01", "Entrada comercial", "Landing, PWA install, pricing e widget alinhados ao mesmo sistema visual."],
              ["02", "Checkout seguro", "Fluxo bonito, claro e preparado para Pix ou cartão."],
              ["03", "Cockpit admin", "Infra, billing, valuation e governança no mesmo eixo visual."],
              ["04", "Marca consistente", "A esfera passa a ser o ativo central de reconhecimento."],
            ].map(([n, title, desc]) => (
              <div key={n} className="rounded-[28px] border border-white/10 bg-black/20 p-5">
                <div className="text-sm font-black text-cyan-300">{n}</div>
                <div className="mt-3 text-lg font-black">{title}</div>
                <div className="mt-3 text-sm leading-6 text-white/62">{desc}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section id="pricing" className="mx-auto max-w-7xl px-4 py-16">
        <div className="flex flex-col gap-6 md:flex-row md:items-end md:justify-between">
          <div className="max-w-2xl">
            <div className="text-[11px] font-black uppercase tracking-[0.28em] text-white/42">Pricing</div>
            <h2 className="mt-4 text-3xl font-black tracking-tight md:text-4xl">Planos claros para vender agora.</h2>
            <p className="mt-4 text-sm leading-7 text-white/66">
              O PWA já nasce com proposta comercial simples, premium e escalável. O checkout está integrado ao fluxo de entrada da plataforma.
            </p>
          </div>

          <div className="flex items-center gap-3 rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-xs font-black uppercase tracking-[0.22em] text-white/45">
            <OrkioSphereMark size={16} />
            {plansLoaded ? "Planos sincronizados" : "Carregando planos"}
          </div>
        </div>

        <div className="mt-8 grid gap-5 lg:grid-cols-3">
          {pricingPlans.map((plan, idx) => (
            <PriceCard
              key={plan.code}
              plan={plan}
              tone={idx === 1 ? "featured" : "subtle"}
              onChoose={openCheckout}
            />
          ))}
        </div>

        <div className="mt-8 grid gap-5 lg:grid-cols-[minmax(0,1fr)_360px]">
          <div className="rounded-[32px] border border-white/10 bg-white/[0.04] p-6 backdrop-blur">
            <div className="text-[11px] font-black uppercase tracking-[0.24em] text-white/42">Commercial flow</div>
            <div className="mt-4 grid gap-4 md:grid-cols-4">
              {[
                ["1", "Escolha o plano", "Professional, Business ou Enterprise."],
                ["2", "Redirecionamento seguro", "Pix ou cartão com retorno ao PWA."],
                ["3", "Confirmação automática", "Pagamento confirmado ativa acesso."],
                ["4", "Entrada no Orkio", "Cadastro concluído e console liberado."],
              ].map(([step, title, desc]) => (
                <div key={step} className="rounded-[26px] border border-white/8 bg-black/20 p-5">
                  <div className="text-sm font-black text-cyan-300">{step}</div>
                  <div className="mt-3 text-base font-black">{title}</div>
                  <div className="mt-3 text-sm leading-6 text-white/62">{desc}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-[32px] border border-white/10 bg-[linear-gradient(135deg,rgba(34,211,238,0.12),rgba(124,58,237,0.16),rgba(255,255,255,0.05))] p-6 backdrop-blur">
            <div className="flex items-center gap-3">
              <OrkioSphereMark size={26} badge />
              <div className="text-sm font-black">Private access</div>
            </div>
            <div className="mt-4 text-sm leading-7 text-white/72">
              Usuários convidados por você continuam entrando por código privado, com a mesma governança do fluxo pago.
            </div>
            <button
              onClick={() => nav("/auth?mode=register")}
              className="mt-6 w-full rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-black text-white hover:bg-white/10"
            >
              Tenho código de acesso
            </button>
          </div>
        </div>
      </section>


      <section id="proof" className="mx-auto max-w-7xl px-4 py-16">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div className="max-w-3xl">
            <div className="text-[11px] font-black uppercase tracking-[0.28em] text-white/42">Commercial proof</div>
            <h2 className="mt-4 text-3xl font-black tracking-tight md:text-4xl">
              A landing agora vende contexto, urgência e confiança.
            </h2>
            <p className="mt-4 text-sm leading-7 text-white/66">
              Este bloco foi desenhado para founder-led distribution em LinkedIn, WhatsApp e rede de parceiros.
              Em vez de parecer um experimento técnico, o Orkio passa a se apresentar como produto pronto para adoção controlada.
            </p>
          </div>

          <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-3 text-[11px] font-black uppercase tracking-[0.22em] text-white/44">
            Early access com escassez e narrativa institucional
          </div>
        </div>

        <div className="mt-8 grid gap-5 lg:grid-cols-3">
          <ProofCard
            eyebrow="Who buys first"
            title="Founders e consultores sentem o valor mais rápido."
            copy="Esse ICP tem dor imediata de organização estratégica, execução e clareza. O Orkio entra como sistema de direção, plano e acompanhamento."
            bullets={[
              "Business plan e playbook 30/60/90 em fluxo único",
              "Histórico e memória operacional sem espalhar contexto",
              "Entrada simples por PWA para compartilhar e indicar",
            ]}
          />
          <ProofCard
            eyebrow="Why now"
            title="O timing comercial está maduro."
            copy="Você já tem produto, pricing, checkout, valuation e governança em linha. O próximo crescimento vem da distribuição certa, não de mais teoria."
            bullets={[
              "Lançamento via PWA reduz fricção e custo",
              "Checkout ativo acelera conversão sem depender de app stores",
              "Admin já mostra sinais de negócio, infra e receita",
            ]}
          />
          <ProofCard
            eyebrow="What increases trust"
            title="Confiança visual e operacional no primeiro clique."
            copy="Quem chega precisa sentir produto premium, não protótipo. Essa camada visual reforça a percepção de valor e encurta o caminho até o pagamento."
            bullets={[
              "Marca concentrada na esfera e interface mais memorável",
              "Fluxo comercial elegante do plano ao checkout",
              "Governança e acesso privado preservados no mesmo sistema",
            ]}
          />
        </div>

        <div className="mt-8 grid gap-5 lg:grid-cols-[minmax(0,1fr)_360px]">
          <div className="rounded-[34px] border border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.07),rgba(255,255,255,0.03))] p-6 backdrop-blur md:p-8">
            <div className="text-[11px] font-black uppercase tracking-[0.26em] text-white/42">Use cases with stronger conversion</div>
            <div className="mt-6 grid gap-4 md:grid-cols-2">
              {[
                ["Founder operating system", "Estruturar visão, prioridades, business plan, captação e trilha de execução sem perder contexto."],
                ["Consulting delivery engine", "Transformar método em recorrência, evidência de valor e mais capacidade operacional por cliente."],
                ["Team execution cockpit", "Centralizar decisões, aprovações, playbooks e memória institucional num workspace governado."],
                ["Enterprise entry wedge", "Começar por uma dor clara, provar valor rápido e expandir para setup, integrações e planos maiores."],
              ].map(([title, desc]) => (
                <div key={title} className="rounded-[28px] border border-white/8 bg-black/20 p-5">
                  <div className="text-base font-black">{title}</div>
                  <div className="mt-3 text-sm leading-6 text-white/62">{desc}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-[34px] border border-white/10 bg-[linear-gradient(135deg,rgba(34,211,238,0.12),rgba(124,58,237,0.16),rgba(255,255,255,0.05))] p-6 backdrop-blur">
            <div className="flex items-center gap-3">
              <OrkioSphereMark size={24} badge />
              <div className="text-sm font-black">Early access signal</div>
            </div>
            <div className="mt-5 space-y-4">
              {[
                ["Canal", "Distribuição founder-led por LinkedIn, WhatsApp e rede de negócios."],
                ["Oferta", "Acesso antecipado com entrada paga ou código privado governado."],
                ["Escassez", "Janela ideal para captar operadores certos antes da abertura ampla."],
              ].map(([title, desc]) => (
                <div key={title} className="rounded-[24px] border border-white/8 bg-black/20 p-4">
                  <div className="text-[11px] font-black uppercase tracking-[0.22em] text-white/42">{title}</div>
                  <div className="mt-2 text-sm leading-6 text-white/74">{desc}</div>
                </div>
              ))}
            </div>
            <button
              onClick={() => setModalOpen(true)}
              className="mt-6 w-full rounded-2xl bg-gradient-to-r from-cyan-300 via-sky-400 to-violet-500 px-4 py-3 text-sm font-black text-slate-950 hover:brightness-110"
            >
              Garantir meu acesso agora
            </button>
          </div>
        </div>

        <div className="mt-8 rounded-[34px] border border-white/10 bg-white/[0.04] p-6 backdrop-blur md:p-8">
          <div className="flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">
            <div className="max-w-3xl">
              <div className="text-[11px] font-black uppercase tracking-[0.26em] text-white/42">Suggested social proof layer</div>
              <h3 className="mt-3 text-2xl font-black tracking-tight">Pronto para receber logos, depoimentos curtos e parceiros âncora.</h3>
              <p className="mt-3 text-sm leading-7 text-white/66">
                Deixei a estrutura preparada para encaixar cases, logos de clientes e sinais institucionais assim que você escolher os primeiros nomes públicos.
              </p>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 lg:w-[420px]">
              {[
                "Founder network ready",
                "Consulting-ready delivery",
                "PWA with billing",
                "Governed private access",
              ].map((item) => (
                <div key={item} className="rounded-2xl border border-white/8 bg-black/20 px-4 py-4 text-sm font-semibold text-white/80">
                  {item}
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>


      <section id="security" className="mx-auto max-w-7xl px-4 py-16">
        <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_380px]">
          <div className="rounded-[36px] border border-white/10 bg-white/[0.04] p-6 backdrop-blur md:p-8">
            <div className="text-[11px] font-black uppercase tracking-[0.28em] text-white/42">Security and trust</div>
            <h2 className="mt-4 text-3xl font-black tracking-tight md:text-4xl">O design agora sustenta a promessa do produto.</h2>
            <p className="mt-4 text-sm leading-7 text-white/66">
              Menos elementos supérfluos, marca mais memorável e interface mais sólida. Isso melhora percepção de valor,
              reduz fricção e prepara o Orkio para escala comercial.
            </p>
          </div>

          <div className="rounded-[36px] border border-white/10 bg-[linear-gradient(180deg,rgba(34,211,238,0.1),rgba(124,58,237,0.14),rgba(255,255,255,0.04))] p-6 backdrop-blur">
            <div className="flex justify-center">
              <OrkioSphereMark size={86} badge />
            </div>
            <div className="mt-6 text-center text-sm leading-7 text-white/72">
              A esfera passa a ser o núcleo visual do Orkio em landing, widget, PWA e entrada de autenticação.
            </div>
            <button
              onClick={() => setModalOpen(true)}
              className="mt-6 w-full rounded-2xl bg-gradient-to-r from-cyan-300 via-sky-400 to-violet-500 px-4 py-3 text-sm font-black text-slate-950 hover:brightness-110"
            >
              Ir para checkout
            </button>
          </div>
        </div>
      </section>

      <div className="mx-auto max-w-7xl px-4 pb-16 pt-2">
        <div className="rounded-[38px] border border-white/10 bg-[linear-gradient(135deg,rgba(34,211,238,0.12),rgba(124,58,237,0.16),rgba(255,255,255,0.05))] p-8 text-center shadow-[0_24px_80px_rgba(0,0,0,0.35)] backdrop-blur-xl">
          <div className="mx-auto flex w-fit items-center gap-3 rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-[11px] font-black uppercase tracking-[0.24em] text-white/46">
            <OrkioSphereMark size={18} />
            Launch-ready PWA
          </div>
          <h2 className="mt-6 text-3xl font-black tracking-tight md:text-5xl">
            Orkio está pronto para parecer o que ele já é.
          </h2>
          <p className="mx-auto mt-5 max-w-3xl text-base leading-8 text-white/66">
            Uma plataforma de execução e inteligência com visual de produto premium, entrada comercial clara e checkout desenhado para converter.
          </p>
          <div className="mt-8 flex flex-wrap justify-center gap-3">
            <button
              onClick={() => setModalOpen(true)}
              className="rounded-2xl bg-gradient-to-r from-cyan-300 via-sky-400 to-violet-500 px-6 py-4 text-sm font-black text-slate-950 hover:brightness-110"
            >
              Ativar meu acesso
            </button>
            <button
              onClick={() => nav("/auth?mode=login")}
              className="rounded-2xl border border-white/10 bg-white/5 px-6 py-4 text-sm font-semibold text-white hover:bg-white/10"
            >
              Já tenho acesso
            </button>
          </div>
        </div>
      </div>

      <PublicChatWidget />
      <Footer />

      <PricingCheckoutModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        defaultPlanCode={selectedPlanCode}
      />
    </div>
  );
}
