import React from "react";

export default function EmptyStatePremium({
  title = "Seu próximo avanço começa aqui",
  subtitle = "Acione uma primeira vitória guiada, com clareza, controle humano e acabamento premium.",
  primaryLabel = "Iniciar primeira ação guiada",
  secondaryLabel = "Ver capacidades da plataforma",
  onPrimaryAction,
  onSecondaryAction,
  recommendedStep = "Acionar Orion para validar a próxima melhoria em modo governado.",
}) {
  return (
    <section className="rounded-3xl border border-white/10 bg-white/5 p-6 shadow-[0_24px_80px_rgba(0,0,0,0.35)] backdrop-blur-xl">
      <div className="mb-4 flex items-center gap-2 text-xs uppercase tracking-[0.22em] text-cyan-300">
        <span className="inline-flex h-2 w-2 rounded-full bg-cyan-300" />
        Premium First Win
      </div>
      <h2 className="text-2xl font-semibold text-white">{title}</h2>
      <p className="mt-3 max-w-2xl text-sm leading-6 text-white/70">{subtitle}</p>
      <div className="mt-6 grid gap-3 md:grid-cols-[1fr_auto_auto]">
        <div className="rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-sm text-white/75">
          Próximo passo recomendado: <strong className="text-white">{recommendedStep}</strong>
        </div>
        <button type="button" onClick={onPrimaryAction} className="rounded-2xl bg-white px-5 py-3 text-sm font-medium text-black transition hover:opacity-90">
          {primaryLabel}
        </button>
        <button type="button" onClick={onSecondaryAction} className="rounded-2xl border border-white/15 px-5 py-3 text-sm font-medium text-white/85 transition hover:bg-white/5">
          {secondaryLabel}
        </button>
      </div>
    </section>
  );
}
