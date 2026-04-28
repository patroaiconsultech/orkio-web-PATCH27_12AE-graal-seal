import React from "react";

export default function ChatTopbar({ title = "Orkio Console", objective = "Primeira vitória guiada", humanControl = true }) {
  return (
    <header className="mb-4 rounded-3xl border border-white/10 bg-white/5 px-5 py-4 backdrop-blur-xl">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-xs uppercase tracking-[0.24em] text-white/45">Console premium</div>
          <h1 className="mt-1 text-lg font-semibold text-white">{title}</h1>
        </div>
        <div className="rounded-full border border-emerald-400/20 bg-emerald-400/10 px-3 py-1 text-xs text-emerald-200">
          {humanControl ? "Controle humano ativo" : "Fluxo assistido"}
        </div>
      </div>
      <p className="mt-3 text-sm text-white/65">Objetivo atual: {objective}</p>
    </header>
  );
}
