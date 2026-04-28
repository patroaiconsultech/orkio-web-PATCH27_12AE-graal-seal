import React from "react";

export default function MessageComposer({
  value,
  onChange,
  onSubmit,
  disabled = false,
  placeholder = "Digite sua próxima instrução com clareza...",
}) {
  return (
    <form className="mt-4 rounded-3xl border border-white/10 bg-black/20 p-3 backdrop-blur-xl" onSubmit={onSubmit}>
      <div className="flex flex-col gap-3 md:flex-row">
        <textarea
          value={value}
          onChange={onChange}
          disabled={disabled}
          placeholder={placeholder}
          rows={3}
          className="min-h-[72px] flex-1 resize-none rounded-2xl border border-white/10 bg-transparent px-4 py-3 text-sm text-white outline-none placeholder:text-white/35"
        />
        <button type="submit" disabled={disabled} className="rounded-2xl bg-cyan-300 px-5 py-3 text-sm font-semibold text-black transition hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-50">
          Enviar
        </button>
      </div>
    </form>
  );
}
