import React from "react";

export default function EmptyStatePremium({
  user,
  onPrimaryAction,
  onSecondaryAction,
  onTertiaryAction,
  onFillPrompt,
}) {
  const firstName = String(user?.name || user?.email || "").trim().split(/\s+/)[0] || "Daniel";

  const shell = {
    borderRadius: "24px",
    border: "1px solid rgba(255,255,255,0.10)",
    background: "linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03))",
    boxShadow: "0 20px 80px rgba(0,0,0,0.22)",
    padding: "22px",
    maxWidth: "920px",
    margin: "12px auto 0",
    overflow: "hidden",
  };

  const eyebrow = {
    display: "inline-flex",
    alignItems: "center",
    gap: "8px",
    padding: "6px 12px",
    borderRadius: "999px",
    border: "1px solid rgba(89,165,255,0.30)",
    background: "rgba(89,165,255,0.10)",
    color: "rgba(214,236,255,0.96)",
    fontSize: "12px",
    fontWeight: 900,
    letterSpacing: "0.02em",
    marginBottom: "14px",
  };

  const title = {
    margin: 0,
    color: "#fff",
    fontSize: "clamp(26px, 4vw, 38px)",
    lineHeight: 1.05,
    fontWeight: 900,
    letterSpacing: "-0.03em",
  };

  const subtitle = {
    margin: "14px 0 0",
    maxWidth: "760px",
    color: "rgba(255,255,255,0.72)",
    fontSize: "15px",
    lineHeight: 1.6,
  };

  const cardGrid = {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
    gap: "12px",
    marginTop: "20px",
  };

  const card = {
    borderRadius: "18px",
    border: "1px solid rgba(255,255,255,0.08)",
    background: "rgba(255,255,255,0.03)",
    padding: "14px",
  };

  const cardTitle = {
    color: "#fff",
    fontSize: "14px",
    fontWeight: 800,
    marginBottom: "6px",
  };

  const cardText = {
    color: "rgba(255,255,255,0.66)",
    fontSize: "13px",
    lineHeight: 1.55,
  };

  const actionRow = {
    display: "flex",
    gap: "10px",
    flexWrap: "wrap",
    marginTop: "22px",
  };

  const primaryBtn = {
    border: 0,
    borderRadius: "14px",
    padding: "13px 16px",
    cursor: "pointer",
    fontWeight: 900,
    color: "#0b1020",
    background: "linear-gradient(135deg, #7dd3fc, #c4b0ff)",
    boxShadow: "0 8px 30px rgba(125,211,252,0.22)",
  };

  const secondaryBtn = {
    borderRadius: "14px",
    border: "1px solid rgba(255,255,255,0.12)",
    padding: "13px 16px",
    cursor: "pointer",
    fontWeight: 800,
    color: "#fff",
    background: "rgba(255,255,255,0.04)",
  };

  const quickRow = {
    display: "flex",
    gap: "8px",
    flexWrap: "wrap",
    marginTop: "18px",
  };

  const quickBtn = {
    borderRadius: "999px",
    border: "1px solid rgba(255,255,255,0.10)",
    background: "rgba(255,255,255,0.04)",
    color: "rgba(255,255,255,0.82)",
    padding: "9px 12px",
    fontSize: "12px",
    fontWeight: 800,
    cursor: "pointer",
  };

  const nextStep = {
    marginTop: "18px",
    padding: "14px 16px",
    borderRadius: "16px",
    border: "1px solid rgba(255,255,255,0.08)",
    background: "rgba(255,255,255,0.025)",
    color: "rgba(255,255,255,0.75)",
    fontSize: "13px",
    lineHeight: 1.6,
  };

  return (
    <div style={shell}>
      <div style={eyebrow}>
        <span>✨</span>
        <span>Experiência premium sob seu controle</span>
      </div>

      <h2 style={title}>
        {firstName}, vamos transformar potência em clareza de execução.
      </h2>

      <p style={subtitle}>
        O backend já está forte. Agora o console precisa mostrar valor desde o primeiro segundo:
        direção clara, sensação de comando e uma primeira vitória guiada sem fricção.
      </p>

      <div style={cardGrid}>
        <div style={card}>
          <div style={cardTitle}>Primeira vitória rápida</div>
          <div style={cardText}>
            Receba um próximo passo executável, sem ruído, já na primeira interação.
          </div>
        </div>
        <div style={card}>
          <div style={cardTitle}>Controle humano explícito</div>
          <div style={cardText}>
            Branch, patch e PR continuam governados por sua autorização escrita.
          </div>
        </div>
        <div style={card}>
          <div style={cardTitle}>Fluxo premium e fluido</div>
          <div style={cardText}>
            Comece por chat, voz ou estratégia sem perder contexto, autenticação ou streaming.
          </div>
        </div>
      </div>

      <div style={actionRow}>
        <button type="button" style={primaryBtn} onClick={onPrimaryAction}>
          Guiar minha primeira vitória
        </button>
        <button type="button" style={secondaryBtn} onClick={onSecondaryAction}>
          Montar diagnóstico executivo
        </button>
        <button type="button" style={secondaryBtn} onClick={onTertiaryAction}>
          Preencher prompt estratégico
        </button>
      </div>

      <div style={quickRow}>
        <button
          type="button"
          style={quickBtn}
          onClick={() => onFillPrompt?.("@Orion me entregue uma leitura executiva da prioridade mais importante agora e o próximo melhor passo.")}
        >
          Prioridade e próximo passo
        </button>
        <button
          type="button"
          style={quickBtn}
          onClick={() => onFillPrompt?.("@Team mapeiem a melhor oportunidade de crescimento imediato com baixo risco.")}
        >
          Crescimento com baixo risco
        </button>
        <button
          type="button"
          style={quickBtn}
          onClick={() => onFillPrompt?.("@Orion organize um plano prático de execução para hoje, com foco em impacto real.")}
        >
          Plano prático para hoje
        </button>
      </div>

      <div style={nextStep}>
        <strong>Próximo passo recomendado:</strong> use a primeira ação guiada para gerar uma resposta
        de alto valor logo no início. Isso reduz atrito, aumenta a percepção premium e faz o produto
        parecer vivo desde a tela vazia.
      </div>
    </div>
  );
}
