import React from "react";

const COLORS = {
  bg: "#020617",
  panel: "rgba(15, 23, 42, 0.84)",
  panelStrong: "#0f172a",
  border: "rgba(148, 163, 184, 0.16)",
  text: "#f8fafc",
  muted: "#94a3b8",
  subtle: "#64748b",
  accent: "#7c3aed",
  accent2: "#2563eb",
  success: "#22c55e",
};

const featureCards = [
  {
    title: "Execução auditável",
    description: "Cada etapa deixa rastro operacional verificável, com mais confiança e menos ruído.",
  },
  {
    title: "Inteligência orquestrada",
    description: "Diagnóstico, despacho e consolidação fluem como uma única experiência de alto padrão.",
  },
  {
    title: "Pronta para decisão",
    description: "A interface conduz para a próxima ação com clareza, sem parecer um console técnico bruto.",
  },
];

export default function EmptyStatePremium({ onStart }) {
  return (
    <div
      style={{
        minHeight: "100vh",
        background:
          "radial-gradient(circle at top left, rgba(124, 58, 237, 0.22), transparent 28%), radial-gradient(circle at top right, rgba(37, 99, 235, 0.18), transparent 24%), #020617",
        color: COLORS.text,
        padding: 28,
        boxSizing: "border-box",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <div
        style={{
          width: "100%",
          maxWidth: 1240,
          display: "grid",
          gridTemplateColumns: "minmax(0, 1.1fr) minmax(320px, 460px)",
          gap: 24,
          alignItems: "stretch",
        }}
      >
        <div
          style={{
            background: COLORS.panel,
            border: `1px solid ${COLORS.border}`,
            borderRadius: 32,
            padding: 36,
            backdropFilter: "blur(14px)",
            boxShadow: "0 30px 80px rgba(2, 6, 23, 0.5)",
          }}
        >
          <div
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 10,
              borderRadius: 999,
              padding: "10px 14px",
              background: "rgba(124, 58, 237, 0.14)",
              border: "1px solid rgba(124, 58, 237, 0.2)",
              color: "#ddd6fe",
              fontSize: 13,
              fontWeight: 700,
              letterSpacing: "0.03em",
              marginBottom: 20,
            }}
          >
            <span
              style={{
                width: 8,
                height: 8,
                borderRadius: "50%",
                background: COLORS.success,
                boxShadow: "0 0 0 6px rgba(34, 197, 94, 0.16)",
              }}
            />
            Ambiente operacional ativo
          </div>

          <div style={{ maxWidth: 720 }}>
            <h1
              style={{
                margin: 0,
                fontSize: 52,
                lineHeight: 1.02,
                letterSpacing: "-0.04em",
              }}
            >
              Orkio Intelligence Console
            </h1>

            <p
              style={{
                marginTop: 18,
                marginBottom: 0,
                color: COLORS.muted,
                fontSize: 18,
                lineHeight: 1.7,
                maxWidth: 680,
              }}
            >
              Uma experiência de execução estratégica desenhada para parecer
              premium desde o primeiro clique: clara, elegante, confiável e
              orientada à próxima decisão.
            </p>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
              gap: 16,
              marginTop: 30,
            }}
          >
            {featureCards.map((card) => (
              <div
                key={card.title}
                style={{
                  background: "rgba(15, 23, 42, 0.58)",
                  border: `1px solid ${COLORS.border}`,
                  borderRadius: 24,
                  padding: 20,
                }}
              >
                <div
                  style={{
                    fontSize: 15,
                    fontWeight: 700,
                    marginBottom: 10,
                    color: COLORS.text,
                  }}
                >
                  {card.title}
                </div>
                <div
                  style={{
                    fontSize: 14,
                    lineHeight: 1.65,
                    color: COLORS.muted,
                  }}
                >
                  {card.description}
                </div>
              </div>
            ))}
          </div>

          <div
            style={{
              marginTop: 28,
              display: "flex",
              gap: 14,
              flexWrap: "wrap",
              alignItems: "center",
            }}
          >
            <button
              onClick={onStart}
              style={{
                border: "none",
                background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 100%)",
                color: "white",
                borderRadius: 16,
                padding: "16px 22px",
                fontSize: 15,
                fontWeight: 700,
                cursor: "pointer",
                boxShadow: "0 20px 40px rgba(76, 29, 149, 0.35)",
              }}
            >
              Iniciar execução premium
            </button>

            <div
              style={{
                color: COLORS.subtle,
                fontSize: 14,
                display: "flex",
                alignItems: "center",
                gap: 10,
              }}
            >
              <span
                style={{
                  width: 10,
                  height: 10,
                  borderRadius: "50%",
                  background: COLORS.success,
                  display: "inline-block",
                }}
              />
              Primeiro resultado guiado em poucos segundos
            </div>
          </div>
        </div>

        <div
          style={{
            background: COLORS.panelStrong,
            border: `1px solid ${COLORS.border}`,
            borderRadius: 32,
            padding: 28,
            boxShadow: "0 30px 80px rgba(2, 6, 23, 0.45)",
            display: "flex",
            flexDirection: "column",
            gap: 18,
          }}
        >
          <div>
            <div
              style={{
                color: COLORS.muted,
                textTransform: "uppercase",
                letterSpacing: "0.12em",
                fontSize: 12,
                marginBottom: 10,
              }}
            >
              Primeira vitória
            </div>
            <div
              style={{
                fontSize: 28,
                fontWeight: 800,
                lineHeight: 1.15,
                marginBottom: 10,
              }}
            >
              Entre no fluxo certo imediatamente
            </div>
            <div
              style={{
                color: COLORS.muted,
                fontSize: 15,
                lineHeight: 1.7,
              }}
            >
              O console já apresenta o valor antes da execução começar:
              controle operacional, sinal de atividade e uma ação principal
              inequívoca.
            </div>
          </div>

          <div
            style={{
              borderRadius: 24,
              padding: 20,
              background: "linear-gradient(180deg, rgba(30, 41, 59, 0.72) 0%, rgba(15, 23, 42, 0.88) 100%)",
              border: `1px solid ${COLORS.border}`,
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                gap: 16,
                alignItems: "center",
                marginBottom: 18,
              }}
            >
              <div>
                <div style={{ fontSize: 13, color: COLORS.subtle, marginBottom: 6 }}>
                  Status do ambiente
                </div>
                <div style={{ fontSize: 18, fontWeight: 700 }}>Operação pronta</div>
              </div>
              <div
                style={{
                  color: "#86efac",
                  background: "rgba(34, 197, 94, 0.14)",
                  border: "1px solid rgba(34, 197, 94, 0.22)",
                  borderRadius: 999,
                  padding: "8px 12px",
                  fontSize: 12,
                  fontWeight: 700,
                }}
              >
                Live
              </div>
            </div>

            <div style={{ display: "grid", gap: 14 }}>
              {[
                "Diagnóstico e despacho apresentados em sequência elegante.",
                "Timeline e logs organizados para leitura executiva.",
                "Visual de alta confiança sem depender de bibliotecas extras.",
              ].map((item) => (
                <div
                  key={item}
                  style={{
                    display: "flex",
                    alignItems: "flex-start",
                    gap: 12,
                    color: COLORS.muted,
                    fontSize: 14,
                    lineHeight: 1.65,
                  }}
                >
                  <span
                    style={{
                      width: 8,
                      height: 8,
                      borderRadius: "50%",
                      background: COLORS.accent2,
                      marginTop: 8,
                      flexShrink: 0,
                    }}
                  />
                  <span>{item}</span>
                </div>
              ))}
            </div>
          </div>

          <div
            style={{
              borderRadius: 24,
              padding: 18,
              border: `1px dashed ${COLORS.border}`,
              color: COLORS.subtle,
              fontSize: 13,
              lineHeight: 1.7,
            }}
          >
            Direção de UX: manter percepção de exclusividade, clareza de controle
            humano e sensação de produto maduro já no estado vazio.
          </div>
        </div>
      </div>
    </div>
  );
}
