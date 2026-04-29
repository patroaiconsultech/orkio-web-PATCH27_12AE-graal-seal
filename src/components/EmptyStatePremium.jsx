import React from "react";

const COLORS = {
  surface: "linear-gradient(180deg, rgba(12, 19, 34, 0.98) 0%, rgba(7, 11, 21, 1) 100%)",
  border: "rgba(148, 163, 184, 0.16)",
  text: "#f8fafc",
  muted: "#94a3b8",
  subtle: "#64748b",
  accent: "#7c3aed",
  accent2: "#2563eb",
  success: "#22c55e",
  warning: "#f59e0b",
};

const SUGGESTIONS = [
  "Quero um diagnóstico executivo da plataforma",
  "Monte um plano cirúrgico para a próxima melhoria",
  "Mostre a prioridade mais importante desta semana",
];

const ACTIONS = [
  {
    id: "primary",
    title: "Conversa guiada",
    description: "Entrar no fluxo principal com uma pergunta de alto valor já pronta.",
  },
  {
    id: "secondary",
    title: "Blueprint",
    description: "Abrir um plano de estruturação com foco em impacto e sequência.",
  },
  {
    id: "tertiary",
    title: "Próximos passos",
    description: "Preencher o prompt com o próximo melhor movimento operacional.",
  },
];

function callMaybe(fn, ...args) {
  if (typeof fn === "function") fn(...args);
}

export default function EmptyStatePremium({
  user,
  onStart,
  onPrimaryAction,
  onSecondaryAction,
  onTertiaryAction,
  onFillPrompt,
}) {
  const firstName = String(user?.name || user?.full_name || user?.email || "Founder")
    .split("@")[0]
    .split(" ")[0]
    .trim();

  const handleAction = (id) => {
    if (id === "primary") {
      if (typeof onPrimaryAction === "function") return onPrimaryAction();
      return callMaybe(onStart);
    }
    if (id === "secondary") return callMaybe(onSecondaryAction);
    if (id === "tertiary") return callMaybe(onTertiaryAction);
  };

  return (
    <div
      style={{
        width: "100%",
        background: COLORS.surface,
        border: `1px solid ${COLORS.border}`,
        borderRadius: 34,
        padding: 30,
        boxShadow: "0 34px 90px rgba(2, 6, 23, 0.42)",
        position: "relative",
        overflow: "hidden",
      }}
    >
      <div
        style={{
          position: "absolute",
          inset: 0,
          background:
            "radial-gradient(circle at top left, rgba(124, 58, 237, 0.26), transparent 34%), radial-gradient(circle at top right, rgba(37, 99, 235, 0.18), transparent 26%), linear-gradient(180deg, rgba(255,255,255,0.01), rgba(255,255,255,0))",
          pointerEvents: "none",
        }}
      />

      <div style={{ position: "relative", zIndex: 1 }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: 16,
            flexWrap: "wrap",
            alignItems: "center",
            marginBottom: 20,
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
              border: "1px solid rgba(124, 58, 237, 0.22)",
              color: "#ddd6fe",
              fontSize: 12,
              fontWeight: 900,
              letterSpacing: "0.06em",
              textTransform: "uppercase",
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
            Orkio Command Deck
          </div>

          <div
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 10,
              borderRadius: 999,
              padding: "10px 14px",
              background: "rgba(15, 23, 42, 0.56)",
              border: `1px solid ${COLORS.border}`,
              color: COLORS.muted,
              fontSize: 12,
              fontWeight: 800,
            }}
          >
            <span
              style={{
                width: 8,
                height: 8,
                borderRadius: "50%",
                background: COLORS.accent2,
                boxShadow: "0 0 0 6px rgba(37, 99, 235, 0.14)",
              }}
            />
            Shell preservado · experiência reforçada
          </div>
        </div>

        <div style={{ maxWidth: 900 }}>
          <h1
            style={{
              margin: 0,
              color: COLORS.text,
              fontSize: 48,
              lineHeight: 1.02,
              letterSpacing: "-0.045em",
            }}
          >
            Bem-vindo, {firstName || "Founder"}
          </h1>

          <p
            style={{
              marginTop: 16,
              marginBottom: 0,
              color: COLORS.muted,
              fontSize: 18,
              lineHeight: 1.78,
              maxWidth: 820,
            }}
          >
            A navegação, as threads e os acessos continuam exatamente onde devem estar.
            O que muda aqui é o centro da experiência: mais clareza, mais direção e uma
            primeira vitória perceptível já no primeiro olhar.
          </p>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "2.1fr 1fr",
            gap: 16,
            marginTop: 24,
          }}
        >
          <div
            style={{
              borderRadius: 26,
              padding: 22,
              background: "linear-gradient(135deg, rgba(124, 58, 237, 0.18) 0%, rgba(37, 99, 235, 0.12) 100%)",
              border: "1px solid rgba(124, 58, 237, 0.18)",
            }}
          >
            <div
              style={{
                color: "#ddd6fe",
                fontSize: 12,
                letterSpacing: "0.14em",
                textTransform: "uppercase",
                fontWeight: 900,
                marginBottom: 10,
              }}
            >
              Próxima melhor ação
            </div>
            <div
              style={{
                color: COLORS.text,
                fontSize: 28,
                lineHeight: 1.12,
                fontWeight: 900,
                marginBottom: 10,
              }}
            >
              Comece com uma conversa guiada e já entre na trilha executiva certa
            </div>
            <div
              style={{
                color: COLORS.muted,
                fontSize: 15,
                lineHeight: 1.72,
                maxWidth: 700,
              }}
            >
              Em vez de um vazio contemplativo, o centro do console agora entrega ação,
              direção e contexto. O usuário percebe valor mais rápido e entende o que fazer em segundos.
            </div>

            <div
              style={{
                display: "flex",
                gap: 12,
                flexWrap: "wrap",
                marginTop: 18,
              }}
            >
              <button
                onClick={() => handleAction("primary")}
                style={{
                  border: "none",
                  background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 100%)",
                  color: "white",
                  borderRadius: 16,
                  padding: "15px 18px",
                  fontSize: 14,
                  fontWeight: 900,
                  cursor: "pointer",
                  boxShadow: "0 22px 44px rgba(76, 29, 149, 0.34)",
                }}
              >
                Iniciar conversa guiada
              </button>

              <button
                onClick={() => handleAction("secondary")}
                style={{
                  border: "1px solid rgba(148, 163, 184, 0.18)",
                  background: "rgba(15, 23, 42, 0.62)",
                  color: COLORS.text,
                  borderRadius: 16,
                  padding: "15px 18px",
                  fontSize: 14,
                  fontWeight: 800,
                  cursor: "pointer",
                }}
              >
                Abrir blueprint
              </button>
            </div>
          </div>

          <div
            style={{
              borderRadius: 26,
              padding: 20,
              background: "rgba(15, 23, 42, 0.58)",
              border: `1px solid ${COLORS.border}`,
            }}
          >
            <div
              style={{
                color: COLORS.subtle,
                fontSize: 12,
                letterSpacing: "0.12em",
                textTransform: "uppercase",
                fontWeight: 900,
                marginBottom: 8,
              }}
            >
              Sinal de prontidão
            </div>

            <div
              style={{
                color: COLORS.text,
                fontSize: 22,
                lineHeight: 1.15,
                fontWeight: 900,
                marginBottom: 10,
              }}
            >
              Pronto para operar
            </div>

            <div
              style={{
                color: COLORS.muted,
                fontSize: 14,
                lineHeight: 1.7,
                marginBottom: 14,
              }}
            >
              Threads, usuário, wallet e navegação já estão disponíveis. A camada premium entra como reforço de decisão.
            </div>

            <div style={{ display: "grid", gap: 10 }}>
              {[
                ["Navegação", "Visível"],
                ["Primeira ação", "Guiada"],
                ["Leitura", "Executiva"],
              ].map(([label, value]) => (
                <div
                  key={label}
                  style={{
                    borderRadius: 18,
                    padding: "12px",
                    border: "1px solid rgba(148, 163, 184, 0.12)",
                    background: "rgba(255,255,255,0.03)",
                  }}
                >
                  <div
                    style={{
                      fontSize: 11,
                      color: COLORS.subtle,
                      textTransform: "uppercase",
                      letterSpacing: "0.08em",
                      marginBottom: 6,
                      fontWeight: 800,
                    }}
                  >
                    {label}
                  </div>
                  <div style={{ color: COLORS.text, fontSize: 14, fontWeight: 900 }}>
                    {value}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
            gap: 14,
            marginTop: 20,
          }}
        >
          {ACTIONS.map((action) => (
            <button
              key={action.id}
              onClick={() => handleAction(action.id)}
              style={{
                textAlign: "left",
                background: "rgba(15, 23, 42, 0.52)",
                border: `1px solid ${COLORS.border}`,
                borderRadius: 22,
                padding: 18,
                cursor: "pointer",
                color: COLORS.text,
              }}
            >
              <div
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  justifyContent: "center",
                  width: 36,
                  height: 36,
                  borderRadius: 14,
                  marginBottom: 12,
                  background: action.id === "primary"
                    ? "rgba(124, 58, 237, 0.18)"
                    : action.id === "secondary"
                    ? "rgba(37, 99, 235, 0.16)"
                    : "rgba(245, 158, 11, 0.12)",
                  border: `1px solid ${
                    action.id === "primary"
                      ? "rgba(124, 58, 237, 0.24)"
                      : action.id === "secondary"
                      ? "rgba(37, 99, 235, 0.24)"
                      : "rgba(245, 158, 11, 0.18)"
                  }`,
                  fontWeight: 900,
                  fontSize: 14,
                }}
              >
                {action.id === "primary" ? "01" : action.id === "secondary" ? "02" : "03"}
              </div>
              <div style={{ fontSize: 16, fontWeight: 900, marginBottom: 8 }}>
                {action.title}
              </div>
              <div style={{ color: COLORS.muted, fontSize: 13, lineHeight: 1.65 }}>
                {action.description}
              </div>
            </button>
          ))}
        </div>

        <div
          style={{
            borderRadius: 26,
            padding: 18,
            marginTop: 18,
            background: "rgba(2, 6, 23, 0.34)",
            border: `1px solid ${COLORS.border}`,
          }}
        >
          <div
            style={{
              color: COLORS.subtle,
              fontSize: 12,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              fontWeight: 900,
              marginBottom: 10,
            }}
          >
            Começos rápidos
          </div>

          <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
            {SUGGESTIONS.map((prompt) => (
              <button
                key={prompt}
                onClick={() => callMaybe(onFillPrompt, prompt)}
                style={{
                  border: "1px solid rgba(148, 163, 184, 0.16)",
                  background: "rgba(15, 23, 42, 0.56)",
                  color: COLORS.muted,
                  borderRadius: 999,
                  padding: "10px 14px",
                  fontSize: 13,
                  lineHeight: 1.35,
                  cursor: "pointer",
                }}
              >
                {prompt}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
