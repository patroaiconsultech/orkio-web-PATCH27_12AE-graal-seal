import React from "react";

const COLORS = {
  surface: "linear-gradient(180deg, rgba(15, 23, 42, 0.96) 0%, rgba(7, 12, 22, 0.98) 100%)",
  border: "rgba(148, 163, 184, 0.16)",
  text: "#f8fafc",
  muted: "#94a3b8",
  subtle: "#64748b",
  accent: "#7c3aed",
  accent2: "#2563eb",
  success: "#22c55e",
};

const PROMPT_SUGGESTIONS = [
  "Quero um diagnóstico executivo da plataforma",
  "Monte um plano cirúrgico para a próxima melhoria",
  "Mostre a prioridade mais importante desta semana",
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

  const handlePrimary = () => {
    if (typeof onPrimaryAction === "function") {
      onPrimaryAction();
      return;
    }
    callMaybe(onStart);
  };

  return (
    <div
      style={{
        width: "100%",
        display: "grid",
        gridTemplateColumns: "minmax(0, 1.18fr) minmax(320px, 420px)",
        gap: 20,
        alignItems: "stretch",
      }}
    >
      <div
        style={{
          background: COLORS.surface,
          border: `1px solid ${COLORS.border}`,
          borderRadius: 32,
          padding: 28,
          boxShadow: "0 32px 90px rgba(2, 6, 23, 0.42)",
          position: "relative",
          overflow: "hidden",
        }}
      >
        <div
          style={{
            position: "absolute",
            inset: 0,
            background:
              "radial-gradient(circle at top left, rgba(124, 58, 237, 0.22), transparent 32%), radial-gradient(circle at top right, rgba(37, 99, 235, 0.16), transparent 28%)",
            pointerEvents: "none",
          }}
        />

        <div style={{ position: "relative", zIndex: 1 }}>
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
              fontWeight: 800,
              letterSpacing: "0.06em",
              textTransform: "uppercase",
              marginBottom: 18,
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
            Console premium ativo
          </div>

          <div style={{ maxWidth: 760 }}>
            <h1
              style={{
                margin: 0,
                color: COLORS.text,
                fontSize: 42,
                lineHeight: 1.02,
                letterSpacing: "-0.04em",
              }}
            >
              Bem-vindo, {firstName || "Founder"}
            </h1>

            <p
              style={{
                marginTop: 16,
                marginBottom: 0,
                color: COLORS.muted,
                fontSize: 17,
                lineHeight: 1.75,
                maxWidth: 760,
              }}
            >
              O shell da plataforma permanece intacto. Agora o centro da tela entrega
              uma primeira vitória mais clara: orientação executiva, ações úteis e
              percepção premium sem esconder threads, acessos ou navegação.
            </p>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
              gap: 14,
              marginTop: 24,
            }}
          >
            {[
              {
                title: "Execução auditável",
                description: "Cada ação nasce com contexto, rastreabilidade e leitura executiva.",
              },
              {
                title: "Camada premium real",
                description: "Mais contraste, mais valor percebido e menos sensação de MVP cru.",
              },
              {
                title: "Continuidade preservada",
                description: "Sidebar, threads, wallet e acessos continuam disponíveis.",
              },
            ].map((card) => (
              <div
                key={card.title}
                style={{
                  background: "rgba(15, 23, 42, 0.52)",
                  border: `1px solid ${COLORS.border}`,
                  borderRadius: 22,
                  padding: 18,
                }}
              >
                <div
                  style={{
                    color: COLORS.text,
                    fontSize: 15,
                    fontWeight: 800,
                    marginBottom: 8,
                  }}
                >
                  {card.title}
                </div>
                <div
                  style={{
                    color: COLORS.muted,
                    fontSize: 13,
                    lineHeight: 1.65,
                  }}
                >
                  {card.description}
                </div>
              </div>
            ))}
          </div>

          <div
            style={{
              display: "flex",
              gap: 12,
              flexWrap: "wrap",
              marginTop: 24,
            }}
          >
            <button
              onClick={handlePrimary}
              style={{
                border: "none",
                background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 100%)",
                color: "white",
                borderRadius: 16,
                padding: "15px 18px",
                fontSize: 14,
                fontWeight: 800,
                cursor: "pointer",
                boxShadow: "0 20px 40px rgba(76, 29, 149, 0.32)",
              }}
            >
              Iniciar conversa guiada
            </button>

            <button
              onClick={() => callMaybe(onSecondaryAction)}
              style={{
                border: "1px solid rgba(148, 163, 184, 0.18)",
                background: "rgba(15, 23, 42, 0.56)",
                color: COLORS.text,
                borderRadius: 16,
                padding: "15px 18px",
                fontSize: 14,
                fontWeight: 700,
                cursor: "pointer",
              }}
            >
              Abrir blueprint
            </button>

            <button
              onClick={() => callMaybe(onTertiaryAction)}
              style={{
                border: "1px solid rgba(148, 163, 184, 0.18)",
                background: "rgba(15, 23, 42, 0.56)",
                color: COLORS.text,
                borderRadius: 16,
                padding: "15px 18px",
                fontSize: 14,
                fontWeight: 700,
                cursor: "pointer",
              }}
            >
              Ver próximos passos
            </button>
          </div>

          <div style={{ marginTop: 22 }}>
            <div
              style={{
                color: COLORS.subtle,
                fontSize: 12,
                textTransform: "uppercase",
                letterSpacing: "0.12em",
                marginBottom: 10,
                fontWeight: 800,
              }}
            >
              Começos rápidos
            </div>

            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              {PROMPT_SUGGESTIONS.map((prompt) => (
                <button
                  key={prompt}
                  onClick={() => callMaybe(onFillPrompt, prompt)}
                  style={{
                    border: "1px solid rgba(148, 163, 184, 0.16)",
                    background: "rgba(2, 6, 23, 0.34)",
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

      <div
        style={{
          display: "grid",
          gap: 16,
          alignContent: "start",
        }}
      >
        <div
          style={{
            background: "linear-gradient(180deg, rgba(15, 23, 42, 0.94) 0%, rgba(9, 13, 24, 0.98) 100%)",
            border: `1px solid ${COLORS.border}`,
            borderRadius: 28,
            padding: 20,
            boxShadow: "0 24px 70px rgba(2, 6, 23, 0.32)",
          }}
        >
          <div
            style={{
              color: COLORS.muted,
              fontSize: 12,
              textTransform: "uppercase",
              letterSpacing: "0.12em",
              marginBottom: 10,
              fontWeight: 800,
            }}
          >
            Estado atual
          </div>

          <div
            style={{
              fontSize: 26,
              lineHeight: 1.1,
              fontWeight: 900,
              color: COLORS.text,
              marginBottom: 10,
            }}
          >
            Plataforma pronta para ação
          </div>

          <div
            style={{
              color: COLORS.muted,
              fontSize: 14,
              lineHeight: 1.7,
              marginBottom: 16,
            }}
          >
            A navegação lateral continua no lugar certo. O centro do console agora comunica
            valor mais rápido, com ações úteis e leitura operacional antes da primeira mensagem.
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
              gap: 10,
            }}
          >
            {[
              ["Threads", "Ativas"],
              ["Jornada", "Preservada"],
              ["UX", "Mais clara"],
              ["Valor", "Mais visível"],
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
                <div style={{ color: COLORS.text, fontSize: 14, fontWeight: 800 }}>
                  {value}
                </div>
              </div>
            ))}
          </div>
        </div>

        <div
          style={{
            background: "linear-gradient(180deg, rgba(15, 23, 42, 0.94) 0%, rgba(9, 13, 24, 0.98) 100%)",
            border: `1px solid ${COLORS.border}`,
            borderRadius: 28,
            padding: 18,
            boxShadow: "0 24px 70px rgba(2, 6, 23, 0.28)",
          }}
        >
          <div
            style={{
              color: COLORS.muted,
              fontSize: 12,
              textTransform: "uppercase",
              letterSpacing: "0.12em",
              marginBottom: 10,
              fontWeight: 800,
            }}
          >
            Direção de design
          </div>

          <div
            style={{
              display: "grid",
              gap: 12,
              color: COLORS.muted,
              fontSize: 14,
              lineHeight: 1.65,
            }}
          >
            {[
              "O premium passa a reforçar o centro do console sem apagar o shell do produto.",
              "A primeira vitória fica mais evidente com ações concretas e prompts iniciais.",
              "A sensação visual sobe por contraste, profundidade e hierarquia real.",
            ].map((item) => (
              <div
                key={item}
                style={{
                  display: "flex",
                  gap: 10,
                  alignItems: "flex-start",
                }}
              >
                <span
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: "50%",
                    background: "linear-gradient(135deg, #7c3aed, #2563eb)",
                    marginTop: 8,
                    flexShrink: 0,
                  }}
                />
                <span>{item}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
