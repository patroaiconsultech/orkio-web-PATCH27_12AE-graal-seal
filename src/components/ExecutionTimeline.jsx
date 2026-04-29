import React from "react";

const COLORS = {
  bg: "#020617",
  panel: "#0b1220",
  panelSoft: "#0f172a",
  border: "rgba(148, 163, 184, 0.14)",
  text: "#f8fafc",
  muted: "#94a3b8",
  subtle: "#64748b",
  accent: "#7c3aed",
  accentSoft: "rgba(124, 58, 237, 0.18)",
  success: "#22c55e",
  successSoft: "rgba(34, 197, 94, 0.14)",
};

export default function ExecutionTimeline({ steps }) {
  const total = Array.isArray(steps) ? steps.length : 0;

  return (
    <div
      style={{
        background: `linear-gradient(180deg, ${COLORS.panelSoft} 0%, ${COLORS.panel} 100%)`,
        border: `1px solid ${COLORS.border}`,
        borderRadius: 24,
        padding: 24,
        boxShadow: "0 24px 60px rgba(2, 6, 23, 0.45)",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: 16,
          alignItems: "center",
          marginBottom: 20,
          flexWrap: "wrap",
        }}
      >
        <div>
          <div
            style={{
              fontSize: 12,
              letterSpacing: "0.14em",
              textTransform: "uppercase",
              color: COLORS.muted,
              marginBottom: 8,
            }}
          >
            Execution flow
          </div>
          <h3
            style={{
              margin: 0,
              color: COLORS.text,
              fontSize: 22,
              lineHeight: 1.2,
            }}
          >
            Linha de execução estratégica
          </h3>
        </div>

        <div
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 10,
            background: COLORS.accentSoft,
            border: `1px solid rgba(124, 58, 237, 0.22)`,
            color: "#ddd6fe",
            borderRadius: 999,
            padding: "10px 14px",
            fontSize: 13,
            fontWeight: 600,
          }}
        >
          <span
            style={{
              width: 8,
              height: 8,
              borderRadius: "50%",
              background: COLORS.accent,
              boxShadow: "0 0 0 6px rgba(124, 58, 237, 0.18)",
              display: "inline-block",
            }}
          />
          {total > 0 ? `${total} etapa${total > 1 ? "s" : ""} concluída${total > 1 ? "s" : ""}` : "Aguardando início"}
        </div>
      </div>

      {total === 0 ? (
        <div
          style={{
            border: `1px dashed ${COLORS.border}`,
            borderRadius: 18,
            padding: 22,
            background: "rgba(15, 23, 42, 0.55)",
          }}
        >
          <div
            style={{
              fontSize: 15,
              color: COLORS.text,
              fontWeight: 600,
              marginBottom: 8,
            }}
          >
            Nenhuma execução iniciada
          </div>
          <div style={{ color: COLORS.muted, fontSize: 14, lineHeight: 1.6 }}>
            Quando a sessão começar, a timeline mostrará o avanço do diagnóstico,
            o despacho dos especialistas e a consolidação executiva final.
          </div>
        </div>
      ) : (
        <div style={{ display: "grid", gap: 14 }}>
          {steps.map((step, index) => {
            const isLast = index === total - 1;

            return (
              <div
                key={`${step.title}-${index}`}
                style={{
                  display: "grid",
                  gridTemplateColumns: "52px 1fr",
                  gap: 16,
                  alignItems: "flex-start",
                }}
              >
                <div
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    alignItems: "center",
                    minHeight: 74,
                  }}
                >
                  <div
                    style={{
                      width: 40,
                      height: 40,
                      borderRadius: 16,
                      background: isLast ? COLORS.successSoft : COLORS.accentSoft,
                      border: `1px solid ${isLast ? "rgba(34, 197, 94, 0.22)" : "rgba(124, 58, 237, 0.22)"}`,
                      color: isLast ? "#86efac" : "#ddd6fe",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      fontWeight: 700,
                      fontSize: 14,
                    }}
                  >
                    {String(index + 1).padStart(2, "0")}
                  </div>

                  {!isLast && (
                    <div
                      style={{
                        width: 2,
                        flex: 1,
                        marginTop: 10,
                        borderRadius: 999,
                        background:
                          "linear-gradient(180deg, rgba(124, 58, 237, 0.45) 0%, rgba(71, 85, 105, 0.1) 100%)",
                      }}
                    />
                  )}
                </div>

                <div
                  style={{
                    background: "rgba(15, 23, 42, 0.72)",
                    border: `1px solid ${COLORS.border}`,
                    borderRadius: 18,
                    padding: 18,
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      gap: 12,
                      alignItems: "flex-start",
                      marginBottom: 8,
                      flexWrap: "wrap",
                    }}
                  >
                    <strong
                      style={{
                        color: COLORS.text,
                        fontSize: 16,
                        lineHeight: 1.3,
                      }}
                    >
                      {step.title}
                    </strong>

                    <span
                      style={{
                        display: "inline-flex",
                        alignItems: "center",
                        gap: 8,
                        color: isLast ? "#86efac" : "#c4b5fd",
                        background: isLast ? COLORS.successSoft : COLORS.accentSoft,
                        border: `1px solid ${isLast ? "rgba(34, 197, 94, 0.22)" : "rgba(124, 58, 237, 0.22)"}`,
                        borderRadius: 999,
                        padding: "6px 10px",
                        fontSize: 12,
                        fontWeight: 600,
                      }}
                    >
                      {isLast ? "Consolidação atual" : "Etapa concluída"}
                    </span>
                  </div>

                  <div
                    style={{
                      color: COLORS.muted,
                      fontSize: 14,
                      lineHeight: 1.65,
                    }}
                  >
                    {step.description}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
