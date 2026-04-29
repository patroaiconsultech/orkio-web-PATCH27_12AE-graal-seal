import React from "react";

const COLORS = {
  panel: "#0b1220",
  panelSoft: "#0f172a",
  border: "rgba(148, 163, 184, 0.14)",
  text: "#f8fafc",
  muted: "#94a3b8",
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
        padding: 20,
        boxShadow: "0 24px 60px rgba(2, 6, 23, 0.42)",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          gap: 12,
          flexWrap: "wrap",
          marginBottom: 16,
        }}
      >
        <div>
          <div
            style={{
              fontSize: 11,
              letterSpacing: "0.14em",
              textTransform: "uppercase",
              color: COLORS.muted,
              marginBottom: 6,
              fontWeight: 800,
            }}
          >
            Execution flow
          </div>
          <div
            style={{
              color: COLORS.text,
              fontSize: 20,
              lineHeight: 1.2,
              fontWeight: 900,
            }}
          >
            Linha de execução estratégica
          </div>
        </div>

        <div
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 10,
            background: COLORS.accentSoft,
            border: "1px solid rgba(124, 58, 237, 0.22)",
            color: "#ddd6fe",
            borderRadius: 999,
            padding: "9px 13px",
            fontSize: 12,
            fontWeight: 700,
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
          {total > 0 ? `${total} etapa${total > 1 ? "s" : ""}` : "Aguardando"}
        </div>
      </div>

      {total === 0 ? (
        <div
          style={{
            border: `1px dashed ${COLORS.border}`,
            borderRadius: 18,
            padding: 18,
            color: COLORS.muted,
            fontSize: 14,
            lineHeight: 1.65,
            background: "rgba(15, 23, 42, 0.55)",
          }}
        >
          A timeline aparecerá aqui com o avanço do diagnóstico, do despacho e da consolidação final.
        </div>
      ) : (
        <div style={{ display: "grid", gap: 12 }}>
          {steps.map((step, index) => {
            const isLast = index === total - 1;
            return (
              <div
                key={`${step.title}-${index}`}
                style={{
                  display: "grid",
                  gridTemplateColumns: "44px 1fr",
                  gap: 14,
                  alignItems: "flex-start",
                }}
              >
                <div
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    alignItems: "center",
                    minHeight: 72,
                  }}
                >
                  <div
                    style={{
                      width: 34,
                      height: 34,
                      borderRadius: 14,
                      background: isLast ? COLORS.successSoft : COLORS.accentSoft,
                      border: `1px solid ${isLast ? "rgba(34, 197, 94, 0.22)" : "rgba(124, 58, 237, 0.22)"}`,
                      color: isLast ? "#86efac" : "#ddd6fe",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      fontWeight: 800,
                      fontSize: 13,
                    }}
                  >
                    {index + 1}
                  </div>

                  {!isLast && (
                    <div
                      style={{
                        width: 2,
                        flex: 1,
                        marginTop: 8,
                        borderRadius: 999,
                        background:
                          "linear-gradient(180deg, rgba(124, 58, 237, 0.44) 0%, rgba(71, 85, 105, 0.1) 100%)",
                      }}
                    />
                  )}
                </div>

                <div
                  style={{
                    background: "rgba(15, 23, 42, 0.72)",
                    border: `1px solid ${COLORS.border}`,
                    borderRadius: 18,
                    padding: 16,
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "flex-start",
                      gap: 12,
                      flexWrap: "wrap",
                      marginBottom: 8,
                    }}
                  >
                    <strong style={{ color: COLORS.text, fontSize: 15, lineHeight: 1.3 }}>
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
                        padding: "5px 10px",
                        fontSize: 11,
                        fontWeight: 700,
                      }}
                    >
                      {isLast ? "Atual" : "Concluída"}
                    </span>
                  </div>

                  <div style={{ color: COLORS.muted, fontSize: 14, lineHeight: 1.65 }}>
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
