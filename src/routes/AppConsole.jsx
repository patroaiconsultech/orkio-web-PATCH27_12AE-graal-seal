import React, { useEffect, useMemo, useState } from "react";
import EmptyStatePremium from "../components/EmptyStatePremium";
import ExecutionTimeline from "../components/ExecutionTimeline";

const COLORS = {
  bg: "#020617",
  panel: "#0b1220",
  panelSoft: "#0f172a",
  border: "rgba(148, 163, 184, 0.14)",
  text: "#f8fafc",
  muted: "#94a3b8",
  subtle: "#64748b",
  accent: "#7c3aed",
  accent2: "#2563eb",
  success: "#22c55e",
  warning: "#f59e0b",
};

const initialMetrics = [
  { label: "Modo", value: "Superpremium" },
  { label: "Orquestração", value: "Ativa" },
  { label: "Saída", value: "Auditável" },
];

export default function AppConsole() {
  const [started, setStarted] = useState(false);
  const [timeline, setTimeline] = useState([]);
  const [logs, setLogs] = useState([]);
  const [running, setRunning] = useState(false);

  useEffect(() => {
    if (!started) return;

    setRunning(true);
    setTimeline([]);
    setLogs([]);

    const steps = [
      { title: "Inicializando", description: "Orion iniciou o diagnóstico com contexto operacional carregado." },
      { title: "Processando", description: "A análise do cenário foi organizada para reduzir ruído e priorizar clareza." },
      { title: "Executando", description: "O dispatch dos especialistas foi iniciado com rastreabilidade e foco na ação." },
      { title: "Finalizando", description: "A consolidação executiva está pronta para leitura e próxima decisão." },
    ];

    const logMessages = [
      "Runtime premium ativo",
      "Contexto estratégico carregado",
      "Dispatch multiagente em andamento",
      "Consolidação executiva concluída",
    ];

    let i = 0;
    const interval = setInterval(() => {
      if (i < steps.length) {
        setTimeline((prev) => [...prev, steps[i]]);
        setLogs((prev) => [...prev, logMessages[i]]);
        i += 1;
      } else {
        clearInterval(interval);
        setRunning(false);
      }
    }, 900);

    return () => clearInterval(interval);
  }, [started]);

  const progress = useMemo(() => {
    const total = 4;
    return Math.min(100, Math.round((timeline.length / total) * 100));
  }, [timeline.length]);

  const currentStage = useMemo(() => {
    if (!started) return "Pronto para iniciar";
    if (running) return timeline[timeline.length - 1]?.title || "Preparando";
    return "Execução concluída";
  }, [started, running, timeline]);

  if (!started) {
    return <EmptyStatePremium onStart={() => setStarted(true)} />;
  }

  return (
    <div
      style={{
        minHeight: "100vh",
        background:
          "radial-gradient(circle at top left, rgba(124, 58, 237, 0.16), transparent 28%), radial-gradient(circle at top right, rgba(37, 99, 235, 0.12), transparent 24%), #020617",
        color: COLORS.text,
        padding: 24,
        boxSizing: "border-box",
      }}
    >
      <div style={{ maxWidth: 1400, margin: "0 auto" }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "flex-start",
            gap: 20,
            marginBottom: 22,
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
                marginBottom: 10,
              }}
            >
              Operational console
            </div>
            <h1
              style={{
                margin: 0,
                fontSize: 34,
                lineHeight: 1.08,
                letterSpacing: "-0.03em",
              }}
            >
              Console de Execução Superpremium
            </h1>
            <p
              style={{
                marginTop: 12,
                marginBottom: 0,
                color: COLORS.muted,
                fontSize: 15,
                lineHeight: 1.7,
                maxWidth: 760,
              }}
            >
              Diagnóstico, despacho e consolidação apresentados com mais clareza,
              valor percebido e acabamento executivo.
            </p>
          </div>

          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 12,
              justifyContent: "flex-end",
            }}
          >
            {initialMetrics.map((metric) => (
              <div
                key={metric.label}
                style={{
                  minWidth: 140,
                  background: "rgba(15, 23, 42, 0.72)",
                  border: `1px solid ${COLORS.border}`,
                  borderRadius: 18,
                  padding: "14px 16px",
                }}
              >
                <div style={{ color: COLORS.subtle, fontSize: 12, marginBottom: 6 }}>
                  {metric.label}
                </div>
                <div style={{ fontSize: 16, fontWeight: 700 }}>{metric.value}</div>
              </div>
            ))}
          </div>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(0, 1.45fr) minmax(320px, 0.9fr)",
            gap: 22,
          }}
        >
          <div style={{ display: "grid", gap: 18 }}>
            <div
              style={{
                background: `linear-gradient(180deg, rgba(15, 23, 42, 0.88) 0%, rgba(11, 18, 32, 0.92) 100%)`,
                border: `1px solid ${COLORS.border}`,
                borderRadius: 28,
                padding: 24,
                boxShadow: "0 24px 60px rgba(2, 6, 23, 0.42)",
              }}
            >
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  gap: 16,
                  alignItems: "center",
                  flexWrap: "wrap",
                  marginBottom: 18,
                }}
              >
                <div>
                  <div style={{ fontSize: 13, color: COLORS.subtle, marginBottom: 8 }}>
                    Estado atual
                  </div>
                  <div style={{ fontSize: 24, fontWeight: 800 }}>{currentStage}</div>
                </div>

                <div
                  style={{
                    display: "inline-flex",
                    alignItems: "center",
                    gap: 10,
                    borderRadius: 999,
                    padding: "10px 14px",
                    background: running ? "rgba(124, 58, 237, 0.16)" : "rgba(34, 197, 94, 0.14)",
                    border: `1px solid ${running ? "rgba(124, 58, 237, 0.2)" : "rgba(34, 197, 94, 0.22)"}`,
                    color: running ? "#ddd6fe" : "#86efac",
                    fontSize: 13,
                    fontWeight: 700,
                  }}
                >
                  <span
                    style={{
                      width: 8,
                      height: 8,
                      borderRadius: "50%",
                      background: running ? COLORS.accent : COLORS.success,
                      boxShadow: running
                        ? "0 0 0 6px rgba(124, 58, 237, 0.18)"
                        : "0 0 0 6px rgba(34, 197, 94, 0.16)",
                    }}
                  />
                  {running ? "Execução em andamento" : "Execução concluída"}
                </div>
              </div>

              <div
                style={{
                  background: "rgba(2, 6, 23, 0.42)",
                  borderRadius: 999,
                  height: 12,
                  overflow: "hidden",
                  border: `1px solid ${COLORS.border}`,
                }}
              >
                <div
                  style={{
                    width: `${progress}%`,
                    height: "100%",
                    background: "linear-gradient(90deg, #7c3aed 0%, #2563eb 100%)",
                    borderRadius: 999,
                    transition: "width 300ms ease",
                  }}
                />
              </div>

              <div
                style={{
                  marginTop: 12,
                  display: "flex",
                  justifyContent: "space-between",
                  gap: 12,
                  flexWrap: "wrap",
                  color: COLORS.muted,
                  fontSize: 13,
                }}
              >
                <span>{progress}% do fluxo concluído</span>
                <span>{running ? "Condução guiada ativa" : "Pronto para nova rodada"}</span>
              </div>
            </div>

            <ExecutionTimeline steps={timeline} />
          </div>

          <div style={{ display: "grid", gap: 18 }}>
            <div
              style={{
                background: "rgba(15, 23, 42, 0.88)",
                border: `1px solid ${COLORS.border}`,
                borderRadius: 28,
                padding: 22,
                boxShadow: "0 24px 60px rgba(2, 6, 23, 0.42)",
              }}
            >
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  gap: 16,
                  alignItems: "center",
                  marginBottom: 16,
                }}
              >
                <div>
                  <div style={{ color: COLORS.subtle, fontSize: 12, marginBottom: 8 }}>
                    Logs executivos
                  </div>
                  <h3 style={{ margin: 0, fontSize: 22 }}>Telemetria de sessão</h3>
                </div>
                <div
                  style={{
                    color: running ? "#fcd34d" : "#86efac",
                    background: running ? "rgba(245, 158, 11, 0.14)" : "rgba(34, 197, 94, 0.14)",
                    border: `1px solid ${running ? "rgba(245, 158, 11, 0.22)" : "rgba(34, 197, 94, 0.22)"}`,
                    borderRadius: 999,
                    padding: "8px 12px",
                    fontSize: 12,
                    fontWeight: 700,
                  }}
                >
                  {running ? "Processando" : "Estável"}
                </div>
              </div>

              <div style={{ display: "grid", gap: 12 }}>
                {logs.length === 0 ? (
                  <div
                    style={{
                      padding: 18,
                      borderRadius: 18,
                      border: `1px dashed ${COLORS.border}`,
                      color: COLORS.muted,
                      lineHeight: 1.65,
                      fontSize: 14,
                    }}
                  >
                    Os logs aparecerão aqui conforme a execução avançar.
                  </div>
                ) : (
                  logs.map((log, index) => (
                    <div
                      key={`${log}-${index}`}
                      style={{
                        display: "flex",
                        gap: 12,
                        alignItems: "flex-start",
                        padding: 14,
                        borderRadius: 18,
                        background: "rgba(2, 6, 23, 0.38)",
                        border: `1px solid ${COLORS.border}`,
                      }}
                    >
                      <span
                        style={{
                          width: 10,
                          height: 10,
                          borderRadius: "50%",
                          background: index === logs.length - 1 && running ? COLORS.warning : COLORS.accent2,
                          marginTop: 6,
                          flexShrink: 0,
                        }}
                      />
                      <div style={{ color: COLORS.text, fontSize: 14, lineHeight: 1.65 }}>
                        {log}
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>

            <div
              style={{
                background: "rgba(11, 18, 32, 0.92)",
                border: `1px solid ${COLORS.border}`,
                borderRadius: 28,
                padding: 22,
                boxShadow: "0 24px 60px rgba(2, 6, 23, 0.42)",
              }}
            >
              <div style={{ color: COLORS.subtle, fontSize: 12, marginBottom: 8 }}>
                Direção de produto
              </div>
              <h3 style={{ marginTop: 0, fontSize: 22, marginBottom: 14 }}>
                Valor percebido mais alto
              </h3>

              <div style={{ display: "grid", gap: 14 }}>
                {[
                  "Hierarquia visual mais clara para o usuário entender o estado da sessão imediatamente.",
                  "Timeline com acabamento premium, menos cara de debug bruto e mais leitura executiva.",
                  "Logs transformados em telemetria legível, preservando percepção de controle.",
                ].map((item) => (
                  <div
                    key={item}
                    style={{
                      display: "flex",
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
                        background: COLORS.accent,
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
      </div>
    </div>
  );
}
