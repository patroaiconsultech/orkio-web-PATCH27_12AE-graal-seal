
import React from "react";

export default function OrkioSphereMark({
  size = 40,
  ring = true,
  glow = true,
  badge = false,
  className = "",
  style = {},
}) {
  const haloSize = Math.round(size * 1.9);
  const ringSize = Math.round(size * 1.28);

  return (
    <div
      className={className}
      style={{
        position: "relative",
        width: size,
        height: size,
        flex: `0 0 ${size}px`,
        ...style,
      }}
      aria-hidden="true"
    >
      {glow ? (
        <span
          style={{
            position: "absolute",
            inset: "50% auto auto 50%",
            width: haloSize,
            height: haloSize,
            borderRadius: "999px",
            transform: "translate(-50%, -50%)",
            background:
              "radial-gradient(circle, rgba(56,189,248,0.34) 0%, rgba(124,58,237,0.16) 38%, rgba(6,8,18,0) 72%)",
            filter: "blur(12px)",
            opacity: 0.95,
          }}
        />
      ) : null}

      {ring ? (
        <span
          style={{
            position: "absolute",
            inset: "50% auto auto 50%",
            width: ringSize,
            height: ringSize,
            borderRadius: "999px",
            transform: "translate(-50%, -50%)",
            border: "1px solid rgba(191,219,254,0.24)",
            boxShadow: "0 0 0 1px rgba(255,255,255,0.04) inset",
          }}
        />
      ) : null}

      <span
        style={{
          position: "absolute",
          inset: 0,
          borderRadius: "999px",
          background:
            "radial-gradient(circle at 28% 24%, rgba(255,255,255,0.96) 0%, rgba(221,242,255,0.88) 12%, rgba(127,219,255,0.82) 22%, rgba(74,167,255,0.95) 36%, rgba(103,90,255,0.98) 58%, rgba(55,30,108,1) 80%, rgba(8,10,24,1) 100%)",
          boxShadow:
            "inset -12px -18px 24px rgba(5,8,20,0.45), inset 8px 10px 18px rgba(255,255,255,0.28), 0 22px 50px rgba(34,211,238,0.14), 0 18px 36px rgba(79,70,229,0.22)",
        }}
      />
      <span
        style={{
          position: "absolute",
          inset: `${size * 0.16}px ${size * 0.18}px auto auto`,
          width: Math.max(8, Math.round(size * 0.2)),
          height: Math.max(8, Math.round(size * 0.2)),
          borderRadius: "999px",
          background: "rgba(255,255,255,0.82)",
          filter: "blur(0.4px)",
          opacity: 0.9,
        }}
      />
      <span
        style={{
          position: "absolute",
          left: "50%",
          top: "62%",
          width: Math.round(size * 0.52),
          height: Math.max(4, Math.round(size * 0.1)),
          transform: "translateX(-50%)",
          borderRadius: "999px",
          background: "rgba(255,255,255,0.17)",
          boxShadow: "0 1px 4px rgba(0,0,0,0.16) inset",
          opacity: 0.8,
        }}
      />
      {badge ? (
        <span
          style={{
            position: "absolute",
            right: -2,
            bottom: -2,
            width: Math.max(12, Math.round(size * 0.26)),
            height: Math.max(12, Math.round(size * 0.26)),
            borderRadius: "999px",
            background: "linear-gradient(135deg,#22c55e,#14b8a6)",
            border: "2px solid rgba(7,9,16,0.9)",
            boxShadow: "0 8px 18px rgba(16,185,129,0.35)",
          }}
        />
      ) : null}
    </div>
  );
}
