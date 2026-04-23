
import React, { useEffect, useMemo, useState } from "react";
import OrkioSphereMark from "../ui/OrkioSphereMark.jsx";

export default function PWAInstallPrompt() {
  const [deferredPrompt, setDeferredPrompt] = useState(null);
  const [dismissed, setDismissed] = useState(false);

  const isIOS = useMemo(() => {
    if (typeof navigator === "undefined") return false;
    return /iphone|ipad|ipod/i.test(navigator.userAgent || "");
  }, []);

  useEffect(() => {
    if (window.matchMedia && window.matchMedia("(display-mode: standalone)").matches) return;
    if (localStorage.getItem("orkio_pwa_dismissed") === "1") {
      setDismissed(true);
      return;
    }
    const onPrompt = (e) => {
      e.preventDefault();
      setDeferredPrompt(e);
    };
    window.addEventListener("beforeinstallprompt", onPrompt);
    return () => window.removeEventListener("beforeinstallprompt", onPrompt);
  }, []);

  if (dismissed) return null;
  if (!deferredPrompt && !isIOS) return null;

  const install = async () => {
    if (!deferredPrompt) return;
    deferredPrompt.prompt();
    await deferredPrompt.userChoice;
    setDeferredPrompt(null);
  };

  const dismiss = () => {
    localStorage.setItem("orkio_pwa_dismissed", "1");
    setDismissed(true);
  };

  return (
    <div
      style={{
        position: "fixed",
        left: 16,
        right: 16,
        bottom: 92,
        zIndex: 60,
        margin: "0 auto",
        maxWidth: 780,
        padding: 14,
        borderRadius: 22,
        border: "1px solid rgba(255,255,255,0.10)",
        background:
          "linear-gradient(135deg, rgba(9,12,24,0.95), rgba(13,18,36,0.96))",
        color: "#fff",
        display: "flex",
        gap: 14,
        alignItems: "center",
        justifyContent: "space-between",
        boxShadow: "0 18px 54px rgba(0,0,0,.42)",
        backdropFilter: "blur(18px)",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
        <OrkioSphereMark size={34} badge={false} />
        <div style={{ fontSize: 12 }}>
          <div style={{ fontWeight: 800, fontSize: 13 }}>Install Orkio</div>
          <div style={{ opacity: 0.78, maxWidth: 420 }}>
            {isIOS && !deferredPrompt
              ? "Tap Share and then Add to Home Screen."
              : "Keep Orkio one tap away with the full-screen PWA experience."}
          </div>
        </div>
      </div>
      <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
        <button
          type="button"
          onClick={dismiss}
          style={{
            padding: "10px 12px",
            borderRadius: 12,
            border: "1px solid rgba(255,255,255,0.1)",
            background: "transparent",
            color: "#fff",
            fontWeight: 700,
          }}
        >
          Later
        </button>
        {!isIOS || deferredPrompt ? (
          <button
            type="button"
            onClick={install}
            style={{
              padding: "10px 14px",
              borderRadius: 12,
              border: 0,
              background: "linear-gradient(135deg,#67e8f9,#60a5fa,#8b5cf6)",
              color: "#08111f",
              fontWeight: 900,
              boxShadow: "0 14px 30px rgba(96,165,250,0.25)",
            }}
          >
            Install now
          </button>
        ) : null}
      </div>
    </div>
  );
}
