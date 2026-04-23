
import React from "react";
import { Outlet, useNavigate } from "react-router-dom";
import PWAInstallPrompt from "../components/PWAInstallPrompt.jsx";
import Footer from "./Footer.jsx";
import OrkioSphereMark from "./OrkioSphereMark.jsx";
import { getToken, getUser, isAdmin } from "../lib/auth.js";

export default function Layout() {
  const nav = useNavigate();
  const token = getToken();
  const user = getUser();
  const admin = !!token && isAdmin(user);

  return (
    <div className="min-h-screen bg-[#060812] text-white flex flex-col">
      <div className="sticky top-0 z-40 border-b border-white/8 bg-[#060812]/78 backdrop-blur-xl">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-3">
          <button
            type="button"
            onClick={() => nav(token ? "/app" : "/")}
            className="flex items-center gap-3 rounded-2xl border border-white/8 bg-white/5 px-3 py-2 text-left transition hover:bg-white/8"
          >
            <OrkioSphereMark size={28} badge={false} />
            <div>
              <div className="text-[11px] font-black uppercase tracking-[0.28em] text-white/45">Orkio</div>
              <div className="text-sm font-semibold text-white/88">Execution intelligence</div>
            </div>
          </button>

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => nav(token ? "/app" : "/auth")}
              className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-white/88 transition hover:bg-white/10"
            >
              {token ? "Open app" : "Sign in"}
            </button>
            {admin ? (
              <button
                type="button"
                onClick={() => nav("/admin")}
                className="rounded-xl bg-gradient-to-r from-cyan-300 via-sky-400 to-violet-500 px-4 py-2 text-sm font-black text-slate-950 transition hover:brightness-110"
              >
                Admin
              </button>
            ) : null}
          </div>
        </div>
      </div>

      <div className="flex-1">
        <Outlet />
      </div>

      <PWAInstallPrompt />
      <Footer />
    </div>
  );
}
