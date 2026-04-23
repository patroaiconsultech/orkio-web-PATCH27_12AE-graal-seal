import React from "react";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";

import Landing from "./routes/Landing.jsx";
import AuthPage from "./routes/AuthPage.jsx";
import AppConsole from "./routes/AppConsole.jsx";
import AdminConsole from "./routes/AdminConsole.jsx";
import AdminEscalations from "./routes/AdminEscalations.jsx";
import Contact from "./routes/Contact.jsx";
import AdminTrademarkCenter from "./routes/AdminTrademarkCenter.jsx";
import AdminValuationCenter from "./routes/AdminValuationCenter.jsx";
import AdminEvolutionCenter from "./routes/AdminEvolutionCenter.jsx";
import BillingWalletCenter from "./routes/BillingWalletCenter.jsx";
import PrivacySettings from "./routes/PrivacySettings.jsx";
import Privacy from "./routes/legal/Privacy.jsx";
import Terms from "./routes/legal/Terms.jsx";
import Cookies from "./routes/legal/Cookies.jsx";
import AiUsage from "./routes/legal/AiUsage.jsx";
import AiGovernance from "./routes/legal/AiGovernance.jsx";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Landing />} />
        <Route path="/auth" element={<AuthPage />} />
        <Route path="/app" element={<AppConsole />} />
        <Route path="/admin" element={<AdminConsole />} />
        <Route path="/admin/escalations" element={<AdminEscalations />} />
        <Route path="/admin/trademarks" element={<AdminTrademarkCenter />} />
        <Route path="/admin/valuation" element={<AdminValuationCenter />} />
        <Route path="/admin/evolution" element={<AdminEvolutionCenter />} />
        <Route path="/wallet" element={<BillingWalletCenter />} />
        <Route path="/contact" element={<Contact />} />
        <Route path="/privacy-settings" element={<PrivacySettings />} />
        <Route path="/legal/privacy" element={<Privacy />} />
        <Route path="/legal/terms" element={<Terms />} />
        <Route path="/legal/cookies" element={<Cookies />} />
        <Route path="/legal/ai-usage" element={<AiUsage />} />
        <Route path="/legal/ai-governance" element={<AiGovernance />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}
