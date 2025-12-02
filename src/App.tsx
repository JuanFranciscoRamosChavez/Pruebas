import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { useState, useEffect } from "react";
import Index from "./pages/Index";
import Pipelines from "./pages/Pipelines";
import Masking from "./pages/Masking";
import History from "./pages/History";
import Connections from "./pages/Connections";
import SettingsPage from "./pages/SettingsPage";
import NotFound from "./pages/NotFound";
import Login from "./pages/Login";

const queryClient = new QueryClient();

const App = () => {
  // Estado para manejar el rol del usuario (simulado en localStorage)
  const [userRole, setUserRole] = useState<string | null>(localStorage.getItem("userRole"));

  const handleLogin = (role: 'admin' | 'operator') => {
    localStorage.setItem("userRole", role);
    setUserRole(role);
  };

  const handleLogout = () => {
    localStorage.removeItem("userRole");
    setUserRole(null);
  };

  // Si no hay usuario logueado, mostramos Login
  if (!userRole) {
    return (
      <QueryClientProvider client={queryClient}>
        <TooltipProvider>
          <Toaster />
          <Sonner />
          <Login onLogin={handleLogin} />
        </TooltipProvider>
      </QueryClientProvider>
    );
  }

  // Si hay usuario, mostramos la app completa pasando el rol
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Sonner />
        <BrowserRouter>
          <Routes>
            {/* Pasamos el rol como prop a las páginas que lo necesiten */}
            <Route path="/" element={<Index />} />
            <Route path="/pipelines" element={<Pipelines userRole={userRole} />} />
            <Route path="/masking" element={<Masking userRole={userRole} />} />
            <Route path="/history" element={<History />} />
            <Route path="/connections" element={<Connections />} />
            <Route path="/settings" element={<SettingsPage />} />
            <Route path="*" element={<NotFound />} />
          </Routes>
        </BrowserRouter>
      </TooltipProvider>
    </QueryClientProvider>
  );
};

export default App;