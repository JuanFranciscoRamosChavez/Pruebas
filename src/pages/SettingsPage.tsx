import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Bell, Shield, Clock, Save, RotateCcw } from "lucide-react";
import { toast } from "sonner";
import { useEffect, useState } from "react";

const SettingsPage = () => {
  // Estado local que refleja el config.yaml real
  const [settings, setSettings] = useState({
    app_name: "DataMask ETL",
    batch_size: 1000,
    extraction_window_days: 90,
    // Estos son visuales para la demo (no están en el yaml simple, pero los mantenemos en el estado)
    notifications_enabled: true,
    daily_summary: false,
    audit_logging: true,
    encryption: true,
    schedule_enabled: true,
    auto_retry: true,
    timeout_minutes: 30,
    email_contact: "admin@company.com"
  });

  // 1. Cargar configuración real al iniciar
  useEffect(() => {
    fetch('http://localhost:5000/api/settings')
      .then(res => res.json())
      .then(data => {
        // Mezclamos lo que viene del server con los defaults visuales
        setSettings(prev => ({ ...prev, ...data }));
        toast.success("Configuración cargada desde el servidor");
      })
      .catch(err => console.error("Error cargando settings:", err));
  }, []);

  // 2. Guardar cambios en el servidor
  const handleSave = async () => {
    const toastId = toast.loading("Guardando configuración...");
    try {
      const response = await fetch('http://localhost:5000/api/settings', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          // Solo enviamos al backend lo que el backend entiende (config.yaml)
          app_name: settings.app_name,
          batch_size: settings.batch_size,
          extraction_window_days: settings.extraction_window_days
        })
      });
      
      if(response.ok) {
        toast.dismiss(toastId);
        toast.success("Configuración guardada correctamente en config.yaml");
      } else {
        throw new Error("Error del servidor");
      }
    } catch (e) { 
        toast.dismiss(toastId);
        toast.error("Error guardando cambios"); 
    }
  };

  return (
    <Layout>
      <Header title="Configuración Global" description="Ajusta el comportamiento del sistema ETL" />
      
      <div className="p-6 space-y-6 max-w-3xl">
        
        {/* --- 1. PARÁMETROS REALES (ETL) --- */}
        <Card className="card-gradient border-border/50 border-l-4 border-l-primary">
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><DatabaseIcon className="h-5 w-5 text-primary" /> Parámetros del Motor ETL</CardTitle>
            <CardDescription>Configuración técnica que afecta la extracción de datos (config.yaml)</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
                <Label>Nombre de la Aplicación</Label>
                <Input value={settings.app_name} onChange={e => setSettings({...settings, app_name: e.target.value})} />
            </div>
            <div className="grid grid-cols-2 gap-4">
                <div>
                    <Label>Tamaño del Lote (Batch Size)</Label>
                    <Input type="number" value={settings.batch_size} onChange={e => setSettings({...settings, batch_size: parseInt(e.target.value)})} />
                    <p className="text-xs text-muted-foreground mt-1">Registros por transacción SQL.</p>
                </div>
                <div>
                    <Label>Ventana de Extracción (Días)</Label>
                    <Input type="number" value={settings.extraction_window_days} onChange={e => setSettings({...settings, extraction_window_days: parseInt(e.target.value)})} />
                    <p className="text-xs text-muted-foreground mt-1">Antigüedad máxima para carga inicial.</p>
                </div>
            </div>
          </CardContent>
        </Card>

        {/* --- 2. NOTIFICACIONES (Visual) --- */}
        <Card className="card-gradient border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Bell className="h-5 w-5 text-primary" /> Notificaciones</CardTitle>
            <CardDescription>Configura alertas sobre tus pipelines</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between">
              <div><Label>Alertas por email</Label><p className="text-sm text-muted-foreground">Recibe alertas cuando un pipeline falle</p></div>
              <Switch checked={settings.notifications_enabled} onCheckedChange={c => setSettings({...settings, notifications_enabled: c})} />
            </div>
            <div className="pt-2">
              <Label>Email de contacto</Label>
              <Input value={settings.email_contact} onChange={e => setSettings({...settings, email_contact: e.target.value})} className="mt-1.5"/>
            </div>
          </CardContent>
        </Card>

        {/* --- 3. SEGURIDAD (Visual) --- */}
        <Card className="card-gradient border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Shield className="h-5 w-5 text-primary" /> Seguridad</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between">
              <div><Label>Auditoría detallada</Label><p className="text-sm text-muted-foreground">Registrar JSON completo en BD QA</p></div>
              <Switch checked={settings.audit_logging} onCheckedChange={c => setSettings({...settings, audit_logging: c})} />
            </div>
          </CardContent>
        </Card>

        {/* --- 4. PROGRAMACIÓN (Visual) --- */}
        <Card className="card-gradient border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2"><Clock className="h-5 w-5 text-primary" /> Programación</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between">
              <div><Label>Scheduler Activo</Label><p className="text-sm text-muted-foreground">Ejecución automática (Cron)</p></div>
              <Switch checked={settings.schedule_enabled} onCheckedChange={c => setSettings({...settings, schedule_enabled: c})} />
            </div>
            <div className="flex items-center justify-between">
              <div><Label>Reintentos automáticos</Label><p className="text-sm text-muted-foreground">Reintentar 3 veces ante fallo de red</p></div>
              <Switch checked={settings.auto_retry} onCheckedChange={c => setSettings({...settings, auto_retry: c})} />
            </div>
          </CardContent>
        </Card>

        {/* Botones */}
        <div className="flex items-center gap-3 pt-4">
          <Button onClick={handleSave} className="w-full md:w-auto"><Save className="mr-2 h-4 w-4"/> Guardar Cambios</Button>
          <Button variant="outline" onClick={() => window.location.reload()}><RotateCcw className="mr-2 h-4 w-4"/> Cancelar</Button>
        </div>
      </div>
    </Layout>
  );
};

// Icono auxiliar
function DatabaseIcon({className}: {className?: string}) {
    return <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>;
}

export default SettingsPage;