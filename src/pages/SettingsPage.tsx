import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { 
  Shield, 
  Clock, 
  Save, 
  RotateCcw,
  Database as DatabaseIcon,
  FileText, // Icono para el reporte
  Download  // Icono para el botón de descarga
} from "lucide-react";
import { toast } from "sonner";
import { useEffect, useState } from "react";

// Estructura de datos que coincide con el backend
interface AppSettings {
  app_name: string;
  batch_size: number;
  extraction_window_days: number;
  notifications: {
    enabled: boolean;
    email: string;
    daily_summary: boolean;
  };
  security: {
    audit_detailed: boolean;
    encryption_at_rest: boolean;
    log_retention_days: number;
  };
  scheduler: {
    enabled: boolean;
    auto_retry: boolean;
    timeout_minutes: number;
    interval_minutes: number;
  };
}

// Valores iniciales seguros
const defaultSettings: AppSettings = {
  app_name: "DataMask ETL",
  batch_size: 1000,
  extraction_window_days: 90,
  notifications: { enabled: false, email: "", daily_summary: false },
  security: { audit_detailed: true, encryption_at_rest: false, log_retention_days: 90 },
  scheduler: { enabled: false, auto_retry: false, timeout_minutes: 30, interval_minutes: 5 }
};

const SettingsPage = () => {
  const [settings, setSettings] = useState<AppSettings>(defaultSettings);
  const [loading, setLoading] = useState(true);

  // 1. Cargar configuración desde el Backend
  useEffect(() => {
    fetch('http://localhost:5000/api/settings')
      .then(res => {
        if (!res.ok) throw new Error("Error de conexión");
        return res.json();
      })
      .then(data => {
        // Fusionar con defaults para evitar errores si faltan campos
        setSettings(prev => ({
          ...prev,
          ...data,
          notifications: { ...prev.notifications, ...(data.notifications || {}) },
          security: { ...prev.security, ...(data.security || {}) },
          scheduler: { ...prev.scheduler, ...(data.scheduler || {}) },
        }));
        toast.success("Configuración cargada");
      })
      .catch(() => toast.error("No se pudo cargar la configuración"))
      .finally(() => setLoading(false));
  }, []);

  // 2. Guardar cambios en el Backend
  const handleSave = async () => {
    const toastId = toast.loading("Aplicando cambios...");
    try {
      const response = await fetch('http://localhost:5000/api/settings', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(settings)
      });

      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Configuración guardada exitosamente");
      } else {
        throw new Error("Error del servidor");
      }
    } catch (e) {
      toast.dismiss(toastId);
      toast.error("Error al guardar configuración");
    }
  };

  // Helpers para actualizar el estado anidado limpiamente
  const updateNotification = (key: string, val: any) => 
    setSettings(p => ({...p, notifications: {...p.notifications, [key]: val}}));
  
  const updateSecurity = (key: string, val: any) => 
    setSettings(p => ({...p, security: {...p.security, [key]: val}}));
  
  const updateScheduler = (key: string, val: any) => 
    setSettings(p => ({...p, scheduler: {...p.scheduler, [key]: val}}));

  if (loading) {
      return <Layout><div className="p-12 text-center text-muted-foreground">Cargando configuración...</div></Layout>;
  }

  return (
    <Layout>
      <Header 
        title="Configuración Global" 
        description="Ajusta el comportamiento del sistema y las preferencias"
      />
      
      <div className="p-6 space-y-6 max-w-3xl">
        
        {/* SECCIÓN 1: MOTOR ETL (CORE) */}
        <Card className="card-gradient border-border/50 border-l-4 border-l-primary">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <DatabaseIcon className="h-5 w-5 text-primary" />
              Motor ETL
            </CardTitle>
            <CardDescription>Parámetros técnicos de ejecución</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
                <Label>Nombre de la Aplicación</Label>
                <Input 
                    value={settings.app_name} 
                    onChange={e => setSettings({...settings, app_name: e.target.value})} 
                />
            </div>
            <div className="grid grid-cols-2 gap-4">
                <div>
                    <Label>Batch Size</Label>
                    <Input 
                        type="number" 
                        value={settings.batch_size} 
                        onChange={e => setSettings({...settings, batch_size: Number(e.target.value)})} 
                    />
                    <p className="text-xs text-muted-foreground mt-1">Registros por lote</p>
                </div>
                <div>
                    <Label>Ventana Histórica (Días)</Label>
                    <Input 
                        type="number" 
                        value={settings.extraction_window_days} 
                        onChange={e => setSettings({...settings, extraction_window_days: Number(e.target.value)})} 
                    />
                </div>
            </div>
          </CardContent>
        </Card>

        {/* SECCIÓN 2: REPORTE Y AUDITORÍA (REEMPLAZA EMAIL) */}
        <Card className="card-gradient border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FileText className="h-5 w-5 text-orange-400" />
              Reporte de Auditoría
            </CardTitle>
            <CardDescription>
              Generación y descarga de logs locales (JSON)
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <Label>Generar historial JSON</Label>
                <p className="text-sm text-muted-foreground">Guardar copia local de cada ejecución</p>
              </div>
              {/* Reusamos el campo 'enabled' para activar la generación de JSON */}
              <Switch 
                checked={settings.notifications.enabled} 
                onCheckedChange={c => updateNotification('enabled', c)} 
              />
            </div>
            
            {/* BOTÓN DE DESCARGA */}
            <div className="pt-4 border-t border-border/30">
                <Button 
                    variant="outline" 
                    className="w-full sm:w-auto gap-2"
                    onClick={() => window.open('http://localhost:5000/api/notifications/report', '_blank')}
                >
                    <Download className="h-4 w-4" /> Descargar Reporte JSON
                </Button>
                <p className="text-xs text-muted-foreground mt-2">
                    Descarga el archivo acumulado de ejecuciones exitosas y fallidas.
                </p>
            </div>
          </CardContent>
        </Card>

        {/* SECCIÓN 3: SEGURIDAD */}
        <Card className="card-gradient border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Shield className="h-5 w-5 text-green-500" />
              Seguridad
            </CardTitle>
            <CardDescription>Configuraciones de seguridad y auditoría</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <Label>Registro de auditoría detallado</Label>
                <p className="text-sm text-muted-foreground">Guarda JSON completo de la operación</p>
              </div>
              <Switch 
                checked={settings.security.audit_detailed} 
                onCheckedChange={c => updateSecurity('audit_detailed', c)} 
              />
            </div>
            <div className="flex items-center justify-between">
              <div>
                <Label>Encriptación de logs</Label>
                <p className="text-sm text-muted-foreground">Encripta los logs en reposo</p>
              </div>
              <Switch 
                checked={settings.security.encryption_at_rest} 
                onCheckedChange={c => updateSecurity('encryption_at_rest', c)} 
              />
            </div>
            <div className="pt-2">
              <Label>Retención de logs (días)</Label>
              <Input 
                type="number" 
                value={settings.security.log_retention_days} 
                onChange={e => updateSecurity('log_retention_days', Number(e.target.value))}
                className="mt-1.5 w-32"
              />
            </div>
          </CardContent>
        </Card>

        {/* SECCIÓN 4: PROGRAMACIÓN (SCHEDULER) */}
        <Card className="card-gradient border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="h-5 w-5 text-blue-400" />
              Programación Automática
            </CardTitle>
            <CardDescription>Configuración de ejecuciones automáticas</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex justify-between pb-2 border-b border-border/30">
                <Label className="text-base font-semibold">Scheduler Activo</Label>
                <Switch checked={settings.scheduler.enabled} onCheckedChange={c => updateScheduler('enabled', c)} />
            </div>
            <div className="grid grid-cols-2 gap-6 pt-2">
                <div>
                    <Label>Frecuencia (Minutos)</Label>
                    <Input type="number" min={1} value={settings.scheduler.interval_minutes} onChange={e => updateScheduler('interval_minutes', Number(e.target.value))} className="mt-1.5 font-mono"/>
                </div>
                <div className="flex flex-col justify-end pb-2">
                    <div className="flex justify-between items-center">
                        <Label>Auto-Retry</Label>
                        <Switch checked={settings.scheduler.auto_retry} onCheckedChange={c => updateScheduler('auto_retry', c)} />
                    </div>
                </div>
            </div>
          </CardContent>
        </Card>

        {/* BOTONERA DE ACCIONES */}
        <div className="flex items-center gap-3 pt-4 pb-8">
          <Button onClick={handleSave} className="w-full md:w-auto px-8">
            <Save className="mr-2 h-4 w-4" />
            Guardar Cambios
          </Button>
          <Button variant="outline" onClick={() => window.location.reload()}>
            <RotateCcw className="mr-2 h-4 w-4" />
            Restaurar Valores
          </Button>
        </div>
      </div>
    </Layout>
  );
};

export default SettingsPage;