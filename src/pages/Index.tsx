import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { StatsCard } from "@/components/StatsCard";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { 
  GitBranch, Shield, Database, Activity, 
  Server, CheckCircle2, XCircle, Clock, ArrowRight 
} from "lucide-react";
import { Link } from "react-router-dom";
import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts';

const Index = () => {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    fetch('http://localhost:5000/api/dashboard')
      .then(res => {
        if (!res.ok) throw new Error("Error de red");
        return res.json();
      })
      .then(setData)
      .catch(err => {
        console.error("Fallo al cargar dashboard:", err);
        setError(true);
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) return (
    <Layout>
      <div className="p-8 flex items-center justify-center h-full min-h-[500px]">
        <div className="animate-pulse flex flex-col items-center">
          <Activity className="h-10 w-10 text-primary mb-4 animate-spin" />
          <p className="text-muted-foreground">Conectando con el Centro de Comando...</p>
        </div>
      </div>
    </Layout>
  );

  if (error || !data) return (
    <Layout>
       <div className="p-8 text-center">
         <h2 className="text-xl font-bold text-red-500">Sistema Offline</h2>
         <p className="text-muted-foreground">No se pudo conectar con el Backend API.</p>
         <Button className="mt-4" onClick={() => window.location.reload()}>Reintentar</Button>
       </div>
    </Layout>
  );

  // Extracción segura con valores por defecto
  const kpi = data.kpi || { pipelines: 0, rules: 0, records: 0, success_rate: 0 };
  const chart_data = data.chart_data || [];
  const recent_activity = data.recent_activity || [];
  const system_status = data.system_status || { api: "offline", scheduler: "offline", db_prod: "offline", db_qa: "offline" };

  return (
    <Layout>
      <Header title="Centro de Comando" description="Visión general del ecosistema de datos" />
      
      <div className="p-6 space-y-6">
        
        {/* 1. KPIS PRINCIPALES */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <StatsCard title="Pipelines Activos" value={kpi.pipelines} description="Flujos configurados" icon={GitBranch} />
          <StatsCard title="Reglas de Seguridad" value={kpi.rules} description="Columnas protegidas" icon={Shield} variant="primary" />
          <StatsCard title="Registros Migrados" value={kpi.records.toLocaleString()} description="Volumen total histórico" icon={Database} />
          <StatsCard title="Salud del ETL" value={`${kpi.success_rate}%`} description="Tasa de éxito global" icon={Activity} variant="success" />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          
          {/* 2. GRÁFICA DE VOLUMEN (Ocupa 2 columnas) */}
          <Card className="lg:col-span-2 card-gradient border-border/50 shadow-sm">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Database className="h-5 w-5 text-blue-500" />
                Volumen por Tabla
              </CardTitle>
              <CardDescription>Distribución de registros procesados en QA</CardDescription>
            </CardHeader>
            <CardContent className="h-[300px]">
              {chart_data.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={chart_data} layout="vertical" margin={{ left: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#374151" opacity={0.2} />
                    <XAxis type="number" hide />
                    <YAxis dataKey="name" type="category" width={120} tick={{fontSize: 12}} />
                    <Tooltip 
                        contentStyle={{ backgroundColor: '#1f2937', borderColor: '#374151', color: '#f3f4f6' }}
                        cursor={{fill: 'transparent'}}
                    />
                    <Bar dataKey="value" radius={[0, 4, 4, 0]} barSize={30}>
                        {chart_data.map((entry: any, index: number) => (
                        <Cell key={`cell-${index}`} fill={index % 2 === 0 ? '#3b82f6' : '#6366f1'} />
                        ))}
                    </Bar>
                    </BarChart>
                </ResponsiveContainer>
              ) : (
                  <div className="h-full flex items-center justify-center text-muted-foreground">
                      No hay datos suficientes para graficar.
                  </div>
              )}
            </CardContent>
          </Card>

          {/* 3. MONITOR DE ESTADO (Columna derecha) */}
          <div className="space-y-6">
            <Card className="border-border/50 shadow-sm">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Server className="h-5 w-5 text-purple-500" />
                  Estado de Infraestructura
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <SystemStatusRow label="API Backend" status={system_status.api} />
                <SystemStatusRow label="Job Scheduler" status={system_status.scheduler} />
                <SystemStatusRow label="DB Producción" status={system_status.db_prod} />
                <SystemStatusRow label="DB QA (Destino)" status={system_status.db_qa} />
              </CardContent>
            </Card>

            <Card className="bg-primary/5 border-primary/20">
              <CardContent className="p-6 flex flex-col items-center text-center space-y-3">
                <div className="p-3 bg-primary/20 rounded-full">
                   <GitBranch className="h-6 w-6 text-primary" />
                </div>
                <div>
                  <h3 className="font-bold text-foreground">Gestionar Pipelines</h3>
                  <p className="text-xs text-muted-foreground mt-1">Configura nuevas extracciones o ejecuta manualmente.</p>
                </div>
                <Button asChild className="w-full mt-2">
                  <Link to="/pipelines">Ir al Panel <ArrowRight className="ml-2 h-4 w-4" /></Link>
                </Button>
              </CardContent>
            </Card>
          </div>
        </div>

        {/* 4. FEED DE ACTIVIDAD RECIENTE */}
        <Card className="border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="h-5 w-5 text-orange-500" />
              Actividad Reciente
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {recent_activity.map((item: any, idx: number) => (
                <div key={idx} className="flex items-center justify-between border-b border-border/40 last:border-0 pb-3 last:pb-0">
                  <div className="flex items-center gap-3">
                    {item.status === 'success' 
                      ? <CheckCircle2 className="h-5 w-5 text-green-500" />
                      : <XCircle className="h-5 w-5 text-red-500" />
                    }
                    <div>
                      <p className="text-sm font-medium">Migración de tabla <span className="font-mono text-primary">{item.table}</span></p>
                      <p className="text-xs text-muted-foreground">{item.time ? new Date(item.time).toLocaleString() : "Hace un momento"}</p>
                    </div>
                  </div>
                  <Badge variant="secondary">{item.records} registros</Badge>
                </div>
              ))}
              {recent_activity.length === 0 && <p className="text-sm text-muted-foreground text-center py-4">Sin actividad reciente.</p>}
            </div>
          </CardContent>
        </Card>
      </div>
    </Layout>
  );
};

// Componente auxiliar para filas de estado
const SystemStatusRow = ({ label, status }: { label: string, status: string }) => {
  const isOnline = status === 'online' || status === 'running' || status === 'connected';
  return (
    <div className="flex items-center justify-between">
      <span className="text-sm text-muted-foreground">{label}</span>
      <div className="flex items-center gap-2">
        <span className={`h-2 w-2 rounded-full ${isOnline ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
        <span className={`text-xs font-medium ${isOnline ? 'text-green-600' : 'text-red-600'}`}>
          {isOnline ? 'ONLINE' : 'OFFLINE'}
        </span>
      </div>
    </div>
  );
};

export default Index;