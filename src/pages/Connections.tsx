import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Button } from "@/components/ui/button";
import { Database, CheckCircle2, XCircle, RefreshCw, Wifi, Activity, Server } from "lucide-react";
import { useEffect, useState } from "react";
import { toast } from "sonner";

const Connections = () => {
  const [connections, setConnections] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchConnections = () => {
    setLoading(true);
    const toastId = toast.loading("Midiendo latencia de red...");
    
    fetch('http://localhost:5000/api/connections')
      .then(res => res.json())
      .then(data => {
        setConnections(data);
        toast.dismiss(toastId);
        toast.success("Diagnóstico completado");
      })
      .catch(() => {
        toast.dismiss(toastId);
        toast.error("Error al conectar con backend");
      })
      .finally(() => setLoading(false));
  };

  useEffect(() => { fetchConnections(); }, []);

  return (
    <Layout>
      <Header title="Estado de Conexiones" description="Monitoreo de latencia y disponibilidad de bases de datos" />
      
      <div className="p-6 space-y-6 max-w-5xl mx-auto">
        {/* Barra de Acciones */}
        <div className="flex justify-between items-center">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Activity className="h-4 w-4 text-green-500 animate-pulse" />
                <span>Monitoreo activo</span>
            </div>
            <Button onClick={fetchConnections} disabled={loading} variant="outline" className="gap-2">
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`}/> 
                {loading ? "Escaneando..." : "Ejecutar Diagnóstico"}
            </Button>
        </div>

        {/* Grid de Servidores */}
        <div className="grid gap-6 md:grid-cols-2">
          {connections.map((db) => (
            <ServerCard key={db.id} db={db} />
          ))}
          
          {connections.length === 0 && !loading && (
             <div className="col-span-2 text-center p-12 text-muted-foreground">No se encontraron conexiones configuradas.</div>
          )}
        </div>
      </div>
    </Layout>
  );
};

// Componente de Tarjeta de Servidor
const ServerCard = ({ db }: { db: any }) => {
    // Calcular salud basada en latencia
    const isConnected = db.status === 'connected';
    const latencyColor = db.latency < 100 ? "bg-green-500" : db.latency < 300 ? "bg-yellow-500" : "bg-red-500";
    const healthPercent = isConnected ? Math.max(0, 100 - (db.latency / 10)) : 0; // Fórmula visual simple

    return (
        <Card className="card-gradient border-border/50 overflow-hidden relative group hover:border-primary/30 transition-all">
            {/* Indicador Superior de Estado */}
            <div className={`h-1.5 w-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`} />
            
            <CardHeader className="pb-2">
                <div className="flex justify-between items-start">
                    <div className="flex gap-4">
                        <div className={`p-3 rounded-xl h-fit ${db.isProduction ? 'bg-orange-500/10 text-orange-500' : 'bg-blue-500/10 text-blue-500'}`}>
                            <Database className="h-6 w-6" />
                        </div>
                        <div>
                            <CardTitle className="text-lg font-bold flex items-center gap-2">
                                {db.name}
                                {db.isProduction && <Badge variant="destructive" className="text-[10px] h-5">PROD</Badge>}
                            </CardTitle>
                            <p className="text-xs text-muted-foreground font-mono mt-1 flex items-center gap-1">
                                <Server className="h-3 w-3" /> {db.host}
                            </p>
                            <p className="text-xs text-muted-foreground mt-0.5">
                                {db.version || "Versión desconocida"}
                            </p>
                        </div>
                    </div>
                    <Badge variant="outline" className={`${isConnected ? 'text-green-500 border-green-500/30 bg-green-500/5' : 'text-red-500 border-red-500/30 bg-red-500/5'}`}>
                        {isConnected ? <CheckCircle2 className="h-3 w-3 mr-1" /> : <XCircle className="h-3 w-3 mr-1" />}
                        {isConnected ? "ONLINE" : "OFFLINE"}
                    </Badge>
                </div>
            </CardHeader>

            <CardContent className="pt-4">
                <div className="grid grid-cols-2 gap-4 mb-4">
                    <div className="p-3 bg-muted/30 rounded-lg border border-border/50 text-center">
                        <p className="text-xs text-muted-foreground mb-1">Latencia</p>
                        <div className="flex items-center justify-center gap-1 font-mono font-bold text-lg">
                            <Wifi className={`h-4 w-4 ${isConnected ? 'text-green-500' : 'text-gray-400'}`} />
                            {isConnected ? `${db.latency} ms` : "-"}
                        </div>
                    </div>
                    <div className="p-3 bg-muted/30 rounded-lg border border-border/50 text-center">
                        <p className="text-xs text-muted-foreground mb-1">Último Chequeo</p>
                        <p className="font-mono font-bold text-sm mt-1">
                            {new Date(db.lastChecked).toLocaleTimeString()}
                        </p>
                    </div>
                </div>

                <div className="space-y-2">
                    <div className="flex justify-between text-xs">
                        <span className="text-muted-foreground">Calidad de Conexión</span>
                        <span className={isConnected ? "text-green-500" : "text-red-500"}>
                            {isConnected ? "Estable" : "Crítica"}
                        </span>
                    </div>
                    <Progress value={healthPercent} className="h-2" indicatorColor={latencyColor} />
                </div>
            </CardContent>
        </Card>
    );
};

export default Connections;