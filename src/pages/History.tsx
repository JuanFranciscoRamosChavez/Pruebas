import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { ExecutionLogRow } from "@/components/ExecutionLogRow";
import { Button } from "@/components/ui/button";
import { Filter, RefreshCw, AlertCircle } from "lucide-react";
import { StatusBadge } from "@/components/StatusBadge";
import { useEffect, useState } from "react";
import { toast } from "sonner";
import { ExecutionLog } from "@/types/pipeline";

const History = () => {
  const [logs, setLogs] = useState<ExecutionLog[]>([]);
  const [loading, setLoading] = useState(true);

  // Función para obtener datos reales del Backend (Python)
  const fetchHistory = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:5000/api/history');
      if (!response.ok) throw new Error("Error al obtener historial");
      
      const data = await response.json();
      
      // Transformar datos del backend al formato del frontend
      const realLogs: ExecutionLog[] = data.map((item: any, index: number) => ({
        id: `real-${index}`,
        pipelineId: '1', // Hardcodeado por simplicidad
        pipelineName: `Migración: ${item.tabla}`,
        status: item.estado === 'SUCCESS' ? 'success' : 'error',
        startTime: item.fecha,
        duration: 0, // El backend simple no calculó duración, lo dejamos en 0
        recordsExtracted: item.registros,
        recordsMasked: item.registros,
        recordsLoaded: item.registros,
        errors: item.estado === 'ERROR' ? [{ 
          id: `err-${index}`, 
          message: item.mensaje, 
          timestamp: item.fecha, 
          severity: 'error' 
        }] : []
      }));

      setLogs(realLogs);
      toast.success("Historial actualizado desde BD");
    } catch (error) {
      console.error(error);
      toast.error("No se pudo conectar con el Backend de Historial");
    } finally {
      setLoading(false);
    }
  };

  // Cargar al iniciar la página
  useEffect(() => {
    fetchHistory();
  }, []);

  // Calcular estadísticas reales
  const statusCounts = {
    success: logs.filter(l => l.status === 'success').length,
    error: logs.filter(l => l.status === 'error').length,
    total: logs.length
  };

  return (
    <Layout>
      <Header 
        title="Historial Real de Ejecuciones" 
        description="Auditoría proveniente directamente de Supabase QA (Tabla: auditoria)"
      />
      
      <div className="p-6 space-y-6">
        {/* Resumen */}
        <div className="flex items-center gap-4 p-4 rounded-lg bg-card border border-border/50">
          <span className="text-sm text-muted-foreground">Resumen BD:</span>
          <div className="flex items-center gap-3">
            <StatusBadge status="success" size="sm" />
            <span className="text-sm text-muted-foreground">{statusCounts.success}</span>
          </div>
          <div className="flex items-center gap-3">
            <StatusBadge status="error" size="sm" />
            <span className="text-sm text-muted-foreground">{statusCounts.error}</span>
          </div>
          <div className="ml-auto text-xs text-muted-foreground">
            Total Registros de Auditoría: {statusCounts.total}
          </div>
        </div>

        {/* Barra de Acciones */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm">
              <Filter className="h-4 w-4" /> Filtrar
            </Button>
            <Button variant="outline" size="sm" onClick={fetchHistory} disabled={loading}>
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
              {loading ? 'Cargando...' : 'Actualizar Datos'}
            </Button>
          </div>
        </div>

        {/* Lista de Logs */}
        <div className="space-y-3">
          {logs.length === 0 && !loading ? (
            <div className="text-center p-8 text-muted-foreground border border-dashed rounded-lg">
              <AlertCircle className="mx-auto h-8 w-8 mb-2 opacity-50" />
              <p>No hay registros en la base de datos de QA aún.</p>
              <p className="text-xs mt-1">Ejecuta un pipeline para ver datos aquí.</p>
            </div>
          ) : (
            logs.map((log) => (
              <div key={log.id} className="animate-slide-up">
                <ExecutionLogRow log={log} />
              </div>
            ))
          )}
        </div>
      </div>
    </Layout>
  );
};

export default History;