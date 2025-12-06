import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { ExecutionLogRow } from "@/components/ExecutionLogRow";
import { Button } from "@/components/ui/button";
import { Filter, RefreshCw, AlertCircle, CheckCircle2, XCircle, X } from "lucide-react";
import { useEffect, useState } from "react";
import { toast } from "sonner";
import { ExecutionLog } from "@/types/pipeline";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

const History = () => {
  const [logs, setLogs] = useState<ExecutionLog[]>([]);
  const [loading, setLoading] = useState(true);
  
  const [statusFilter, setStatusFilter] = useState<'all' | 'success' | 'error'>('all');

  const fetchHistory = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://localhost:5000/api/history');
      
      if (response.ok) {
        const data = await response.json();
        
        if (Array.isArray(data)) {
            const realLogs: ExecutionLog[] = data.map((item: any, index: number) => {
                const isSuccess = item.estado && item.estado.includes('SUCCESS');
                
                return {
                id: `real-${index}`,
                pipelineId: '1', 
                pipelineName: `Migración: ${item.tabla}`,
                status: isSuccess ? 'success' : 'error',
                startTime: item.fecha,
                duration: item.duration || 0,
                
                // --- AQUI AGREGAMOS EL CAMPO FALTANTE ---
                recordsProcessed: item.registros, 
                // ----------------------------------------
                
                recordsExtracted: item.registros,
                recordsMasked: item.registros,
                recordsLoaded: item.registros,
                errors: !isSuccess ? [{ 
                    id: `err-${index}`, 
                    message: item.mensaje || "Error desconocido", 
                    timestamp: item.fecha, 
                    severity: 'error' 
                }] : []
                };
            });
            setLogs(realLogs);
            toast.success("Historial actualizado");
        }
      }
    } catch (error) {
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchHistory();
  }, []);

  const filteredLogs = logs.filter(log => {
      if (statusFilter === 'all') return true;
      return log.status === statusFilter;
  });

  const statusCounts = {
    success: logs.filter(l => l.status === 'success').length,
    error: logs.filter(l => l.status === 'error').length,
    total: logs.length
  };

  return (
    <Layout>
      <Header 
        title="Historial de Ejecuciones" 
        description="Auditoría detallada de operaciones en base de datos (Supabase QA)"
      />
      
      <div className="p-6 space-y-6">
        {/* Resumen Cards */}
        <div className="flex flex-wrap items-center gap-4 p-4 rounded-lg bg-card border border-border/50 shadow-sm">
          <span className="text-sm font-medium text-muted-foreground mr-2">Resumen Global:</span>
          
          <div className="flex items-center gap-2 px-3 py-1.5 rounded-md bg-green-500/10 border border-green-500/20">
            <CheckCircle2 className="h-4 w-4 text-green-600" />
            <span className="text-sm font-bold text-green-700">{statusCounts.success}</span>
            <span className="text-xs text-green-600/80">Exitosos</span>
          </div>

          <div className="flex items-center gap-2 px-3 py-1.5 rounded-md bg-red-500/10 border border-red-500/20">
            <XCircle className="h-4 w-4 text-red-600" />
            <span className="text-sm font-bold text-red-700">{statusCounts.error}</span>
            <span className="text-xs text-red-600/80">Fallidos</span>
          </div>

          <div className="ml-auto text-xs text-muted-foreground bg-muted px-2 py-1 rounded">
            Total Registros: {statusCounts.total}
          </div>
        </div>

        {/* Barra de Acciones */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            
            <DropdownMenu>
                <DropdownMenuTrigger asChild>
                    <Button variant={statusFilter !== 'all' ? "secondary" : "outline"} size="sm" className="gap-2">
                        <Filter className="h-4 w-4" />
                        {statusFilter === 'all' ? 'Filtrar por Estado' : statusFilter === 'success' ? 'Solo Exitosos' : 'Solo Errores'}
                    </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="start">
                    <DropdownMenuLabel>Filtrar visualización</DropdownMenuLabel>
                    <DropdownMenuSeparator />
                    <DropdownMenuCheckboxItem checked={statusFilter === 'all'} onCheckedChange={() => setStatusFilter('all')}>
                        Todos
                    </DropdownMenuCheckboxItem>
                    <DropdownMenuCheckboxItem checked={statusFilter === 'success'} onCheckedChange={() => setStatusFilter('success')}>
                        Exitosos
                    </DropdownMenuCheckboxItem>
                    <DropdownMenuCheckboxItem checked={statusFilter === 'error'} onCheckedChange={() => setStatusFilter('error')}>
                        Con Errores
                    </DropdownMenuCheckboxItem>
                </DropdownMenuContent>
            </DropdownMenu>

            {statusFilter !== 'all' && (
                <Button variant="ghost" size="icon" onClick={() => setStatusFilter('all')} className="h-9 w-9">
                    <X className="h-4 w-4 text-muted-foreground" />
                </Button>
            )}

            <Button variant="ghost" size="sm" onClick={fetchHistory} disabled={loading}>
              <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            </Button>
          </div>
          
          <span className="text-xs text-muted-foreground">
             Mostrando {filteredLogs.length} eventos
          </span>
        </div>

        <div className="space-y-3">
          {filteredLogs.length === 0 && !loading ? (
            <div className="text-center p-12 border-2 border-dashed rounded-xl bg-muted/5">
              <AlertCircle className="mx-auto h-10 w-10 mb-3 text-muted-foreground/50" />
              <p className="text-muted-foreground font-medium">No se encontraron registros.</p>
              <p className="text-xs text-muted-foreground mt-1">
                {statusFilter !== 'all' ? "Prueba cambiando los filtros." : "Ejecuta un pipeline para generar historial."}
              </p>
            </div>
          ) : (
            filteredLogs.map((log) => (
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