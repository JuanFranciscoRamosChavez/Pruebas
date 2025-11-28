import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { ExecutionLogRow } from "@/components/ExecutionLogRow";
import { Button } from "@/components/ui/button";
import { Filter, Download, RefreshCw } from "lucide-react";
import { mockExecutionLogs } from "@/data/mockData";
import { StatusBadge } from "@/components/StatusBadge";

const History = () => {
  const statusCounts = {
    success: mockExecutionLogs.filter(l => l.status === 'success').length,
    error: mockExecutionLogs.filter(l => l.status === 'error').length,
    warning: mockExecutionLogs.filter(l => l.status === 'warning').length,
    running: mockExecutionLogs.filter(l => l.status === 'running').length,
  };

  return (
    <Layout>
      <Header 
        title="Historial de Ejecuciones" 
        description="Registro completo de todas las ejecuciones de pipelines con detalles de auditoría"
      />
      
      <div className="p-6 space-y-6">
        {/* Summary Stats */}
        <div className="flex items-center gap-4 p-4 rounded-lg bg-card border border-border/50">
          <span className="text-sm text-muted-foreground">Resumen:</span>
          <div className="flex items-center gap-3">
            <StatusBadge status="success" size="sm" />
            <span className="text-sm text-muted-foreground">{statusCounts.success}</span>
          </div>
          <div className="flex items-center gap-3">
            <StatusBadge status="warning" size="sm" />
            <span className="text-sm text-muted-foreground">{statusCounts.warning}</span>
          </div>
          <div className="flex items-center gap-3">
            <StatusBadge status="error" size="sm" />
            <span className="text-sm text-muted-foreground">{statusCounts.error}</span>
          </div>
          <div className="flex items-center gap-3">
            <StatusBadge status="running" size="sm" />
            <span className="text-sm text-muted-foreground">{statusCounts.running}</span>
          </div>
        </div>

        {/* Actions Bar */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm">
              <Filter className="h-4 w-4" />
              Filtrar
            </Button>
            <Button variant="outline" size="sm">
              <RefreshCw className="h-4 w-4" />
              Actualizar
            </Button>
          </div>
          <Button variant="outline" size="sm">
            <Download className="h-4 w-4" />
            Exportar Logs
          </Button>
        </div>

        {/* Execution Logs */}
        <div className="space-y-3">
          {mockExecutionLogs.map((log, index) => (
            <div 
              key={log.id} 
              className="animate-slide-up"
              style={{ animationDelay: `${index * 50}ms` }}
            >
              <ExecutionLogRow log={log} />
            </div>
          ))}
        </div>
      </div>
    </Layout>
  );
};

export default History;
