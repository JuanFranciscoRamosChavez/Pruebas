import { ExecutionLog } from "@/types/pipeline";
import { StatusBadge } from "./StatusBadge";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronUp, Clock, Database, Shield, AlertCircle } from "lucide-react";
import { format } from "date-fns";
import { es } from "date-fns/locale";
import { useState } from "react";
import { cn } from "@/lib/utils";

interface ExecutionLogRowProps {
  log: ExecutionLog;
}

export function ExecutionLogRow({ log }: ExecutionLogRowProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  const formatDuration = (seconds?: number) => {
    if (!seconds) return '-';
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
  };

  const formatDateTime = (dateString: string) => {
    return format(new Date(dateString), "dd MMM yyyy, HH:mm:ss", { locale: es });
  };

  return (
    <div className="border border-border/50 rounded-lg overflow-hidden bg-card/50">
      {/* Main Row */}
      <div 
        className="flex items-center gap-4 p-4 cursor-pointer hover:bg-muted/30 transition-colors"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <Button variant="ghost" size="icon" className="h-8 w-8 shrink-0">
          {isExpanded ? (
            <ChevronUp className="h-4 w-4" />
          ) : (
            <ChevronDown className="h-4 w-4" />
          )}
        </Button>

        <div className="flex-1 min-w-0">
          <p className="font-medium text-foreground truncate">{log.pipelineName}</p>
          <p className="text-xs text-muted-foreground">
            {formatDateTime(log.startTime)}
          </p>
        </div>

        <StatusBadge status={log.status} size="sm" />

        <div className="hidden sm:flex items-center gap-6 text-sm">
          <div className="text-center">
            <p className="font-medium text-foreground">{log.recordsExtracted.toLocaleString()}</p>
            <p className="text-xs text-muted-foreground flex items-center gap-1">
              <Database className="h-3 w-3" /> Extraídos
            </p>
          </div>
          <div className="text-center">
            <p className="font-medium text-foreground">{log.recordsMasked.toLocaleString()}</p>
            <p className="text-xs text-muted-foreground flex items-center gap-1">
              <Shield className="h-3 w-3" /> Enmascarados
            </p>
          </div>
          <div className="text-center">
            <p className="font-medium text-foreground">{log.recordsLoaded.toLocaleString()}</p>
            <p className="text-xs text-muted-foreground flex items-center gap-1">
              <Database className="h-3 w-3" /> Cargados
            </p>
          </div>
          <div className="text-center min-w-[80px]">
            <p className="font-medium text-foreground flex items-center gap-1">
              <Clock className="h-3 w-3" /> {formatDuration(log.duration)}
            </p>
            <p className="text-xs text-muted-foreground">Duración</p>
          </div>
        </div>
      </div>

      {/* Expanded Details */}
      {isExpanded && (
        <div className="border-t border-border/50 p-4 bg-muted/20 animate-fade-in">
          {/* Mobile Stats */}
          <div className="sm:hidden grid grid-cols-2 gap-4 mb-4 pb-4 border-b border-border/50">
            <div>
              <p className="text-xs text-muted-foreground">Extraídos</p>
              <p className="font-medium">{log.recordsExtracted.toLocaleString()}</p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">Enmascarados</p>
              <p className="font-medium">{log.recordsMasked.toLocaleString()}</p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">Cargados</p>
              <p className="font-medium">{log.recordsLoaded.toLocaleString()}</p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">Duración</p>
              <p className="font-medium">{formatDuration(log.duration)}</p>
            </div>
          </div>

          {log.errors.length > 0 ? (
            <div className="space-y-2">
              <p className="text-sm font-medium text-foreground flex items-center gap-2">
                <AlertCircle className="h-4 w-4 text-destructive" />
                Errores ({log.errors.length})
              </p>
              <div className="space-y-2">
                {log.errors.map((error) => (
                  <div 
                    key={error.id}
                    className={cn(
                      "p-3 rounded-md text-sm",
                      error.severity === 'error' 
                        ? "bg-destructive/10 border border-destructive/30"
                        : "bg-warning/10 border border-warning/30"
                    )}
                  >
                    <div className="flex items-start justify-between gap-2 mb-1">
                      <p className={cn(
                        "font-medium",
                        error.severity === 'error' ? "text-destructive" : "text-warning"
                      )}>
                        {error.message}
                      </p>
                      <span className="text-xs text-muted-foreground whitespace-nowrap">
                        {format(new Date(error.timestamp), "HH:mm:ss")}
                      </span>
                    </div>
                    {error.table && (
                      <p className="text-xs text-muted-foreground mb-1">
                        Tabla: <code className="font-mono">{error.table}</code>
                      </p>
                    )}
                    {error.details && (
                      <p className="text-xs text-muted-foreground font-mono mt-2 p-2 bg-background/50 rounded">
                        {error.details}
                      </p>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              Ejecución completada sin errores.
            </p>
          )}
        </div>
      )}
    </div>
  );
}
