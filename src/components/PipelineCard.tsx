import { Pipeline } from "@/types/pipeline";
import { StatusBadge } from "./StatusBadge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Play, Settings, Database, Shield, Clock, ArrowRight } from "lucide-react";
import { formatDistanceToNow, differenceInMinutes } from "date-fns"; // Importamos differenceInMinutes
import { es } from "date-fns/locale";

interface PipelineCardProps {
  pipeline: Pipeline;
  onRun?: (id: string) => void;
  onConfigure?: (id: string) => void;
}

export function PipelineCard({ pipeline, onRun, onConfigure }: PipelineCardProps) {
  
  // Lógica para determinar el estado visual
  let displayStatus = pipeline.status;

  // Si hay una fecha de última ejecución y el estado NO es 'running' (ejecutando)
  if (pipeline.lastRun && pipeline.status !== 'running') {
    const lastRunDate = new Date(pipeline.lastRun);
    const now = new Date();
    
    // Calculamos la diferencia en minutos
    const minutesSinceLastRun = differenceInMinutes(now, lastRunDate);

    // Si pasaron más de 30 minutos, forzamos el estado a 'idle' (Inactivo)
    if (minutesSinceLastRun > 30) {
      displayStatus = 'idle';
    }
  }

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'Nunca';
    return formatDistanceToNow(new Date(dateString), { addSuffix: true, locale: es });
  };

  return (
    <Card className="card-gradient border-border/50 hover:border-primary/30 transition-all duration-300 group">
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-4">
          <div className="space-y-1">
            <CardTitle className="text-lg font-semibold text-foreground group-hover:text-primary transition-colors">
              {pipeline.name}
            </CardTitle>
            <p className="text-sm text-muted-foreground line-clamp-2">
              {pipeline.description}
            </p>
          </div>
          {/* Usamos displayStatus en lugar de pipeline.status */}
          <StatusBadge status={displayStatus} />
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Database Flow */}
        <div className="flex items-center gap-2 text-sm">
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <Database className="h-4 w-4 text-destructive/70" />
            <span className="font-mono text-xs">{pipeline.sourceDb}</span>
          </div>
          <ArrowRight className="h-4 w-4 text-primary/50" />
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <Database className="h-4 w-4 text-success/70" />
            <span className="font-mono text-xs">{pipeline.targetDb}</span>
          </div>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-3 gap-4 py-3 border-y border-border/50">
          <div className="text-center">
            <p className="text-2xl font-bold text-foreground">{pipeline.tablesCount}</p>
            <p className="text-xs text-muted-foreground">Tablas</p>
          </div>
          <div className="text-center">
            <p className="text-2xl font-bold text-foreground">
              {pipeline.recordsProcessed?.toLocaleString() || '-'}
            </p>
            <p className="text-xs text-muted-foreground">Registros</p>
          </div>
          <div className="text-center">
            <p className="text-2xl font-bold text-foreground">{pipeline.maskingRulesCount}</p>
            <p className="text-xs text-muted-foreground flex items-center justify-center gap-1">
              <Shield className="h-3 w-3" /> Reglas
            </p>
          </div>
        </div>

        {/* Timestamps */}
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <div className="flex items-center gap-1">
            <Clock className="h-3 w-3" />
            <span>Última: {formatDate(pipeline.lastRun)}</span>
          </div>
          {pipeline.nextRun && (
            <div className="flex items-center gap-1">
              <span>Próxima: {formatDate(pipeline.nextRun)}</span>
            </div>
          )}
        </div>

        {/* Actions */}
        <div className="flex gap-2 pt-2">
          <Button
            variant={pipeline.status === 'running' ? 'secondary' : 'default'}
            size="sm"
            className="flex-1"
            onClick={() => onRun?.(pipeline.id)}
            disabled={pipeline.status === 'running'}
          >
            <Play className="h-4 w-4" />
            {pipeline.status === 'running' ? 'Ejecutando...' : 'Ejecutar'}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => onConfigure?.(pipeline.id)}
          >
            <Settings className="h-4 w-4" />
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}