import { Pipeline } from "@/types/pipeline";
import { StatusBadge } from "./StatusBadge";
import { Card, CardContent, CardHeader } from "@/components/ui/card"; // Quitamos CardTitle para usar div custom
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Play, Database, Shield, Clock, ArrowRight, Trash2 } from "lucide-react";
import { formatDistanceToNow, differenceInMinutes } from "date-fns";
import { es } from "date-fns/locale";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface PipelineCardProps {
  pipeline: Pipeline;
  onRun?: (id: string) => void;
  onDelete?: (id: string) => void;
  onToggleActive?: (id: string, active: boolean) => void;
  userRole?: string;
}

export function PipelineCard({ pipeline, onRun, onDelete, onToggleActive, userRole }: PipelineCardProps) {
  
  let displayStatus = pipeline.status;
  if (pipeline.lastRun && pipeline.status !== 'running') {
    const lastRunDate = new Date(pipeline.lastRun);
    const now = new Date();
    if (differenceInMinutes(now, lastRunDate) > 30) displayStatus = 'idle';
  }

  const isDisabled = pipeline.isActive === false;

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'Nunca';
    return formatDistanceToNow(new Date(dateString), { addSuffix: true, locale: es });
  };

  return (
    <Card className={`card-gradient border-border/50 transition-all duration-300 group ${isDisabled ? 'opacity-70 grayscale-[0.5]' : 'hover:border-primary/30'}`}>
      <CardHeader className="pb-3 pt-5 px-5">
        {/* FILA SUPERIOR: Título + Badge (Izq) | Controles (Der) */}
        <div className="flex justify-between items-start mb-2">
            {/* IZQUIERDA: Título y Estado */}
            <div className="flex flex-col gap-1.5 min-w-0 pr-2">
                <div className="flex items-center gap-2 flex-wrap">
                    <h3 className="font-bold text-lg leading-none text-foreground truncate" title={pipeline.name}>
                        {pipeline.name}
                    </h3>
                    {/* El Badge ahora vive junto al título para contexto inmediato */}
                    <StatusBadge status={displayStatus} />
                </div>
                <p className="text-sm text-muted-foreground line-clamp-2 h-10 leading-relaxed">
                    {pipeline.description}
                </p>
            </div>

            {/* DERECHA: Acciones Administrativas (Switch + Basura) */}
            {userRole === 'admin' && (
                <div className="flex items-center gap-1 bg-muted/30 p-1 rounded-lg border border-border/50 shrink-0">
                     {/* Switch */}
                     <TooltipProvider>
                        <Tooltip>
                            <TooltipTrigger asChild>
                                <div className="flex items-center px-1"> 
                                <Switch 
                                    checked={pipeline.isActive !== false} 
                                    onCheckedChange={(c) => onToggleActive?.(pipeline.id, c)}
                                    className="scale-75 data-[state=checked]:bg-green-600"
                                />
                                </div>
                            </TooltipTrigger>
                            <TooltipContent>
                                <p>{pipeline.isActive !== false ? 'Activo' : 'Pausado'}</p>
                            </TooltipContent>
                        </Tooltip>
                    </TooltipProvider>
                    
                    {/* Separador visual */}
                    <div className="w-px h-4 bg-border mx-1"></div>

                    {/* Delete Button */}
                    <Button 
                        variant="ghost" 
                        size="icon" 
                        className="h-7 w-7 text-muted-foreground hover:text-destructive hover:bg-destructive/10 rounded-md"
                        onClick={() => onDelete?.(pipeline.id)}
                        disabled={pipeline.status === 'running'}
                        title="Eliminar Pipeline"
                    >
                        <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                </div>
            )}
        </div>
      </CardHeader>
      
      <CardContent className="px-5 pb-5 space-y-5">
        {/* Database Flow */}
        <div className="flex items-center gap-2 text-xs font-medium uppercase tracking-wide text-muted-foreground bg-muted/20 p-2 rounded border border-border/30 justify-center">
           <span>{pipeline.sourceDb}</span>
           <ArrowRight className="h-3 w-3" />
           <span>{pipeline.targetDb}</span>
        </div>

        {/* Stats */}
        <div className={`grid grid-cols-3 gap-2 py-1 ${isDisabled ? 'opacity-50' : ''}`}>
          <div className="text-center">
            <p className="text-xl font-bold text-foreground">{pipeline.tablesCount}</p>
            <p className="text-[10px] uppercase text-muted-foreground font-semibold">Tablas</p>
          </div>
          <div className="text-center border-l border-r border-border/40">
            <p className="text-xl font-bold text-foreground">
              {pipeline.recordsProcessed?.toLocaleString() || '0'}
            </p>
            <p className="text-[10px] uppercase text-muted-foreground font-semibold">Registros</p>
          </div>
          <div className="text-center">
            <p className="text-xl font-bold text-foreground">{pipeline.maskingRulesCount}</p>
            <p className="text-[10px] uppercase text-muted-foreground font-semibold">Reglas</p>
          </div>
        </div>

        {/* Footer: Time & Action */}
        <div className="flex items-center justify-between gap-4 pt-2 border-t border-border/50">
           <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <Clock className="h-3.5 w-3.5" />
            <span>{formatDate(pipeline.lastRun)}</span>
          </div>

          <Button
            // Estilo sólido para la acción principal, más llamativo
            variant={pipeline.status === 'running' ? 'secondary' : 'default'}
            size="sm"
            className="px-6 h-8 text-xs font-semibold shadow-sm"
            onClick={() => onRun?.(pipeline.id)}
            disabled={pipeline.status === 'running' || !onRun || isDisabled}
          >
            <Play className="h-3 w-3 mr-2 fill-current" />
            {pipeline.status === 'running' ? 'Procesando...' : isDisabled ? 'Pausado' : 'Ejecutar'}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}