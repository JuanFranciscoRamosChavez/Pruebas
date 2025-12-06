import { ExecutionLog } from "@/types/pipeline";
import { Badge } from "@/components/ui/badge";
import { CheckCircle2, XCircle, Clock, Database, ArrowRight } from "lucide-react";
import { format } from "date-fns";
import { es } from "date-fns/locale";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

interface ExecutionLogRowProps {
  log: ExecutionLog;
}

export function ExecutionLogRow({ log }: ExecutionLogRowProps) {
  const isSuccess = log.status === 'success';

  // --- LÓGICA DE FORMATEO DE DURACIÓN ---
  const formatDuration = (seconds: number) => {
    if (!seconds && seconds !== 0) return "-";
    
    // Menos de 1 segundo: mostrar ms
    if (seconds < 1) {
      return `${Math.round(seconds * 1000)}ms`;
    }
    // Menos de 1 minuto: mostrar segundos con 2 decimales
    if (seconds < 60) {
      return `${seconds.toFixed(2)}s`;
    }
    // Más de 1 minuto: mostrar minutos y segundos
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    return `${m}m ${s}s`;
  };

  return (
    <Accordion type="single" collapsible className="w-full">
      <AccordionItem value={log.id} className="border rounded-lg bg-card px-4">
        <AccordionTrigger className="hover:no-underline py-3">
          <div className="flex items-center justify-between w-full pr-4">
            
            {/* Columna 1: Estado y Nombre */}
            <div className="flex items-center gap-4 min-w-[250px]">
              {isSuccess ? (
                <CheckCircle2 className="h-5 w-5 text-green-500" />
              ) : (
                <XCircle className="h-5 w-5 text-red-500" />
              )}
              <div className="text-left">
                <p className="font-medium text-sm text-foreground">{log.pipelineName}</p>
                <p className="text-xs text-muted-foreground">
                  {format(new Date(log.startTime), "PP p", { locale: es })}
                </p>
              </div>
            </div>

            {/* Columna 2: Métricas */}
            <div className="flex items-center gap-8 hidden md:flex">
              <div className="text-center">
                <p className="text-xs text-muted-foreground mb-0.5">Registros</p>
                <span className="text-sm font-bold font-mono">
                  {log.recordsProcessed?.toLocaleString() ?? 0}
                </span>
              </div>
              
              {/* DURACIÓN FORMATEADA */}
              <div className="text-center">
                <p className="text-xs text-muted-foreground mb-0.5">Duración</p>
                <div className="flex items-center gap-1 justify-center">
                  <Clock className="h-3 w-3 text-muted-foreground/70" />
                  <span className="text-sm font-bold font-mono">
                    {formatDuration(log.duration)}
                  </span>
                </div>
              </div>
            </div>

            {/* Columna 3: Badge Estado */}
            <Badge variant={isSuccess ? "outline" : "destructive"} className="capitalize">
              {isSuccess ? "Exitoso" : "Fallido"}
            </Badge>
          </div>
        </AccordionTrigger>
        
        <AccordionContent className="pt-2 pb-4 border-t border-border/50 mt-2">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            
            {/* Detalles de Flujo */}
            <div className="space-y-3">
              <h4 className="text-xs font-semibold uppercase text-muted-foreground tracking-wider">Flujo de Datos</h4>
              <div className="flex items-center gap-2 text-sm p-3 bg-muted/30 rounded-md border border-border/50">
                <Database className="h-4 w-4 text-blue-500" />
                <span>Producción</span>
                <ArrowRight className="h-4 w-4 text-muted-foreground mx-2" />
                <Database className="h-4 w-4 text-green-500" />
                <span>QA (Enmascarado)</span>
              </div>
              <div className="grid grid-cols-3 gap-2 text-center text-xs">
                <div className="p-2 bg-muted/20 rounded">
                  <span className="block font-bold text-lg">{log.recordsExtracted ?? '-'}</span>
                  <span className="text-muted-foreground">Extraídos</span>
                </div>
                <div className="p-2 bg-muted/20 rounded">
                  <span className="block font-bold text-lg">{log.recordsMasked ?? '-'}</span>
                  <span className="text-muted-foreground">Enmascarados</span>
                </div>
                <div className="p-2 bg-muted/20 rounded">
                  <span className="block font-bold text-lg">{log.recordsLoaded ?? '-'}</span>
                  <span className="text-muted-foreground">Cargados</span>
                </div>
              </div>
            </div>

            {/* Errores o Mensajes */}
            <div className="space-y-3">
              <h4 className="text-xs font-semibold uppercase text-muted-foreground tracking-wider">
                {isSuccess ? "Resumen" : "Detalle del Error"}
              </h4>
              <div className={`p-3 rounded-md text-sm border font-mono h-full max-h-[120px] overflow-y-auto ${
                isSuccess 
                  ? "bg-green-500/5 border-green-500/20 text-green-700" 
                  : "bg-red-500/5 border-red-500/20 text-red-700"
              }`}>
                {log.errors && log.errors.length > 0 
                  ? log.errors[0].message 
                  : "Operación finalizada correctamente sin incidencias reportadas."}
              </div>
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}