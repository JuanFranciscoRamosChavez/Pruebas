import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { PipelineCard } from "@/components/PipelineCard";
import { Button } from "@/components/ui/button";
import { Plus, Database } from "lucide-react";
import { mockPipelines } from "@/data/mockData";
import { toast } from "sonner";
import { useState } from "react";
import { Pipeline } from "@/types/pipeline";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

const Pipelines = () => {
  const [pipelines, setPipelines] = useState<Pipeline[]>(mockPipelines);
  const [isRunning, setIsRunning] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [newJob, setNewJob] = useState({ name: "", table: "" });

  // --- LÓGICA DE EJECUCIÓN (RUN) ---
  const handleRunPipeline = async (id: string) => {
    // 1. Caso Simulado (Pipelines recién creados sin backend real)
    if (id.startsWith('new-') || id.startsWith('real-')) {
      toast.info("Ejecutando simulación...", { description: "Pipeline demostrativo." });
      setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'running' } : p));
      
      setTimeout(() => {
        setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'success', lastRun: new Date().toISOString() } : p));
        toast.success("Ejecución exitosa", { description: "Datos procesados correctamente." });
      }, 2000);
      return;
    }

    // 2. Caso Real (Backend Python)
    setIsRunning(true);
    setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'running' } : p));
    const toastId = toast.loading("Conectando con el motor ETL...");

    try {
      const response = await fetch('http://localhost:5000/api/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });

      // Intentar parsear JSON de forma segura
      let data = { message: "Proceso finalizado." };
      try {
        const jsonResponse = await response.json();
        if (jsonResponse) data = jsonResponse;
      } catch (e) {
        console.warn("Respuesta no-JSON:", e);
      }

      if (response.ok) {
        // --- ÉXITO VERDE ---
        setPipelines(prev => prev.map(p => 
          p.id === id ? { 
            ...p, 
            status: 'success', 
            lastRun: new Date().toISOString(),
            recordsProcessed: 150 
          } : p
        ));
        
        toast.dismiss(toastId);
        toast.success("¡Pipeline Finalizado!", {
          description: data.message || "Datos migrados correctamente.",
          duration: 5000,
        });
      } else {
        // --- ERROR ROJO (Del Servidor) ---
        throw new Error(data.message || `Error ${response.status}`);
      }

    } catch (error: any) {
      // --- ERROR ROJO (De Red/Código) ---
      console.error("Error pipeline:", error);
      setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'error' } : p));
      
      toast.dismiss(toastId);
      toast.error("Error en la ejecución", {
        description: error.message || "No se pudo conectar al servidor."
      });
    } finally {
      setIsRunning(false);
    }
  };

  // --- LÓGICA DE CREACIÓN (CREATE) ---
  const handleCreateJob = async () => {
    if (!newJob.name || !newJob.table) {
      toast.error("Faltan datos");
      return;
    }

    const toastId = toast.loading("Guardando configuración...");

    try {
      const response = await fetch('http://localhost:5000/api/pipelines', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newJob)
      });

      if (response.ok) {
        const newPipeline: Pipeline = {
          id: `real-${Date.now()}`,
          name: newJob.name,
          description: `Migración de tabla: ${newJob.table}`,
          sourceDb: 'supabase-prod',
          targetDb: 'supabase-qa',
          status: 'idle',
          tablesCount: 1,
          maskingRulesCount: 2,
          recordsProcessed: 0
        };

        setPipelines([...pipelines, newPipeline]);
        setNewJob({ name: "", table: "" });
        setIsModalOpen(false);
        
        toast.dismiss(toastId);
        toast.success("Pipeline Creado", { description: "Configuración guardada en YAML." });
      } else {
        throw new Error("Error al guardar");
      }
    } catch (error) {
      // Fallback: Si no hay backend, lo creamos simulado para que no se rompa la demo
      const simPipeline: Pipeline = {
        id: `new-${Date.now()}`,
        name: newJob.name,
        description: `(Simulado) Tabla: ${newJob.table}`,
        sourceDb: 'supabase-prod',
        targetDb: 'supabase-qa',
        status: 'idle',
        tablesCount: 1,
        maskingRulesCount: 0,
        recordsProcessed: 0
      };
      setPipelines([...pipelines, simPipeline]);
      setNewJob({ name: "", table: "" });
      setIsModalOpen(false);
      
      toast.dismiss(toastId);
      toast.success("Pipeline Creado (Modo Demo)", { 
        description: "Backend no disponible, usando modo simulación." 
      });
    }
  };

  return (
    <Layout>
      <Header title="Gestión de Pipelines" description="Control de migración de datos sensibles" />
      
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground bg-muted px-3 py-1 rounded-full border border-border/50 flex items-center gap-2">
              <Database className="h-3 w-3" /> Ambiente: <strong>Supabase Cloud</strong>
            </span>
          </div>

          <Dialog open={isModalOpen} onOpenChange={setIsModalOpen}>
            <DialogTrigger asChild>
              <Button disabled={isRunning}>
                <Plus className="h-4 w-4 mr-2" />
                Nuevo Job
              </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-[425px] bg-card border-border">
              <DialogHeader>
                <DialogTitle>Configurar Nuevo Pipeline</DialogTitle>
                <DialogDescription>Parámetros para nueva tarea ETL.</DialogDescription>
              </DialogHeader>
              <div className="grid gap-4 py-4">
                <div className="grid gap-2">
                  <Label htmlFor="name">Nombre</Label>
                  <Input 
                    id="name" 
                    placeholder="Ej: Migración Nómina" 
                    value={newJob.name}
                    onChange={(e) => setNewJob({...newJob, name: e.target.value})}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="table">Tabla Origen</Label>
                  <Select onValueChange={(val) => setNewJob({...newJob, table: val})}>
                    <SelectTrigger>
                      <SelectValue placeholder="Seleccionar..." />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="empleados">empleados (RH)</SelectItem>
                      <SelectItem value="inventario">inventario</SelectItem>
                      <SelectItem value="pagos">pagos</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <DialogFooter>
                <Button onClick={handleCreateJob}>Crear</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {pipelines.map((pipeline) => (
            <div key={pipeline.id} className="animate-fade-in">
              <PipelineCard 
                pipeline={pipeline} 
                onRun={handleRunPipeline}
                onConfigure={() => toast.info("Ver config.yaml")}
              />
            </div>
          ))}
        </div>
        
        {pipelines.length === 0 && (
            <div className="text-center p-12 border-2 border-dashed rounded-xl">
                <p className="text-muted-foreground">Sin pipelines activos.</p>
            </div>
        )}
      </div>
    </Layout>
  );
};

export default Pipelines;