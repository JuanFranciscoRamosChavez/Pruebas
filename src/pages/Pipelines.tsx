import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { PipelineCard } from "@/components/PipelineCard";
import { Button } from "@/components/ui/button";
import { Plus, RefreshCw, Database } from "lucide-react";
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
  
  // Estado para el formulario del nuevo Job
  const [newJob, setNewJob] = useState({ name: "", table: "" });

  const handleRunPipeline = async (id: string) => {
    // Evitar correr si es un pipeline simulado (recién creado)
    if (id.startsWith('new-')) {
      toast.info("Simulación de ejecución iniciada", {
        description: "Este es un pipeline demostrativo creado desde la UI.",
      });
      setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'running' } : p));
      setTimeout(() => {
        setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'success', lastRun: new Date().toISOString() } : p));
        toast.success("Ejecución simulada exitosa");
      }, 3000);
      return;
    }

    // Lógica REAL para el pipeline principal
    setIsRunning(true);
    setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'running' } : p));
    toast.info("Iniciando Migración Real...", { description: "Backend Python procesando Supabase..." });

    try {
      const response = await fetch('http://localhost:5000/api/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      const data = await response.json();

      if (response.ok) {
        setPipelines(prev => prev.map(p => 
          p.id === id ? { ...p, status: 'success', lastRun: new Date().toISOString(), recordsProcessed: 150 } : p
        ));
        toast.success("¡Pipeline Finalizado!", { description: data.message, duration: 5000 });
      } else {
        throw new Error(data.message);
      }
    } catch (error) {
      console.error("Error:", error);
      setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'error' } : p));
      toast.error("Fallo en la ejecución", { description: "Revisa la conexión con el backend." });
    } finally {
      setIsRunning(false);
    }
  };

const handleCreateJob = async () => {
    if (!newJob.name || !newJob.table) {
      toast.error("Completa todos los campos");
      return;
    }

    // Notificar al usuario que estamos procesando
    const toastId = toast.loading("Configurando nuevo pipeline en el servidor...");

    try {
      // 1. Petición REAL al Backend para guardar en YAML
      const response = await fetch('http://localhost:5000/api/pipelines', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newJob)
      });

      const data = await response.json();

      if (response.ok) {
        // 2. Éxito: Agregamos visualmente la tarjeta
        const newPipeline: Pipeline = {
          id: `real-${Date.now()}`,
          name: newJob.name,
          description: `Extracción configurada para la tabla: ${newJob.table}`,
          sourceDb: 'supabase-prod',
          targetDb: 'supabase-qa',
          status: 'idle',
          tablesCount: 1,
          maskingRulesCount: 2, // Las reglas default que pusimos en Python
          recordsProcessed: 0
        };

        setPipelines([...pipelines, newPipeline]);
        setNewJob({ name: "", table: "" });
        setIsModalOpen(false);
        
        // Actualizar el toast a éxito
        toast.dismiss(toastId);
        toast.success("¡Pipeline Creado!", {
          description: data.message, // "Pipeline guardado en disco"
        });
      } else {
        throw new Error(data.message);
      }
    } catch (error) {
      console.error("Error creando job:", error);
      toast.dismiss(toastId);
      toast.error("Error al crear", {
        description: error instanceof Error ? error.message : "Fallo en el backend"
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

          {/* --- AQUÍ ESTÁ EL BOTÓN CON EL MODAL --- */}
          <Dialog open={isModalOpen} onOpenChange={setIsModalOpen}>
            <DialogTrigger asChild>
              <Button>
                <Plus className="h-4 w-4 mr-2" />
                Nuevo Job
              </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-[425px] bg-card border-border">
              <DialogHeader>
                <DialogTitle>Configurar Nuevo Pipeline</DialogTitle>
                <DialogDescription>
                  Define los parámetros para una nueva tarea de migración ETL.
                </DialogDescription>
              </DialogHeader>
              <div className="grid gap-4 py-4">
                <div className="grid gap-2">
                  <Label htmlFor="name">Nombre del Pipeline</Label>
                  <Input 
                    id="name" 
                    placeholder="Ej: Migración Empleados" 
                    value={newJob.name}
                    onChange={(e) => setNewJob({...newJob, name: e.target.value})}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor="table">Tabla Origen</Label>
                  <Select onValueChange={(val) => setNewJob({...newJob, table: val})}>
                    <SelectTrigger>
                      <SelectValue placeholder="Seleccionar tabla..." />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="empleados">empleados (RH)</SelectItem>
                      <SelectItem value="inventario">inventario</SelectItem>
                      <SelectItem value="pagos">pagos_historial</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <DialogFooter>
                <Button onClick={handleCreateJob}>Crear Configuración</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
          {/* --------------------------------------- */}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {pipelines.map((pipeline) => (
            <div key={pipeline.id} className="animate-fade-in">
              <PipelineCard 
                pipeline={pipeline} 
                onRun={handleRunPipeline}
                onConfigure={() => toast.info("Configuración en config.yaml")}
              />
            </div>
          ))}
        </div>
      </div>
    </Layout>
  );
};

export default Pipelines;