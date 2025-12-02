import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { PipelineCard } from "@/components/PipelineCard";
import { Button } from "@/components/ui/button";
import { Plus, Database, Lock } from "lucide-react";
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

// Definimos que este componente recibe el rol del usuario
interface PipelinesProps {
  userRole?: string;
}

const Pipelines = ({ userRole }: PipelinesProps) => {
  const [pipelines, setPipelines] = useState<Pipeline[]>(mockPipelines);
  const [isRunning, setIsRunning] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [newJob, setNewJob] = useState({ name: "", table: "" });

  const handleRunPipeline = async (id: string) => {
    const pipeline = pipelines.find(p => p.id === id);

    // Lógica de ejecución (Permitida para ambos roles)
    if (id.startsWith('new-') || id.startsWith('real-')) {
        // ... (lógica simulada igual que antes) ...
        toast.info("Simulación iniciada");
        setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'running' } : p));
        setTimeout(() => {
            setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'success', lastRun: new Date().toISOString() } : p));
            toast.success("Ejecución simulada exitosa");
        }, 2000);
        return;
    }

    setIsRunning(true);
    setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'running' } : p));
    const toastId = toast.loading(`Ejecutando ${pipeline?.name}...`);

    try {
      const response = await fetch('http://localhost:5000/api/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });

      let data = { message: "Proceso finalizado." };
      try {
        const json = await response.json();
        if (json) data = json;
      } catch (e) { console.warn("No JSON", e); }

      if (response.ok) {
        setPipelines(prev => prev.map(p => 
          p.id === id ? { ...p, status: 'success', lastRun: new Date().toISOString(), recordsProcessed: 150 } : p
        ));
        toast.dismiss(toastId);
        toast.success("¡Éxito!", { description: data.message });
      } else {
        throw new Error(data.message || `Error ${response.status}`);
      }
    } catch (error: any) {
      console.error("Error:", error);
      setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'error' } : p));
      toast.dismiss(toastId);
      toast.error("Error", { description: error.message || "Fallo de conexión" });
    } finally {
      setIsRunning(false);
    }
  };

  const handleCreateJob = async () => {
    // Seguridad Frontend: Bloquear si no es admin
    if (userRole !== 'admin') {
      toast.error("Acceso Denegado", { description: "Solo los desarrolladores pueden crear pipelines." });
      return;
    }

    if (!newJob.name || !newJob.table) {
      toast.error("Faltan datos");
      return;
    }

    const toastId = toast.loading("Creando configuración...");

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
          description: `Tabla: ${newJob.table}`,
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
        toast.success("Pipeline Creado");
      } else {
        throw new Error("Error al guardar");
      }
    } catch (error: any) {
        // Fallback Demo
        const simPipeline: Pipeline = {
            id: `new-${Date.now()}`,
            name: newJob.name,
            description: `(Simulado) ${newJob.table}`,
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
        toast.success("Creado (Modo Demo)");
    }
  };

  return (
    <Layout>
      <Header title="Gestión de Pipelines" description="Control de migración de datos sensibles" />
      
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <span className="text-sm text-muted-foreground bg-muted px-3 py-1 rounded-full border border-border/50 flex items-center gap-2">
              <Database className="h-3 w-3" /> Ambiente: <strong>Supabase Cloud</strong>
            </span>
            
            {/* Indicador Visual del Rol */}
            <span className={`text-xs px-2 py-1 rounded font-bold border ${
              userRole === 'admin' 
                ? 'bg-purple-100 text-purple-700 border-purple-200' 
                : 'bg-blue-100 text-blue-700 border-blue-200'
            }`}>
              {userRole === 'admin' ? 'DESARROLLADOR' : 'OPERADOR'}
            </span>
          </div>

          {/* --- BOTÓN CONDICIONAL (PUNTO 8) --- */}
          {userRole === 'admin' ? (
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
                    <DialogDescription>Define los parámetros ETL.</DialogDescription>
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
                        <SelectTrigger><SelectValue placeholder="Seleccionar..." /></SelectTrigger>
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
          ) : (
            // Botón Bloqueado para Operadores
            <Button disabled variant="secondary" className="opacity-70 cursor-not-allowed">
                <Lock className="h-3 w-3 mr-2" />
                Solo Lectura
            </Button>
          )}
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
      </div>
    </Layout>
  );
};

export default Pipelines;