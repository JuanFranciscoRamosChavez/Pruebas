import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { PipelineCard } from "@/components/PipelineCard";
import { Button } from "@/components/ui/button";
import { Plus, Database, Lock, RefreshCw } from "lucide-react";
import { toast } from "sonner";
import { useState, useEffect } from "react"; // Agregamos useEffect
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

interface PipelinesProps {
  userRole?: string;
}

const Pipelines = ({ userRole }: PipelinesProps) => {
  const [pipelines, setPipelines] = useState<Pipeline[]>([]); // Inicializamos vacío
  const [loading, setLoading] = useState(true);
  const [isRunning, setIsRunning] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [newJob, setNewJob] = useState({ name: "", table: "" });
  const [availableTables, setAvailableTables] = useState<string[]>([]);

  // --- 1. CARGAR PIPELINES REALES AL INICIAR ---
  const fetchPipelines = async () => {
    setLoading(true);
    try {
      const res = await fetch('http://localhost:5000/api/pipelines');
      if (!res.ok) throw new Error("Error de conexión");
      const data = await res.json();
      setPipelines(data);
    } catch (e) {
      console.error(e);
      toast.error("No se pudieron cargar los pipelines");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPipelines();
  }, []); // Se ejecuta al montar el componente
  // ---------------------------------------------

  const loadTables = async () => {
    try {
      const res = await fetch('http://localhost:5000/api/source/tables');
      const tables = await res.json();
      if (Array.isArray(tables)) setAvailableTables(tables);
    } catch (e) { toast.error("Error buscando tablas"); }
  };

  const handleRunPipeline = async (id: string) => {
    setIsRunning(true);
    // Optimistic Update (Visual)
    setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'running' } : p));
    toast.info("Iniciando ejecución...");

    try {
      const response = await fetch('http://localhost:5000/api/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      const data = await response.json();

      if (response.ok) {
        toast.success("¡Éxito!", { description: data.message });
        // Recargar la lista real para ver fecha y estado actualizado de la BD
        await fetchPipelines(); 
      } else {
        throw new Error(data.message);
      }
    } catch (error: any) {
      setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'error' } : p));
      toast.error("Error", { description: error.message });
    } finally {
      setIsRunning(false);
    }
  };

  const handleCreateJob = async () => {
    if (!newJob.name || !newJob.table) return toast.error("Faltan datos");
    const toastId = toast.loading("Guardando...");

    try {
      const response = await fetch('http://localhost:5000/api/pipelines', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newJob)
      });

      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Pipeline Creado");
        setIsModalOpen(false);
        setNewJob({ name: "", table: "" });
        // Recargar lista del servidor para que aparezca el nuevo
        await fetchPipelines();
      } else {
        throw new Error("Error al guardar");
      }
    } catch (error: any) {
      toast.dismiss(toastId);
      toast.error("Error", { description: error.message });
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
            <span className={`text-xs px-2 py-1 rounded font-bold border ${userRole === 'admin' ? 'bg-purple-100 text-purple-700' : 'bg-blue-100 text-blue-700'}`}>
              {userRole === 'admin' ? 'DESARROLLADOR' : 'OPERADOR'}
            </span>
          </div>

          {userRole === 'admin' ? (
            <Dialog open={isModalOpen} onOpenChange={(open) => { setIsModalOpen(open); if (open) loadTables(); }}>
                <DialogTrigger asChild><Button disabled={isRunning}><Plus className="h-4 w-4 mr-2" /> Nuevo Job</Button></DialogTrigger>
                <DialogContent className="sm:max-w-[425px] bg-card border-border">
                <DialogHeader><DialogTitle>Nuevo Pipeline</DialogTitle><DialogDescription>El sistema detectará la estructura.</DialogDescription></DialogHeader>
                <div className="grid gap-4 py-4">
                    <div className="grid gap-2">
                    <Label>Tabla Producción</Label>
                    <Select onValueChange={(val) => setNewJob({ table: val, name: `Migración ${val.charAt(0).toUpperCase() + val.slice(1)}` })}>
                        <SelectTrigger><SelectValue placeholder="Seleccionar..." /></SelectTrigger>
                        <SelectContent>{availableTables.map(t => <SelectItem key={t} value={t}>{t}</SelectItem>)}</SelectContent>
                    </Select>
                    </div>
                    <div className="grid gap-2"><Label>Nombre</Label><Input value={newJob.name} onChange={(e) => setNewJob({...newJob, name: e.target.value})}/></div>
                </div>
                <DialogFooter><Button onClick={handleCreateJob}>Crear</Button></DialogFooter>
                </DialogContent>
            </Dialog>
          ) : (
            <Button disabled variant="secondary"><Lock className="h-3 w-3 mr-2" /> Solo Lectura</Button>
          )}
        </div>

        {loading ? (
            <div className="flex justify-center py-12"><RefreshCw className="h-8 w-8 animate-spin text-primary" /></div>
        ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {pipelines.map((pipeline) => (
                <div key={pipeline.id} className="animate-fade-in">
                <PipelineCard pipeline={pipeline} onRun={handleRunPipeline} onConfigure={() => toast.info("Ver config.yaml")} />
                </div>
            ))}
            </div>
        )}

        {!loading && pipelines.length === 0 && (
            <div className="text-center p-12 border-2 border-dashed rounded-xl"><p className="text-muted-foreground">No hay pipelines configurados en el servidor.</p></div>
        )}
      </div>
    </Layout>
  );
};

export default Pipelines;