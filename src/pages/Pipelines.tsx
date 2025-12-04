import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { PipelineCard } from "@/components/PipelineCard";
import { Button } from "@/components/ui/button";
import { Plus, Database, Lock, RefreshCw } from "lucide-react";
import { toast } from "sonner";
import { useState, useEffect } from "react";
import { Pipeline } from "@/types/pipeline";
// Eliminamos el import de mockData para no usarlo por error
// import { mockPipelines } from "@/data/mockData"; 

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
  // CAMBIO 1: Iniciar vacío, no con datos falsos
  const [pipelines, setPipelines] = useState<Pipeline[]>([]); 
  const [loading, setLoading] = useState(true); // Inicia cargando
  const [isRunning, setIsRunning] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [newJob, setNewJob] = useState({ name: "", table: "" });
  
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [isLoadingTables, setIsLoadingTables] = useState(false);

  // --- 1. CARGAR PIPELINES DEL BACKEND ---
  const fetchPipelines = async () => {
    setLoading(true);
    try {
      const res = await fetch('http://localhost:5000/api/pipelines');
      if (res.ok) {
        const data = await res.json();
        if (Array.isArray(data)) {
          setPipelines(data);
        }
      } else {
        // Si falla el backend, dejamos la lista vacía (no mock)
        setPipelines([]);
        // Opcional: toast.error("No se pudo cargar la lista de pipelines");
      }
    } catch (e) {
      console.error("Error de conexión:", e);
      setPipelines([]); // Aseguramos limpieza
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPipelines();
  }, []);

  // --- 2. CARGAR TABLAS DISPONIBLES ---
  const loadTables = async () => {
    setIsLoadingTables(true);
    try {
      const res = await fetch('http://localhost:5000/api/source/tables');
      if (res.ok) {
        const tables = await res.json();
        if (Array.isArray(tables)) {
          setAvailableTables(tables);
          toast.success(`Se detectaron ${tables.length} tablas en Producción`);
        }
      }
    } catch (e) {
      console.error(e);
      toast.error("No se pudieron cargar las tablas");
      setAvailableTables([]);
    } finally {
      setIsLoadingTables(false);
    }
  };

  // --- 3. EJECUTAR PIPELINE ---
const handleRunPipeline = async (id: string) => {
    const pipeline = pipelines.find(p => p.id === id);

    if (id.startsWith('new-') || id.startsWith('real-')) {
        // ... (código de simulación igual) ...
        return;
    }

    setIsRunning(true);
    setPipelines(prev => prev.map(p => p.id === id ? { ...p, status: 'running' } : p));
    const toastId = toast.loading(`Ejecutando ${pipeline?.name}...`);

    try {
      // --- CAMBIO CLAVE AQUÍ ---
      // Enviamos el nombre de la tabla (id) al backend
      const response = await fetch('http://localhost:5000/api/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table: id }) 
      });
      // -------------------------

      let data = { message: "Proceso finalizado." };
      try { data = await response.json(); } catch(e) {}

      if (response.ok) {
        setPipelines(prev => prev.map(p => 
          p.id === id ? { 
            ...p, 
            status: 'success', 
            lastRun: new Date().toISOString(),
            recordsProcessed: 150 // Esto se actualizará al recargar
          } : p
        ));
        toast.dismiss(toastId);
        toast.success("¡Éxito!", { description: data.message });
        
        // Recargar lista para ver datos reales de la BD
        fetchPipelines();
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

  // --- 4. CREAR NUEVO PIPELINE ---
  const handleCreateJob = async () => {
    if (userRole !== 'admin') return toast.error("Acceso Denegado");
    if (!newJob.name || !newJob.table) return toast.error("Faltan datos");

    const toastId = toast.loading("Configurando pipeline...");

    try {
      const response = await fetch('http://localhost:5000/api/pipelines', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newJob)
      });

      const data = await response.json();

      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Pipeline Creado", { description: data.message });
        setIsModalOpen(false);
        setNewJob({ name: "", table: "" });
        fetchPipelines(); // Recargar lista inmediatamente
      } else {
        throw new Error(data.message);
      }
    } catch (error: any) {
      toast.dismiss(toastId);
      toast.error("Error al crear", { description: "No se pudo guardar en el servidor." });
    }
  };

  return (
    <Layout>
      <Header title="Gestión de Pipelines" description="Control de migración de datos sensibles" />
      
      <div className="p-6 space-y-6">
        {/* Barra Superior */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <span className="text-sm text-muted-foreground bg-muted px-3 py-1 rounded-full border border-border/50 flex items-center gap-2">
              <Database className="h-3 w-3" /> Ambiente: <strong>Supabase Cloud</strong>
            </span>
            
            <span className={`text-xs px-2 py-1 rounded font-bold border ${
              userRole === 'admin' 
                ? 'bg-purple-100 text-purple-700 border-purple-200' 
                : 'bg-blue-100 text-blue-700 border-blue-200'
            }`}>
              {userRole === 'admin' ? 'DESARROLLADOR' : 'OPERADOR'}
            </span>
          </div>

          {userRole === 'admin' ? (
            <Dialog open={isModalOpen} onOpenChange={(open) => {
                setIsModalOpen(open);
                if (open) loadTables();
            }}>
                <DialogTrigger asChild>
                    <Button disabled={isRunning}>
                        <Plus className="h-4 w-4 mr-2" /> Nuevo Pipeline
                    </Button>
                </DialogTrigger>
                <DialogContent className="sm:max-w-[425px] bg-card border-border">
                    <DialogHeader>
                        <DialogTitle>Auto-Discovery Pipeline</DialogTitle>
                        <DialogDescription>Detectar tablas disponibles en Producción.</DialogDescription>
                    </DialogHeader>
                    <div className="grid gap-4 py-4">
                        <div className="grid gap-2">
                            <Label htmlFor="table">Tabla Origen</Label>
                            <Select onValueChange={(val) => {
                                setNewJob({ table: val, name: `Migración ${val.charAt(0).toUpperCase() + val.slice(1)}` });
                            }}>
                                <SelectTrigger>
                                    <SelectValue placeholder={isLoadingTables ? "Escaneando..." : "Seleccionar tabla..."} />
                                </SelectTrigger>
                                <SelectContent>
                                    {availableTables.map((table) => (
                                        <SelectItem key={table} value={table}>{table}</SelectItem>
                                    ))}
                                    {availableTables.length === 0 && !isLoadingTables && (
                                        <SelectItem value="none" disabled>No se encontraron tablas</SelectItem>
                                    )}
                                </SelectContent>
                            </Select>
                        </div>
                        <div className="grid gap-2">
                            <Label htmlFor="name">Nombre del Job</Label>
                            <Input 
                                id="name" 
                                value={newJob.name}
                                onChange={(e) => setNewJob({...newJob, name: e.target.value})}
                            />
                        </div>
                    </div>
                    <DialogFooter>
                        <Button onClick={handleCreateJob} disabled={isLoadingTables}>
                            {isLoadingTables ? <RefreshCw className="animate-spin h-4 w-4" /> : "Auto-Configurar"}
                        </Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>
          ) : (
            <Button disabled variant="secondary" className="opacity-70 cursor-not-allowed">
                <Lock className="h-3 w-3 mr-2" /> Solo Lectura
            </Button>
          )}
        </div>

        {/* ESTADO DE CARGA / VACÍO / DATOS */}
        {loading ? (
             <div className="flex justify-center py-12"><RefreshCw className="h-8 w-8 animate-spin text-primary" /></div>
        ) : (
            <>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                    {pipelines?.map((pipeline) => (
                        <div key={pipeline.id} className="animate-fade-in">
                            <PipelineCard 
                                pipeline={pipeline} 
                                onRun={handleRunPipeline}
                                onConfigure={() => toast.info("Ver config.yaml")}
                            />
                        </div>
                    ))}
                </div>
                
                {/* Mensaje si no hay nada real */}
                {(!pipelines || pipelines.length === 0) && (
                    <div className="text-center p-12 border-2 border-dashed rounded-xl bg-muted/10">
                        <p className="text-muted-foreground text-lg font-medium">No hay pipelines configurados.</p>
                        {userRole === 'admin' && <p className="text-sm text-muted-foreground mt-2">Haz clic en "Nuevo Pipeline" para comenzar.</p>}
                    </div>
                )}
            </>
        )}
      </div>
    </Layout>
  );
};

export default Pipelines;