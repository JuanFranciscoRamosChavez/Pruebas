import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { PipelineCard } from "@/components/PipelineCard";
import { Button } from "@/components/ui/button";
import { Plus, Database, Lock, RefreshCw, Percent, Play, Layers, Sprout } from "lucide-react";
import { toast } from "sonner";
import { useState, useEffect } from "react";
import { Pipeline } from "@/types/pipeline";
import { Slider } from "@/components/ui/slider";

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
  const [pipelines, setPipelines] = useState<Pipeline[]>([]); 
  const [loading, setLoading] = useState(true);
  
  // Modals
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [isRunModalOpen, setIsRunModalOpen] = useState(false);
  const [isSeedModalOpen, setIsSeedModalOpen] = useState(false);

  // States
  const [newJob, setNewJob] = useState({ name: "", table: "" });
  const [targetPipeline, setTargetPipeline] = useState<string | null>(null);
  const [runPercentage, setRunPercentage] = useState(100);
  const [isRunning, setIsRunning] = useState(false);
  
  // Seed State - Total de registros a generar
  const [seedCounts, setSeedCounts] = useState({ 
    productos: 30, 
    clientes: 50, 
    ordenes: 100,
    detalles: 300 // Total de detalles a generar
  });
  const [isSeeding, setIsSeeding] = useState(false);

  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [isLoadingTables, setIsLoadingTables] = useState(false);

  const fetchPipelines = async () => {
    setLoading(true);
    try {
      const res = await fetch('http://localhost:5000/api/pipelines');
      if (res.ok) {
        const data = await res.json();
        if (Array.isArray(data)) setPipelines(data);
      } else {
        setPipelines([]);
      }
    } catch (e) {
      console.error("Error de conexion:", e);
      setPipelines([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchPipelines(); }, []);

  const loadTables = async () => {
    setIsLoadingTables(true);
    try {
      const res = await fetch('http://localhost:5000/api/source/tables');
      if (res.ok) {
        const tables = await res.json();
        if (Array.isArray(tables)) {
          setAvailableTables(tables);
          toast.success(`Se detectaron ${tables.length} tablas en Produccion`);
        }
      }
    } catch (e) {
      toast.error("No se pudieron cargar las tablas");
    } finally {
      setIsLoadingTables(false);
    }
  };

  // --- SEEDING (GENERAR DATOS) ---
  const handleSeedData = async () => {
    setIsSeeding(true);
    const toastId = toast.loading("Generando datos semilla en Producción (Esto puede tardar)...");
    
    try {
      const response = await fetch('http://localhost:5000/api/source/seed', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(seedCounts)
      });

      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Datos Generados", { description: "La base de datos de producción ha sido reiniciada con nuevos datos." });
        setIsSeedModalOpen(false);
      } else {
        throw new Error("Error al generar datos");
      }
    } catch (error) {
      toast.dismiss(toastId);
      toast.error("Fallo al generar datos");
    } finally {
      setIsSeeding(false);
    }
  };

  // Ejecución Pipelines
  const openSingleRun = (id: string) => {
    setTargetPipeline(id);
    setRunPercentage(100);
    setIsRunModalOpen(true);
  };

  const openAllRun = () => {
    setTargetPipeline(null);
    setRunPercentage(100);
    setIsRunModalOpen(true);
  };

  const executeRun = async () => {
    setIsRunModalOpen(false);
    setIsRunning(true);
    
    if (targetPipeline) {
        setPipelines(prev => prev.map(p => p.id === targetPipeline ? { ...p, status: 'running' } : p));
    } else {
        setPipelines(prev => prev.map(p => ({ ...p, status: 'running' })));
    }

    const toastId = toast.loading(targetPipeline ? "Ejecutando pipeline..." : "Ejecutando TODOS los pipelines...");

    try {
      const response = await fetch('http://localhost:5000/api/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
            table: targetPipeline,
            percentage: runPercentage 
        }) 
      });

      const data = await response.json();

      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Exito", { description: data.message });
        fetchPipelines();
      } else {
        throw new Error(data.message || `Error ${response.status}`);
      }
    } catch (error: any) {
      toast.dismiss(toastId);
      toast.error("Error", { description: error.message });
      fetchPipelines();
    } finally {
      setIsRunning(false);
      setTargetPipeline(null);
    }
  };

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

      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Pipeline Creado");
        setIsCreateModalOpen(false);
        setNewJob({ name: "", table: "" });
        fetchPipelines(); 
      } else {
        throw new Error("Error al crear");
      }
    } catch (error: any) {
      toast.dismiss(toastId);
      toast.error("Error al crear");
    }
  };

  return (
    <Layout>
      <Header title="Gestion de Pipelines" description="Control de migracion de datos sensibles" />
      
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <span className="text-sm text-muted-foreground bg-muted px-3 py-1 rounded-full border border-border/50 flex items-center gap-2">
              <Database className="h-3 w-3" /> Ambiente: <strong>Supabase Cloud</strong>
            </span>
            <span className={`text-xs px-2 py-1 rounded font-bold border ${
              userRole === 'admin' ? 'bg-purple-100 text-purple-700 border-purple-200' : 'bg-blue-100 text-blue-700 border-blue-200'
            }`}>
              {userRole === 'admin' ? 'DBA' : 'Desarrollador/Tester'}
            </span>
          </div>

          <div className="flex gap-2">
            {/* BOTÓN GENERAR DATOS (Solo DBA) */}
            {userRole === 'admin' && (
                <Dialog open={isSeedModalOpen} onOpenChange={setIsSeedModalOpen}>
                    <DialogTrigger asChild>
                        <Button variant="outline" className="text-green-600 border-green-600/30 hover:bg-green-50">
                            <Sprout className="h-4 w-4 mr-2" />
                            Generar Datos Fuente
                        </Button>
                    </DialogTrigger>
                    <DialogContent className="sm:max-w-[400px]">
                        <DialogHeader>
                            <DialogTitle>Sembrar Datos en Producción</DialogTitle>
                            <DialogDescription>
                                ADVERTENCIA: Esto borrará los datos actuales y generará nuevos.
                            </DialogDescription>
                        </DialogHeader>
                        <div className="grid gap-4 py-2">
                            <div className="grid grid-cols-2 items-center gap-4">
                                <Label htmlFor="prods">Productos</Label>
                                <Input id="prods" type="number" value={seedCounts.productos} onChange={(e) => setSeedCounts({...seedCounts, productos: Number(e.target.value)})} />
                            </div>
                            <div className="grid grid-cols-2 items-center gap-4">
                                <Label htmlFor="clients">Clientes</Label>
                                <Input id="clients" type="number" value={seedCounts.clientes} onChange={(e) => setSeedCounts({...seedCounts, clientes: Number(e.target.value)})} />
                            </div>
                            <div className="grid grid-cols-2 items-center gap-4">
                                <Label htmlFor="orders">Órdenes</Label>
                                <Input id="orders" type="number" value={seedCounts.ordenes} onChange={(e) => setSeedCounts({...seedCounts, ordenes: Number(e.target.value)})} />
                            </div>
                            <div className="grid grid-cols-2 items-center gap-4">
                                <Label htmlFor="details">Total Detalles</Label>
                                <Input id="details" type="number" value={seedCounts.detalles} onChange={(e) => setSeedCounts({...seedCounts, detalles: Number(e.target.value)})} />
                            </div>
                        </div>
                        <DialogFooter>
                            <Button variant="destructive" onClick={handleSeedData} disabled={isSeeding}>
                                {isSeeding ? <RefreshCw className="animate-spin h-4 w-4" /> : "Confirmar Regeneración"}
                            </Button>
                        </DialogFooter>
                    </DialogContent>
                </Dialog>
            )}

            <Button 
                variant="default" 
                className="bg-orange-600 hover:bg-orange-700 text-white"
                onClick={openAllRun}
                disabled={isRunning || pipelines.length === 0}
            >
                <Layers className="h-4 w-4 mr-2" />
                {isRunning ? "Procesando..." : "Ejecutar Todo"}
            </Button>

            {userRole === 'admin' ? (
                <Dialog open={isCreateModalOpen} onOpenChange={(open) => { setIsCreateModalOpen(open); if (open) loadTables(); }}>
                    <DialogTrigger asChild>
                        <Button disabled={isRunning}>
                            <Plus className="h-4 w-4 mr-2" /> Nuevo Pipeline
                        </Button>
                    </DialogTrigger>
                    <DialogContent className="sm:max-w-[425px] bg-card border-border">
                        <DialogHeader>
                            <DialogTitle>Auto-Discovery Pipeline</DialogTitle>
                            <DialogDescription>Detectar tablas disponibles en Produccion.</DialogDescription>
                        </DialogHeader>
                        <div className="grid gap-4 py-4">
                            <div className="grid gap-2">
                                <Label htmlFor="table">Tabla Origen</Label>
                                <Select onValueChange={(val) => {
                                    setNewJob({ table: val, name: `Migracion ${val.charAt(0).toUpperCase() + val.slice(1)}` });
                                }}>
                                    <SelectTrigger>
                                        <SelectValue placeholder={isLoadingTables ? "Escaneando..." : "Seleccionar tabla..."} />
                                    </SelectTrigger>
                                    <SelectContent>
                                        {availableTables.map((table) => (
                                            <SelectItem key={table} value={table}>{table}</SelectItem>
                                        ))}
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
        </div>

        <Dialog open={isRunModalOpen} onOpenChange={setIsRunModalOpen}>
            <DialogContent className="sm:max-w-[425px] bg-card border-border">
                <DialogHeader>
                    <DialogTitle>
                        {targetPipeline ? "Ejecutar Pipeline" : "Ejecución Masiva"}
                    </DialogTitle>
                    <DialogDescription>
                        Configura el volumen de datos a procesar.
                    </DialogDescription>
                </DialogHeader>
                
                <div className="grid gap-6 py-4">
                    <div className="grid gap-4 p-4 border border-border rounded-lg bg-muted/20">
                        <div className="flex justify-between items-center">
                            <Label className="flex items-center gap-2">
                                <Percent className="h-4 w-4 text-primary" />
                                Porcentaje de Datos
                            </Label>
                            <span className="text-sm font-bold font-mono bg-primary/10 px-2 py-1 rounded text-primary">
                                {runPercentage}%
                            </span>
                        </div>
                        <Slider 
                            value={[runPercentage]} 
                            onValueChange={(val) => setRunPercentage(val[0])} 
                            max={100} 
                            step={5} 
                            className="py-2"
                        />
                        <p className="text-xs text-muted-foreground">
                            {runPercentage === 100 
                                ? "Se migrara TODA la información." 
                                : `Se seleccionara aleatoriamente el ${runPercentage}% de los registros.`}
                        </p>
                    </div>
                </div>

                <DialogFooter>
                    <Button variant="outline" onClick={() => setIsRunModalOpen(false)}>Cancelar</Button>
                    <Button onClick={executeRun} className={!targetPipeline ? "bg-orange-600 hover:bg-orange-700" : ""}>
                        <Play className="h-4 w-4 mr-2" /> 
                        {targetPipeline ? "Confirmar" : "Ejecutar Todo"}
                    </Button>
                </DialogFooter>
            </DialogContent>
        </Dialog>

        {loading ? (
             <div className="flex justify-center py-12"><RefreshCw className="h-8 w-8 animate-spin text-primary" /></div>
        ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {pipelines?.map((pipeline) => (
                    <div key={pipeline.id} className="animate-fade-in">
                        <PipelineCard 
                            pipeline={pipeline} 
                            onRun={openSingleRun} 
                            onConfigure={() => toast.info("Ver config.yaml")}
                        />
                    </div>
                ))}
            </div>
        )}
      </div>
    </Layout>
  );
};

export default Pipelines;