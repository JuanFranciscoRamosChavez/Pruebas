import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { PipelineCard } from "@/components/PipelineCard";
import { Button } from "@/components/ui/button";
import { Database, Lock, RefreshCw, Percent, Play, Layers, Sprout, Save, AlertTriangle, ShieldAlert, Plus, RotateCw } from "lucide-react";
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
  const [isRunModalOpen, setIsRunModalOpen] = useState(false);
  const [isSeedModalOpen, setIsSeedModalOpen] = useState(false);
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);

  // States
  const [targetPipeline, setTargetPipeline] = useState<string | null>(null);
  const [runPercentage, setRunPercentage] = useState(100);
  const [isRunning, setIsRunning] = useState(false);
  
  // Create Job State
  const [newJob, setNewJob] = useState({ name: "", table: "" });
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [isLoadingTables, setIsLoadingTables] = useState(false);

  // Seed State
  const [seedCounts, setSeedCounts] = useState({ 
    productos: 30, 
    clientes: 50, 
    ordenes: 100,
    detalles: 300 
  });
  const [isSeeding, setIsSeeding] = useState(false);
  
  // Backup State
  const [isBackingUp, setIsBackingUp] = useState(false);

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

  const showSmartError = (title: string, message: string) => {
    const isSecurityError = message.includes("SEGURIDAD") || message.includes("SECURITY");
    
    toast.error(title, {
        duration: isSecurityError ? 10000 : 5000,
        description: (
            <div className={`mt-2 p-3 rounded-md border text-xs whitespace-pre-wrap font-mono shadow-sm ${
                isSecurityError 
                    ? "bg-red-50 border-red-200 text-red-700 dark:bg-red-950/20 dark:border-red-900 dark:text-red-400" 
                    : "bg-muted/50 border-border text-muted-foreground"
            }`}>
                {isSecurityError && <div className="flex items-center gap-2 mb-2 font-bold border-b border-red-200/50 pb-1">
                    <ShieldAlert className="h-4 w-4" /> BLOQUEO DE SEGURIDAD
                </div>}
                {message}
            </div>
        )
    });
  };

  const handleBackup = async () => {
    setIsBackingUp(true);
    const toastId = toast.loading("Generando respaldo SQL Cifrado...");
    
    try {
        const response = await fetch('http://localhost:5000/api/backup', { method: 'POST' });
        const data = await response.json();
        
        if (response.ok) {
            toast.dismiss(toastId);
            toast.success("Respaldo Seguro Creado", { description: `Archivo: ${data.file}` });
        } else {
            throw new Error(data.error || "Error al respaldar");
        }
    } catch (e: any) {
        toast.dismiss(toastId);
        showSmartError("Error de Respaldo", e.message);
    } finally {
        setIsBackingUp(false);
    }
  };

  const handleSeedData = async () => {
    setIsSeeding(true);
    const toastId = toast.loading("Verificando entornos y generando datos...");
    
    try {
      const response = await fetch('http://localhost:5000/api/source/seed', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(seedCounts)
      });
      const data = await response.json();

      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Datos Generados", { description: "Base de datos reiniciada." });
        setIsSeedModalOpen(false);
      } else {
        throw new Error(data.error || "Error desconocido");
      }
    } catch (error: any) {
      toast.dismiss(toastId);
      showSmartError("Fallo Crítico", error.message);
    } finally {
      setIsSeeding(false);
    }
  };

  const handleDeletePipeline = async (id: string) => {
    if (!confirm("¿Estás seguro de eliminar este pipeline?")) return;
    const toastId = toast.loading("Eliminando pipeline...");
    try {
        const res = await fetch(`http://localhost:5000/api/pipelines/${id}`, { method: 'DELETE' });
        if (res.ok) {
            toast.dismiss(toastId);
            toast.success("Pipeline eliminado");
            setPipelines(prev => prev.filter(p => p.id !== id));
        } else {
            throw new Error("No se pudo eliminar");
        }
    } catch (error: any) {
        toast.dismiss(toastId);
        toast.error("Error al eliminar");
    }
  };

  const handleToggleActive = async (id: string, active: boolean) => {
    setPipelines(prev => prev.map(p => p.id === id ? { ...p, isActive: active } : p));
    try {
        await fetch(`http://localhost:5000/api/pipelines/${id}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ active })
        });
        toast.success(`Pipeline ${active ? 'activado' : 'pausado'}`);
    } catch (error) {
        toast.error("Error al cambiar estado");
        fetchPipelines();
    }
  };

  const handleSyncPipelines = async () => {
      const toastId = toast.loading("Sincronizando con Producción...");
      try {
          const resTables = await fetch('http://localhost:5000/api/source/tables');
          const tables = await resTables.json();
          const currentPipelines = pipelines.map(p => p.id);
          const missing = tables.filter((t: string) => !currentPipelines.includes(t) && t !== '_db_meta');
          
          if (missing.length === 0) {
              toast.dismiss(toastId);
              toast.info("Todo sincronizado", { description: "Todos los pipelines ya existen." });
              return;
          }

          let createdCount = 0;
          for (const table of missing) {
             await fetch('http://localhost:5000/api/pipelines', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: `Migración ${table}`, table: table })
             });
             createdCount++;
          }
          toast.dismiss(toastId);
          toast.success("Sincronización Completada", { description: `Se restauraron ${createdCount} pipelines.` });
          fetchPipelines();
      } catch (e) {
          toast.dismiss(toastId);
          toast.error("Error de sincronización");
      }
  };

  const handleCreateJob = async () => {
    if (userRole !== 'admin') return toast.error("Acceso Denegado");
    if (!newJob.name || !newJob.table) return toast.error("Faltan datos");

    const toastId = toast.loading("Registrando pipeline...");
    try {
      const response = await fetch('http://localhost:5000/api/pipelines', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newJob)
      });
      const data = await response.json();
      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Pipeline Creado");
        setIsCreateModalOpen(false);
        setNewJob({ name: "", table: "" });
        fetchPipelines(); 
      } else {
        throw new Error(data.message || "Error al crear");
      }
    } catch (error: any) {
      toast.dismiss(toastId);
      showSmartError("Error al crear", error.message);
    }
  };

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
        body: JSON.stringify({ table: targetPipeline, percentage: runPercentage }) 
      });

      const data = await response.json();

      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Exito", { description: data.message });
        fetchPipelines();
      } else {
        throw new Error(data.error || data.message || `Error ${response.status}`);
      }
    } catch (error: any) {
      toast.dismiss(toastId);
      showSmartError("Ejecución Detenida", error.message);
      setPipelines(prev => prev.map(p => {
          if (targetPipeline && p.id !== targetPipeline) return p;
          return { ...p, status: 'error' };
      }));
    } finally {
      setIsRunning(false);
      setTargetPipeline(null);
    }
  };

  return (
    <Layout>
      <Header title="Gestion de Pipelines" description="Control de migracion de datos sensibles" />
      <div className="p-6 space-y-6">
        
        {/* BARRA SUPERIOR DE ACCIONES */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <span className="text-sm text-muted-foreground bg-muted px-3 py-1 rounded-full border border-border/50 flex items-center gap-2">
              <Database className="h-3 w-3" /> Ambiente: <strong>Supabase Cloud</strong>
            </span>
            <span className={`text-xs px-2 py-1 rounded font-bold border ${userRole === 'admin' ? 'bg-purple-100 text-purple-700 border-purple-200' : 'bg-blue-100 text-blue-700 border-blue-200'}`}>
              {userRole === 'admin' ? 'DBA' : 'Desarrollador/Tester'}
            </span>
          </div>

          <div className="flex gap-2">
            
            {userRole === 'admin' && (
                <>
                    <Button variant="outline" onClick={handleSyncPipelines} title="Restaurar pipelines faltantes">
                        <RotateCw className="h-4 w-4 mr-2" /> Sincronizar
                    </Button>

                    <Button variant="outline" onClick={handleBackup} disabled={isBackingUp}>
                        <Save className={`h-4 w-4 mr-2 ${isBackingUp ? 'animate-pulse' : ''}`} />
                        {isBackingUp ? "Guardando..." : "Respaldo"}
                    </Button>

                    <Dialog open={isSeedModalOpen} onOpenChange={setIsSeedModalOpen}>
                        <DialogTrigger asChild>
                            {/* CAMBIO: Botón Verde Sólido para mejor contraste */}
                            <Button className="bg-emerald-600 hover:bg-emerald-700 text-white border-0">
                                <Sprout className="h-4 w-4 mr-2" /> Generar Datos
                            </Button>
                        </DialogTrigger>
                        <DialogContent className="sm:max-w-[450px]">
                            <DialogHeader>
                                <DialogTitle>Sembrar Datos</DialogTitle>
                                <DialogDescription>Reiniciar base de datos con datos aleatorios.</DialogDescription>
                            </DialogHeader>
                            
                            {/* ALERTA MEJORADA VISUALMENTE (Estilo neutro/warning) */}
                            <div className="bg-amber-50/50 border border-amber-200 rounded-lg p-4 my-2 flex flex-col gap-3">
                                <div className="flex gap-3">
                                    <div className="p-2 bg-amber-100 rounded-full h-fit">
                                        <AlertTriangle className="h-4 w-4 text-amber-600" />
                                    </div>
                                    <div className="space-y-1">
                                        <h4 className="text-sm font-semibold text-amber-900">¡Atención!</h4>
                                        <p className="text-xs text-amber-700/80 leading-relaxed">
                                            Esta acción <strong>borrará permanentemente</strong> todos los datos actuales en Producción y QA.
                                        </p>
                                    </div>
                                </div>
                                
                                {/* Botón de acción rápida dentro de la alerta */}
                                <Button 
                                    size="sm" 
                                    className="w-full bg-white border border-amber-300 text-amber-800 hover:bg-amber-100 hover:text-amber-900 shadow-sm"
                                    onClick={handleBackup} 
                                    disabled={isBackingUp}
                                >
                                    <Save className="h-3.5 w-3.5 mr-2" /> 
                                    {isBackingUp ? "Realizando copia..." : "Hacer copia de seguridad ahora"}
                                </Button>
                            </div>

                            <div className="grid gap-4 py-2">
                                <div className="grid grid-cols-2 items-center gap-4"><Label>Inventario</Label><Input type="number" value={seedCounts.productos} onChange={(e) => setSeedCounts({...seedCounts, productos: Number(e.target.value)})} /></div>
                                <div className="grid grid-cols-2 items-center gap-4"><Label>Clientes</Label><Input type="number" value={seedCounts.clientes} onChange={(e) => setSeedCounts({...seedCounts, clientes: Number(e.target.value)})} /></div>
                                <div className="grid grid-cols-2 items-center gap-4"><Label>Órdenes</Label><Input type="number" value={seedCounts.ordenes} onChange={(e) => setSeedCounts({...seedCounts, ordenes: Number(e.target.value)})} /></div>
                                <div className="grid grid-cols-2 items-center gap-4"><Label>Detalles</Label><Input type="number" value={seedCounts.detalles} onChange={(e) => setSeedCounts({...seedCounts, detalles: Number(e.target.value)})} /></div>
                            </div>
                            <DialogFooter>
                                <Button variant="destructive" onClick={handleSeedData} disabled={isSeeding || isBackingUp}>
                                    {isSeeding ? <RefreshCw className="animate-spin h-4 w-4 mr-2" /> : "Confirmar Regeneración"}
                                </Button>
                            </DialogFooter>
                        </DialogContent>
                    </Dialog>

                    <Button variant="default" className="bg-orange-600 hover:bg-orange-700 text-white gap-2" onClick={openAllRun} disabled={isRunning || pipelines.length === 0}>
                        <Layers className="h-4 w-4" /> Ejecutar Todo
                    </Button>

                    <Dialog open={isCreateModalOpen} onOpenChange={(open) => { setIsCreateModalOpen(open); if (open) loadTables(); }}>
                        <DialogTrigger asChild>
                            <Button disabled={isRunning}><Plus className="h-4 w-4 mr-2" /> Nuevo</Button>
                        </DialogTrigger>
                        <DialogContent className="sm:max-w-[425px] bg-card border-border">
                            <DialogHeader><DialogTitle>Nuevo Pipeline</DialogTitle></DialogHeader>
                            <div className="grid gap-4 py-4">
                                <div className="grid gap-2">
                                    <Label>Tabla Origen</Label>
                                    <Select onValueChange={(val) => setNewJob({ table: val, name: `Migración ${val}` })}>
                                        <SelectTrigger><SelectValue placeholder={isLoadingTables ? "Escaneando..." : "Seleccionar tabla..."} /></SelectTrigger>
                                        <SelectContent>
                                            {availableTables.map((t) => <SelectItem key={t} value={t}>{t}</SelectItem>)}
                                        </SelectContent>
                                    </Select>
                                </div>
                                <div className="grid gap-2"><Label>Nombre</Label><Input value={newJob.name} onChange={(e) => setNewJob({...newJob, name: e.target.value})} /></div>
                            </div>
                            <DialogFooter>
                                <Button onClick={handleCreateJob} disabled={isLoadingTables}>Crear Pipeline</Button>
                            </DialogFooter>
                        </DialogContent>
                    </Dialog>
                </>
            )}
            
            {userRole !== 'admin' && (
                <Button disabled variant="secondary" className="opacity-70 cursor-not-allowed">
                    <Lock className="h-3 w-3 mr-2" /> Solo Lectura
                </Button>
            )}
          </div>
        </div>

        {/* MODAL DE EJECUCIÓN (Porcentaje) */}
        <Dialog open={isRunModalOpen} onOpenChange={setIsRunModalOpen}>
            <DialogContent className="sm:max-w-[425px] bg-card border-border">
                <DialogHeader>
                    <DialogTitle>{targetPipeline ? "Ejecutar Pipeline" : "Ejecución Masiva"}</DialogTitle>
                    <DialogDescription>Configura el volumen de datos.</DialogDescription>
                </DialogHeader>
                <div className="grid gap-6 py-4">
                    <div className="grid gap-4 p-4 border border-border rounded-lg bg-muted/20">
                        <div className="flex justify-between items-center">
                            <Label className="flex items-center gap-2"><Percent className="h-4 w-4 text-primary" /> Muestreo</Label>
                            <span className="text-sm font-bold font-mono bg-primary/10 px-2 py-1 rounded text-primary">{runPercentage}%</span>
                        </div>
                        <Slider value={[runPercentage]} onValueChange={(val) => setRunPercentage(val[0])} max={100} step={5} className="py-2" />
                    </div>
                </div>
                <DialogFooter>
                    <Button variant="outline" onClick={() => setIsRunModalOpen(false)}>Cancelar</Button>
                    <Button onClick={executeRun}><Play className="h-4 w-4 mr-2" /> Confirmar</Button>
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
                            onRun={userRole === 'admin' ? openSingleRun : undefined}
                            onDelete={userRole === 'admin' ? handleDeletePipeline : undefined}
                            onToggleActive={userRole === 'admin' ? handleToggleActive : undefined}
                            userRole={userRole}
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