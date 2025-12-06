import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { PipelineCard } from "@/components/PipelineCard";
import { Button } from "@/components/ui/button";
import { Database, Lock, RefreshCw, Percent, Play, Layers, Sprout, Save, AlertTriangle, ShieldAlert } from "lucide-react";
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

interface PipelinesProps {
  userRole?: string;
}

const Pipelines = ({ userRole }: PipelinesProps) => {
  const [pipelines, setPipelines] = useState<Pipeline[]>([]); 
  const [loading, setLoading] = useState(true);
  
  // Modals
  const [isRunModalOpen, setIsRunModalOpen] = useState(false);
  const [isSeedModalOpen, setIsSeedModalOpen] = useState(false);

  // States
  const [targetPipeline, setTargetPipeline] = useState<string | null>(null);
  const [runPercentage, setRunPercentage] = useState(100);
  const [isRunning, setIsRunning] = useState(false);
  
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

  // --- UTILIDAD PARA MOSTRAR ERRORES CON FORMATO ---
  const showSmartError = (title: string, message: string) => {
    // Detectamos si es un error de seguridad del Backend
    const isSecurityError = message.includes("SEGURIDAD") || message.includes("SECURITY");
    
    toast.error(title, {
        duration: isSecurityError ? 10000 : 5000, // Más tiempo si es crítico
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

  // --- FUNCIÓN DE RESPALDO CIFRADO ---
  const handleBackup = async () => {
    setIsBackingUp(true);
    const toastId = toast.loading("Generando respaldo SQL Cifrado...");
    
    try {
        const response = await fetch('http://localhost:5000/api/backup', {
            method: 'POST'
        });
        const data = await response.json();
        
        if (response.ok) {
            toast.dismiss(toastId);
            toast.success("Respaldo Seguro Creado", { 
                description: `Archivo guardado en el servidor: ${data.file}` 
            });
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

  // --- SEEDING (GENERAR DATOS) ---
  const handleSeedData = async () => {
    setIsSeeding(true);
    const toastId = toast.loading("Verificando entornos y generando datos...");
    
    try {
      const response = await fetch('http://localhost:5000/api/source/seed', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(seedCounts)
      });

      // Si falla (por ejemplo, por seguridad), leemos el error JSON
      const data = await response.json();

      if (response.ok) {
        toast.dismiss(toastId);
        toast.success("Datos Generados", { description: "La base de datos de producción ha sido reiniciada con nuevos datos." });
        setIsSeedModalOpen(false);
      } else {
        // Lanzamos el error con el mensaje que viene del backend
        throw new Error(data.error || "Error desconocido en el servidor");
      }
    } catch (error: any) {
      toast.dismiss(toastId);
      // Usamos la nueva función visual
      showSmartError("Fallo Crítico", error.message);
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
        throw new Error(data.error || data.message || `Error ${response.status}`);
      }
    } catch (error: any) {
      toast.dismiss(toastId);
      // Usamos la nueva función visual aquí también
      showSmartError("Ejecución Detenida", error.message);
      
      // Actualizamos estado visual a error
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
            
            {/* BOTÓN 1: RESPALDO (SOLO DBA) */}
            {userRole === 'admin' && (
                <Button 
                    variant="outline" 
                    className="gap-2"
                    onClick={handleBackup}
                    disabled={isBackingUp}
                >
                    <Save className={`h-4 w-4 ${isBackingUp ? 'animate-pulse' : ''}`} />
                    {isBackingUp ? "Guardando..." : "Crear Respaldo"}
                </Button>
            )}

            {/* BOTÓN 2: GENERAR DATOS (SOLO DBA) */}
            {userRole === 'admin' && (
                <Dialog open={isSeedModalOpen} onOpenChange={setIsSeedModalOpen}>
                    <DialogTrigger asChild>
                        <Button 
                            variant="outline"
                            className="gap-2 hover:bg-muted/80 border-emerald-600 text-emerald-600 hover:bg-emerald-600 hover:text-white transition-colors"
                        >
                            <Sprout className="h-4 w-4" />
                            Generar Datos Fuente
                        </Button>
                    </DialogTrigger>
                    <DialogContent className="sm:max-w-[450px]">
                        <DialogHeader>
                            <DialogTitle>Sembrar Datos en Producción</DialogTitle>
                            <DialogDescription>
                                Configura el volumen de datos a generar aleatoriamente.
                            </DialogDescription>
                        </DialogHeader>
                        
                        <div className="bg-muted/40 border border-primary/10 rounded-md p-4 my-2 text-sm text-foreground flex flex-col gap-3">
                            <div className="flex items-start gap-3">
                                <AlertTriangle className="h-5 w-5 mt-0.5 text-orange-500 shrink-0" />
                                <div className="space-y-1">
                                    <p className="font-semibold">¡Atención!</p>
                                    <p className="text-muted-foreground text-xs">
                                        Esta acción <strong>borrará permanentemente</strong> todos los datos actuales en Producción y QA para generar nuevos registros.
                                    </p>
                                </div>
                            </div>
                            
                            <div className="flex justify-center pt-1">
                                <Button 
                                    size="sm" 
                                    variant="outline" 
                                    className="h-8 gap-2 border-primary/20 hover:bg-background hover:text-primary transition-colors"
                                    onClick={handleBackup}
                                    disabled={isBackingUp}
                                >
                                    <Save className="h-3.5 w-3.5" />
                                    {isBackingUp ? "Realizando copia..." : "Recomendado: Hacer copia de seguridad antes"}
                                </Button>
                            </div>
                        </div>

                        <div className="grid gap-4 py-2">
                            <div className="grid grid-cols-2 items-center gap-4">
                                <Label htmlFor="prods">Inventario (Productos)</Label>
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
                            <Button variant="default" onClick={handleSeedData} disabled={isSeeding || isBackingUp}>
                                {isSeeding ? <RefreshCw className="animate-spin h-4 w-4 mr-2" /> : null}
                                {isSeeding ? "Generando..." : "Confirmar Regeneración"}
                            </Button>
                        </DialogFooter>
                    </DialogContent>
                </Dialog>
            )}

            {/* BOTÓN EJECUTAR TODO (SOLO DBA) */}
            {userRole === 'admin' ? (
                <Button 
                    variant="default" 
                    className="bg-orange-600 hover:bg-orange-700 text-white gap-2"
                    onClick={openAllRun}
                    disabled={isRunning || pipelines.length === 0}
                >
                    <Layers className="h-4 w-4" />
                    {isRunning ? "Procesando..." : "Ejecutar Todo"}
                </Button>
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
                            onRun={userRole === 'admin' ? openSingleRun : undefined} 
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