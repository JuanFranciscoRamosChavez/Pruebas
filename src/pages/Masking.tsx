import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { MaskingRuleCard } from "@/components/MaskingRuleCard";
import { Button } from "@/components/ui/button";
import { Plus, Filter, Info, Lock, RefreshCw, RotateCcw, X } from "lucide-react"; // Agregamos X para limpiar
import { useState, useEffect } from "react";
import { toast } from "sonner";
import {
  Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle, DialogTrigger
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { 
  DropdownMenu, 
  DropdownMenuContent, 
  DropdownMenuItem, 
  DropdownMenuLabel, 
  DropdownMenuSeparator, 
  DropdownMenuTrigger,
  DropdownMenuCheckboxItem
} from "@/components/ui/dropdown-menu";
import { MaskingRule } from "@/types/pipeline";
import { Badge } from "@/components/ui/badge";

interface MaskingProps { userRole?: string; }

const Masking = ({ userRole }: MaskingProps) => {
  const [rules, setRules] = useState<MaskingRule[]>([]);
  const [loading, setLoading] = useState(true);
  const [isModalOpen, setIsModalOpen] = useState(false);
  
  // --- ESTADO PARA EL FILTRO ---
  const [filterTable, setFilterTable] = useState<string | null>(null);

  // Formulario
  const [tables, setTables] = useState<string[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [newRule, setNewRule] = useState({ table: "", column: "", type: "" });

  const adaptBackendRule = (backendRule: any): MaskingRule => {
    let frontendType: any = 'custom';
    if (backendRule.type === 'hash_email') frontendType = 'email';
    if (backendRule.type === 'fake_name') frontendType = 'name';
    if (backendRule.type === 'preserve_format') frontendType = 'phone';
    if (backendRule.type === 'redact') frontendType = 'ssn';

    return {
        id: backendRule.id || Math.random().toString(),
        name: backendRule.name || "Regla sin nombre",
        type: frontendType,
        replacement: backendRule.type || "custom",
        tables: backendRule.table ? [backendRule.table] : [],
        columns: backendRule.column ? [backendRule.column] : [],
        isActive: backendRule.isActive ?? true
    };
  };

  const fetchRules = async () => {
    setLoading(true);
    try {
      const res = await fetch('http://localhost:5000/api/rules');
      if (res.ok) {
        const data = await res.json();
        if (Array.isArray(data)) {
          setRules(data.map(adaptBackendRule));
        } else {
          setRules([]);
        }
      }
    } catch (e) {
      setRules([]); 
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchRules(); }, []);

  const loadTables = () => {
    fetch('http://localhost:5000/api/pipelines')
      .then(res => res.json())
      .then(data => {
          if (Array.isArray(data)) {
            setTables(data.map((p: any) => p.id));
          }
      })
      .catch(() => toast.error("Error cargando tablas"));
  };

  const handleTableSelect = (table: string) => {
    setNewRule({ ...newRule, table, column: "" });
    setColumns([]);
    fetch(`http://localhost:5000/api/source/columns/${table}`)
      .then(res => res.json())
      .then(data => {
          if (Array.isArray(data)) setColumns(data);
      })
      .catch(() => toast.error("Error cargando columnas"));
  };

  const handleCreateRule = async () => {
    if (!newRule.table || !newRule.column || !newRule.type) return toast.error("Faltan datos");
    
    const toastId = toast.loading("Aplicando regla...");
    try {
      const res = await fetch('http://localhost:5000/api/rules', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(newRule)
      });
      if (res.ok) {
        toast.dismiss(toastId);
        toast.success("Regla creada exitosamente");
        setIsModalOpen(false);
        fetchRules(); 
        setNewRule({ table: "", column: "", type: "" });
      } else throw new Error();
    } catch (e) { 
        toast.dismiss(toastId);
        toast.error("Error al guardar"); 
    }
  };

  const handleToggleRule = async (id: string, isActive: boolean) => {
    if (userRole !== 'admin') return toast.error("Acceso Denegado");
    
    const rule = rules.find(r => r.id === id);
    if (!rule) return;

    if (!isActive) {
        const toastId = toast.loading("Eliminando regla...");
        try {
            const res = await fetch('http://localhost:5000/api/rules', {
                method: 'DELETE',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ table: rule.tables[0], column: rule.columns[0] })
            });
            
            if (res.ok) {
                toast.dismiss(toastId);
                toast.success("Regla eliminada");
                fetchRules();
            } else throw new Error();
        } catch(e) { 
            toast.dismiss(toastId);
            toast.error("Error al eliminar"); 
        }
    }
  };

  const handleResetDefaults = async () => {
    if (userRole !== 'admin') return toast.error("Acceso Denegado");
    const toastId = toast.loading("Restaurando...");
    try {
        const res = await fetch('http://localhost:5000/api/rules/reset', { method: 'POST' });
        if (res.ok) {
            toast.dismiss(toastId);
            toast.success("Restaurado");
            fetchRules(); 
        } else throw new Error();
    } catch (e) { toast.dismiss(toastId); toast.error("Error"); }
  };

  // --- LÓGICA DE FILTRADO ---
  // Obtener tablas únicas presentes en las reglas actuales
  const uniqueTables = Array.from(new Set(rules.flatMap(r => r.tables)));

  // Filtrar reglas
  const filteredRules = filterTable 
    ? rules.filter(r => r.tables.includes(filterTable))
    : rules;

  return (
    <Layout>
      <Header title="Reglas de Enmascaramiento" description="Gestión de privacidad de datos (PII)" />
      
      <div className="p-6 space-y-6">
        <div className="flex items-start gap-3 p-4 rounded-lg bg-blue-50 border border-blue-200 text-blue-800">
          <Info className="h-5 w-5 mt-0.5" />
          <div>
            <p className="text-sm font-bold">Protección Activa</p>
            <p className="text-sm opacity-90">Las reglas se aplican en la siguiente ejecución.</p>
          </div>
        </div>

        <div className="flex justify-between items-center">
           <div className="flex items-center gap-3">
              
              {/* --- BOTÓN FILTRAR FUNCIONAL --- */}
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant={filterTable ? "secondary" : "outline"} size="sm" className="gap-2">
                    <Filter className="h-4 w-4" /> 
                    {filterTable ? `Filtro: ${filterTable}` : "Filtrar por Tabla"}
                    {filterTable && <Badge variant="secondary" className="ml-1 bg-white/20 text-xs h-5 px-1.5">{filteredRules.length}</Badge>}
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="start" className="w-56">
                  <DropdownMenuLabel>Selecciona una tabla</DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={() => setFilterTable(null)}>
                    Mostrar Todas
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  {uniqueTables.map(table => (
                    <DropdownMenuCheckboxItem 
                      key={table}
                      checked={filterTable === table}
                      onCheckedChange={() => setFilterTable(table === filterTable ? null : table)}
                    >
                      {table}
                    </DropdownMenuCheckboxItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>

              {/* Botón para limpiar filtro rápido si está activo */}
              {filterTable && (
                <Button variant="ghost" size="icon" className="h-8 w-8 text-muted-foreground" onClick={() => setFilterTable(null)}>
                    <X className="h-4 w-4" />
                </Button>
              )}

              <span className="text-sm text-muted-foreground font-mono border-l pl-3 ml-1">
                {filteredRules.length} de {rules.length} reglas
              </span>
              
              <Button variant="ghost" size="sm" onClick={fetchRules}>
                  <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`}/>
              </Button>
           </div>

           <div className="flex gap-2">
             {userRole === 'admin' && (
                <Button variant="secondary" onClick={handleResetDefaults}>
                    <RotateCcw className="h-4 w-4 mr-2" /> Restaurar
                </Button>
             )}

             {userRole === 'admin' ? (
               <Dialog open={isModalOpen} onOpenChange={(open) => { setIsModalOpen(open); if(open) loadTables(); }}>
                 <DialogTrigger asChild><Button><Plus className="h-4 w-4 mr-2"/> Nueva Regla</Button></DialogTrigger>
                 <DialogContent>
                   <DialogHeader><DialogTitle>Agregar Regla</DialogTitle></DialogHeader>
                   <div className="grid gap-4 py-4">
                      <div className="grid gap-2"><Label>Tabla</Label>
                          <Select onValueChange={handleTableSelect}>
                              <SelectTrigger><SelectValue placeholder="Selecciona tabla..."/></SelectTrigger>
                              <SelectContent>{tables.map(t => <SelectItem key={t} value={t}>{t}</SelectItem>)}</SelectContent>
                          </Select>
                      </div>
                      <div className="grid gap-2"><Label>Columna</Label>
                          <Select onValueChange={v => setNewRule({...newRule, column: v})} disabled={!columns.length}>
                              <SelectTrigger><SelectValue placeholder="Selecciona columna..."/></SelectTrigger>
                              <SelectContent>{columns.map(c => <SelectItem key={c} value={c}>{c}</SelectItem>)}</SelectContent>
                          </Select>
                      </div>
                      <div className="grid gap-2"><Label>Tipo</Label>
                          <Select onValueChange={v => setNewRule({...newRule, type: v})}>
                              <SelectTrigger><SelectValue placeholder="Método..."/></SelectTrigger>
                              <SelectContent>
                                  <SelectItem value="hash_email">Hash (Email)</SelectItem>
                                  <SelectItem value="fake_name">Faker (Nombre)</SelectItem>
                                  <SelectItem value="preserve_format">Formato Preservado</SelectItem>
                                  <SelectItem value="redact">Redacción (****)</SelectItem>
                              </SelectContent>
                          </Select>
                      </div>
                   </div>
                   <DialogFooter><Button onClick={handleCreateRule}>Guardar</Button></DialogFooter>
                 </DialogContent>
               </Dialog>
             ) : (
               <Button disabled variant="secondary"><Lock className="h-4 w-4 mr-2"/> Solo Lectura</Button>
             )}
           </div>
        </div>

        {/* Grid de Reglas (FILTRADO) */}
        {loading ? (
            <div className="flex justify-center py-12"><RefreshCw className="h-8 w-8 animate-spin text-primary" /></div>
        ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {filteredRules.map((rule) => (
                    <MaskingRuleCard key={rule.id} rule={rule} onToggle={handleToggleRule} />
                ))}
                {!loading && filteredRules.length === 0 && (
                    <div className="col-span-3 text-center p-12 border-2 border-dashed rounded-xl">
                        <p className="text-muted-foreground">
                            {rules.length > 0 ? "No hay reglas para esta tabla." : "No hay reglas configuradas."}
                        </p>
                    </div>
                )}
            </div>
        )}
      </div>
    </Layout>
  );
};

export default Masking;