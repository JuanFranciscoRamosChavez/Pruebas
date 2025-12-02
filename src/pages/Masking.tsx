import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { MaskingRuleCard } from "@/components/MaskingRuleCard";
import { Button } from "@/components/ui/button";
import { Plus, Filter, Info, Lock } from "lucide-react";
import { mockMaskingRules } from "@/data/mockData";
import { useState } from "react";
import { toast } from "sonner";
import { MaskingRule } from "@/types/pipeline";

// 1. Definimos que recibe el rol
interface MaskingProps {
  userRole?: string;
}

const Masking = ({ userRole }: MaskingProps) => {
  const [rules, setRules] = useState<MaskingRule[]>(mockMaskingRules);

  const handleToggleRule = (id: string, isActive: boolean) => {
    // 2. Seguridad: Bloquear modificación si no es admin
    if (userRole !== 'admin') {
      toast.error("Acceso Denegado", { 
        description: "Solo los desarrolladores pueden modificar las reglas de enmascaramiento." 
      });
      return;
    }

    setRules(prev => 
      prev.map(rule => 
        rule.id === id ? { ...rule, isActive } : rule
      )
    );
    const rule = rules.find(r => r.id === id);
    toast.success(`Regla "${rule?.name}" ${isActive ? 'activada' : 'desactivada'}`);
  };

  const activeCount = rules.filter(r => r.isActive).length;

  return (
    <Layout>
      <Header 
        title="Reglas de Enmascaramiento" 
        description="Configura cómo se anonimizan los datos sensibles antes de cargarlos en QA"
      />
      
      <div className="p-6 space-y-6">
        {/* Info Banner */}
        <div className="flex items-start gap-3 p-4 rounded-lg bg-primary/10 border border-primary/30">
          <Info className="h-5 w-5 text-primary mt-0.5" />
          <div>
            <p className="text-sm font-medium text-foreground">
              Protege los datos sensibles automáticamente
            </p>
            <p className="text-sm text-muted-foreground mt-1">
              Las reglas de enmascaramiento se aplican durante la fase de transformación del pipeline. 
              Los datos originales nunca se copian a ambientes de prueba.
            </p>
          </div>
        </div>

        {/* Actions Bar */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Button variant="outline" size="sm">
              <Filter className="h-4 w-4 mr-2" />
              Filtrar
            </Button>
            <p className="text-sm text-muted-foreground">
              {activeCount} de {rules.length} reglas activas
            </p>

            {/* Indicador Visual de Rol */}
            <span className={`text-xs px-2 py-1 rounded font-bold border ml-2 ${
              userRole === 'admin' 
                ? 'bg-purple-100 text-purple-700 border-purple-200' 
                : 'bg-blue-100 text-blue-700 border-blue-200'
            }`}>
              {userRole === 'admin' ? 'DESARROLLADOR' : 'OPERADOR'}
            </span>
          </div>

          {/* 3. Botón Condicional */}
          {userRole === 'admin' ? (
            <Button onClick={() => toast.info("La creación se gestiona en config.yaml")}>
              <Plus className="h-4 w-4 mr-2" />
              Nueva Regla
            </Button>
          ) : (
            <Button disabled variant="secondary" className="opacity-70 cursor-not-allowed">
              <Lock className="h-4 w-4 mr-2" />
              Solo Lectura
            </Button>
          )}
        </div>

        {/* Rules Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {rules.map((rule, index) => (
            <div 
              key={rule.id} 
              className="animate-slide-up"
              style={{ animationDelay: `${index * 100}ms` }}
            >
              <MaskingRuleCard 
                rule={rule} 
                onToggle={handleToggleRule}
              />
            </div>
          ))}
        </div>
      </div>
    </Layout>
  );
};

export default Masking;