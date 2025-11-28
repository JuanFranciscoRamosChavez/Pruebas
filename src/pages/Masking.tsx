import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { MaskingRuleCard } from "@/components/MaskingRuleCard";
import { Button } from "@/components/ui/button";
import { Plus, Filter, Info } from "lucide-react";
import { mockMaskingRules } from "@/data/mockData";
import { useState } from "react";
import { toast } from "sonner";
import { MaskingRule } from "@/types/pipeline";

const Masking = () => {
  const [rules, setRules] = useState<MaskingRule[]>(mockMaskingRules);

  const handleToggleRule = (id: string, isActive: boolean) => {
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
              <Filter className="h-4 w-4" />
              Filtrar
            </Button>
            <p className="text-sm text-muted-foreground">
              {activeCount} de {rules.length} reglas activas
            </p>
          </div>
          <Button>
            <Plus className="h-4 w-4" />
            Nueva Regla
          </Button>
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
