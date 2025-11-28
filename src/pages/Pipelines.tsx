import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { PipelineCard } from "@/components/PipelineCard";
import { Button } from "@/components/ui/button";
import { Plus, Filter } from "lucide-react";
import { mockPipelines } from "@/data/mockData";
import { toast } from "sonner";

const Pipelines = () => {
  const handleRunPipeline = (id: string) => {
    const pipeline = mockPipelines.find(p => p.id === id);
    toast.success(`Pipeline "${pipeline?.name}" iniciado`, {
      description: "La ejecución comenzará en unos segundos.",
    });
  };

  const handleConfigurePipeline = (id: string) => {
    toast.info("Abriendo configuración del pipeline...");
  };

  return (
    <Layout>
      <Header 
        title="Pipelines" 
        description="Gestiona y ejecuta tus pipelines de extracción y carga de datos"
      />
      
      <div className="p-6 space-y-6">
        {/* Actions Bar */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm">
              <Filter className="h-4 w-4" />
              Filtrar
            </Button>
          </div>
          <Button>
            <Plus className="h-4 w-4" />
            Nuevo Pipeline
          </Button>
        </div>

        {/* Pipelines Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {mockPipelines.map((pipeline, index) => (
            <div 
              key={pipeline.id} 
              className="animate-slide-up"
              style={{ animationDelay: `${index * 100}ms` }}
            >
              <PipelineCard 
                pipeline={pipeline} 
                onRun={handleRunPipeline}
                onConfigure={handleConfigurePipeline}
              />
            </div>
          ))}
        </div>
      </div>
    </Layout>
  );
};

export default Pipelines;
