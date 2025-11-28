import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { StatsCard } from "@/components/StatsCard";
import { PipelineCard } from "@/components/PipelineCard";
import { ExecutionLogRow } from "@/components/ExecutionLogRow";
import { Button } from "@/components/ui/button";
import { 
  GitBranch, 
  Shield, 
  Database, 
  CheckCircle2, 
  Plus,
  ArrowRight
} from "lucide-react";
import { mockPipelines, mockExecutionLogs } from "@/data/mockData";
import { Link } from "react-router-dom";
import { toast } from "sonner";

const Index = () => {
  const stats = {
    totalPipelines: mockPipelines.length,
    activeMaskingRules: 5,
    recordsProcessed: mockPipelines.reduce((acc, p) => acc + (p.recordsProcessed || 0), 0),
    successRate: 85,
  };

  const handleRunPipeline = (id: string) => {
    const pipeline = mockPipelines.find(p => p.id === id);
    toast.success(`Pipeline "${pipeline?.name}" iniciado`, {
      description: "La ejecución comenzará en unos segundos.",
    });
  };

  const recentLogs = mockExecutionLogs.slice(0, 3);
  const topPipelines = mockPipelines.slice(0, 3);

  return (
    <Layout>
      <Header 
        title="Dashboard" 
        description="Monitorea y gestiona tus pipelines de datos"
      />
      
      <div className="p-6 space-y-8">
        {/* Stats Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <StatsCard
            title="Pipelines Activos"
            value={stats.totalPipelines}
            description="Total de pipelines configurados"
            icon={GitBranch}
            variant="primary"
          />
          <StatsCard
            title="Reglas de Enmascaramiento"
            value={stats.activeMaskingRules}
            description="Reglas activas de anonimización"
            icon={Shield}
            variant="success"
          />
          <StatsCard
            title="Registros Procesados"
            value={stats.recordsProcessed.toLocaleString()}
            description="Total en las últimas 24h"
            icon={Database}
            trend={{ value: 12, isPositive: true }}
          />
          <StatsCard
            title="Tasa de Éxito"
            value={`${stats.successRate}%`}
            description="Ejecuciones exitosas"
            icon={CheckCircle2}
            variant="success"
            trend={{ value: 3, isPositive: true }}
          />
        </div>

        {/* Pipelines Section */}
        <section>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-semibold text-foreground">Pipelines Recientes</h2>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" asChild>
                <Link to="/pipelines">
                  Ver todos
                  <ArrowRight className="h-4 w-4" />
                </Link>
              </Button>
              <Button size="sm">
                <Plus className="h-4 w-4" />
                Nuevo Pipeline
              </Button>
            </div>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {topPipelines.map((pipeline, index) => (
              <div 
                key={pipeline.id} 
                className="animate-slide-up"
                style={{ animationDelay: `${index * 100}ms` }}
              >
                <PipelineCard 
                  pipeline={pipeline} 
                  onRun={handleRunPipeline}
                />
              </div>
            ))}
          </div>
        </section>

        {/* Execution History Section */}
        <section>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-semibold text-foreground">Historial de Ejecuciones</h2>
            <Button variant="outline" size="sm" asChild>
              <Link to="/history">
                Ver historial completo
                <ArrowRight className="h-4 w-4" />
              </Link>
            </Button>
          </div>
          
          <div className="space-y-3">
            {recentLogs.map((log, index) => (
              <div 
                key={log.id} 
                className="animate-slide-up"
                style={{ animationDelay: `${index * 100}ms` }}
              >
                <ExecutionLogRow log={log} />
              </div>
            ))}
          </div>
        </section>
      </div>
    </Layout>
  );
};

export default Index;
