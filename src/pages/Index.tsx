import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { StatsCard } from "@/components/StatsCard";
import { Button } from "@/components/ui/button";
import { GitBranch, Shield, Database, CheckCircle2, ArrowRight } from "lucide-react";
import { Link } from "react-router-dom";
import { useEffect, useState } from "react";

const Index = () => {
  const [stats, setStats] = useState({ pipelines: 0, rules: 0, records: 0, success_rate: 0 });

  useEffect(() => {
    fetch('http://localhost:5000/api/dashboard')
      .then(res => res.json())
      .then(data => setStats(data))
      .catch(err => console.error("Error cargando stats:", err));
  }, []);

  return (
    <Layout>
      <Header title="Dashboard" description="Estado del sistema ETL en tiempo real" />
      <div className="p-6 space-y-8">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <StatsCard title="Pipelines Activos" value={stats.pipelines} description="Tablas configuradas" icon={GitBranch} variant="primary" />
          <StatsCard title="Reglas Activas" value={stats.rules} description="Columnas enmascaradas" icon={Shield} variant="success" />
          <StatsCard title="Registros Procesados" value={stats.records.toLocaleString()} description="Total histórico" icon={Database} />
          <StatsCard title="Tasa de Éxito" value={`${stats.success_rate}%`} description="Ejecuciones exitosas" icon={CheckCircle2} variant="success" />
        </div>
        
        <div className="flex justify-end">
           <Button variant="outline" asChild>
              <Link to="/pipelines">Ir a Pipelines <ArrowRight className="ml-2 h-4 w-4" /></Link>
           </Button>
        </div>
      </div>
    </Layout>
  );
};
export default Index;