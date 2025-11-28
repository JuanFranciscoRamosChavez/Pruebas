import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { 
  Plus, 
  Database, 
  CheckCircle2, 
  XCircle, 
  AlertTriangle,
  Settings,
  Trash2,
  RefreshCw
} from "lucide-react";
import { mockDatabaseConnections } from "@/data/mockData";
import { cn } from "@/lib/utils";
import { toast } from "sonner";

const dbTypeLabels: Record<string, string> = {
  postgresql: 'PostgreSQL',
  mysql: 'MySQL',
  sqlserver: 'SQL Server',
  oracle: 'Oracle',
};

const statusConfig = {
  connected: {
    label: 'Conectado',
    icon: CheckCircle2,
    className: 'text-success',
  },
  disconnected: {
    label: 'Desconectado',
    icon: AlertTriangle,
    className: 'text-warning',
  },
  error: {
    label: 'Error',
    icon: XCircle,
    className: 'text-destructive',
  },
};

const Connections = () => {
  const handleTestConnection = (name: string) => {
    toast.info(`Probando conexión a ${name}...`);
  };

  const productionDbs = mockDatabaseConnections.filter(db => db.isProduction);
  const qaDbs = mockDatabaseConnections.filter(db => !db.isProduction);

  const renderConnectionCard = (db: typeof mockDatabaseConnections[0]) => {
    const status = statusConfig[db.status];
    const StatusIcon = status.icon;

    return (
      <Card key={db.id} className="card-gradient border-border/50 hover:border-primary/30 transition-all">
        <CardHeader className="pb-3">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3">
              <div className={cn(
                "p-2 rounded-lg",
                db.isProduction ? "bg-destructive/20 text-destructive" : "bg-success/20 text-success"
              )}>
                <Database className="h-5 w-5" />
              </div>
              <div>
                <CardTitle className="text-base font-semibold">{db.name}</CardTitle>
                <Badge variant="outline" className="mt-1 text-xs font-mono">
                  {dbTypeLabels[db.type]}
                </Badge>
              </div>
            </div>
            <div className={cn("flex items-center gap-1.5 text-sm", status.className)}>
              <StatusIcon className="h-4 w-4" />
              <span>{status.label}</span>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Connection Details */}
          <div className="p-3 rounded-md bg-muted/50 border border-border/50 font-mono text-sm">
            <p className="text-muted-foreground">
              <span className="text-foreground">{db.host}</span>:{db.port}
            </p>
            <p className="text-muted-foreground mt-1">
              Base de datos: <span className="text-foreground">{db.database}</span>
            </p>
          </div>

          {/* Actions */}
          <div className="flex gap-2">
            <Button 
              variant="outline" 
              size="sm" 
              className="flex-1"
              onClick={() => handleTestConnection(db.name)}
            >
              <RefreshCw className="h-4 w-4" />
              Probar
            </Button>
            <Button variant="ghost" size="sm">
              <Settings className="h-4 w-4" />
            </Button>
            <Button variant="ghost" size="sm" className="text-destructive hover:text-destructive">
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>
        </CardContent>
      </Card>
    );
  };

  return (
    <Layout>
      <Header 
        title="Conexiones de Base de Datos" 
        description="Gestiona las conexiones a bases de datos de producción y QA"
      />
      
      <div className="p-6 space-y-8">
        {/* Add Connection Button */}
        <div className="flex justify-end">
          <Button>
            <Plus className="h-4 w-4" />
            Nueva Conexión
          </Button>
        </div>

        {/* Production Databases */}
        <section>
          <div className="flex items-center gap-2 mb-4">
            <div className="h-3 w-3 rounded-full bg-destructive" />
            <h2 className="text-lg font-semibold text-foreground">Bases de Datos de Producción</h2>
            <Badge variant="outline" className="ml-2">{productionDbs.length}</Badge>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {productionDbs.map(renderConnectionCard)}
          </div>
        </section>

        {/* QA Databases */}
        <section>
          <div className="flex items-center gap-2 mb-4">
            <div className="h-3 w-3 rounded-full bg-success" />
            <h2 className="text-lg font-semibold text-foreground">Bases de Datos de QA</h2>
            <Badge variant="outline" className="ml-2">{qaDbs.length}</Badge>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {qaDbs.map(renderConnectionCard)}
          </div>
        </section>
      </div>
    </Layout>
  );
};

export default Connections;
