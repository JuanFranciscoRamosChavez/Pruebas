import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Database, CheckCircle2, XCircle, AlertTriangle, RefreshCw } from "lucide-react";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

const Connections = () => {
  const [connections, setConnections] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchConnections = () => {
    setLoading(true);
    fetch('http://localhost:5000/api/connections')
      .then(res => res.json())
      .then(data => {
        setConnections(data);
        toast.success("Estados de conexión actualizados");
      })
      .finally(() => setLoading(false));
  };

  useEffect(() => { fetchConnections(); }, []);

  return (
    <Layout>
      <Header title="Conexiones" description="Estado de enlace con Supabase" />
      <div className="p-6 space-y-6">
        <div className="flex justify-end"><Button onClick={fetchConnections} disabled={loading}><RefreshCw className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`}/> Probar Conexión</Button></div>
        <div className="grid gap-4 md:grid-cols-2">
          {connections.map((db) => (
            <Card key={db.id} className="card-gradient border-border/50">
              <CardHeader className="pb-2">
                <div className="flex justify-between">
                  <div className="flex items-center gap-3">
                     <div className={`p-2 rounded-lg ${db.isProduction ? 'bg-red-900/20 text-red-500' : 'bg-green-900/20 text-green-500'}`}><Database className="h-5 w-5" /></div>
                     <div><CardTitle className="text-base">{db.name}</CardTitle><p className="text-xs text-muted-foreground">{db.host}</p></div>
                  </div>
                  <Badge variant="outline" className={db.status === 'connected' ? 'text-green-500 border-green-500' : 'text-red-500 border-red-500'}>
                    {db.status === 'connected' ? <CheckCircle2 className="h-3 w-3 mr-1" /> : <XCircle className="h-3 w-3 mr-1" />} {db.status}
                  </Badge>
                </div>
              </CardHeader>
            </Card>
          ))}
        </div>
      </div>
    </Layout>
  );
};
export default Connections;