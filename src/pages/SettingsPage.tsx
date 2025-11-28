import { Layout } from "@/components/Layout";
import { Header } from "@/components/Header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { 
  Bell, 
  Shield, 
  Clock, 
  Mail,
  Save,
  RotateCcw
} from "lucide-react";
import { toast } from "sonner";

const SettingsPage = () => {
  const handleSave = () => {
    toast.success("Configuración guardada correctamente");
  };

  return (
    <Layout>
      <Header 
        title="Configuración" 
        description="Ajusta el comportamiento del sistema y las preferencias"
      />
      
      <div className="p-6 space-y-6 max-w-3xl">
        {/* Notifications */}
        <Card className="card-gradient border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Bell className="h-5 w-5 text-primary" />
              Notificaciones
            </CardTitle>
            <CardDescription>
              Configura cómo recibir alertas sobre tus pipelines
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <Label>Notificaciones por email</Label>
                <p className="text-sm text-muted-foreground">
                  Recibe alertas cuando un pipeline falle
                </p>
              </div>
              <Switch defaultChecked />
            </div>
            <div className="flex items-center justify-between">
              <div>
                <Label>Resumen diario</Label>
                <p className="text-sm text-muted-foreground">
                  Recibe un resumen diario de todas las ejecuciones
                </p>
              </div>
              <Switch />
            </div>
            <div className="pt-2">
              <Label htmlFor="email">Email de notificaciones</Label>
              <Input 
                id="email" 
                type="email" 
                placeholder="admin@company.com" 
                className="mt-1.5"
              />
            </div>
          </CardContent>
        </Card>

        {/* Security */}
        <Card className="card-gradient border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Shield className="h-5 w-5 text-primary" />
              Seguridad
            </CardTitle>
            <CardDescription>
              Configuraciones de seguridad y auditoría
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <Label>Registro de auditoría detallado</Label>
                <p className="text-sm text-muted-foreground">
                  Guarda registros detallados de cada operación
                </p>
              </div>
              <Switch defaultChecked />
            </div>
            <div className="flex items-center justify-between">
              <div>
                <Label>Encriptación de logs</Label>
                <p className="text-sm text-muted-foreground">
                  Encripta los logs de ejecución en reposo
                </p>
              </div>
              <Switch defaultChecked />
            </div>
            <div className="pt-2">
              <Label htmlFor="retention">Retención de logs (días)</Label>
              <Input 
                id="retention" 
                type="number" 
                defaultValue={90} 
                className="mt-1.5 w-32"
              />
            </div>
          </CardContent>
        </Card>

        {/* Scheduling */}
        <Card className="card-gradient border-border/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="h-5 w-5 text-primary" />
              Programación
            </CardTitle>
            <CardDescription>
              Configuración de ejecuciones automáticas
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <Label>Ejecuciones programadas</Label>
                <p className="text-sm text-muted-foreground">
                  Permite ejecutar pipelines automáticamente
                </p>
              </div>
              <Switch defaultChecked />
            </div>
            <div className="flex items-center justify-between">
              <div>
                <Label>Re-intentos automáticos</Label>
                <p className="text-sm text-muted-foreground">
                  Reintentar ejecuciones fallidas automáticamente
                </p>
              </div>
              <Switch />
            </div>
            <div className="pt-2">
              <Label htmlFor="timeout">Timeout de ejecución (minutos)</Label>
              <Input 
                id="timeout" 
                type="number" 
                defaultValue={30} 
                className="mt-1.5 w-32"
              />
            </div>
          </CardContent>
        </Card>

        {/* Actions */}
        <div className="flex items-center gap-3 pt-4">
          <Button onClick={handleSave}>
            <Save className="h-4 w-4" />
            Guardar Cambios
          </Button>
          <Button variant="outline">
            <RotateCcw className="h-4 w-4" />
            Restaurar Valores
          </Button>
        </div>
      </div>
    </Layout>
  );
};

export default SettingsPage;
