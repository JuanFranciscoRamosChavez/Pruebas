import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Shield, User, Lock } from "lucide-react";
import { toast } from "sonner";

interface LoginProps {
  onLogin: (role: 'admin' | 'operator') => void;
}

const Login = ({ onLogin }: LoginProps) => {
  const handleLogin = (role: 'admin' | 'operator') => {
    toast.success(`Bienvenido, ${role === 'admin' ? 'Desarrollador' : 'Operador'}`);
    onLogin(role);
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-background p-4">
      <Card className="w-full max-w-md card-gradient border-border/50">
        <CardHeader className="text-center">
          <div className="mx-auto bg-primary/20 p-3 rounded-full w-fit mb-4">
            <Lock className="h-8 w-8 text-primary" />
          </div>
          <CardTitle className="text-2xl font-bold">Acceso Seguro DataMask</CardTitle>
          <CardDescription>Selecciona tu perfil para ingresar al sistema</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <Button 
            variant="outline" 
            className="w-full h-16 text-lg justify-start px-6 hover:border-primary/50 hover:bg-primary/10"
            onClick={() => handleLogin('admin')}
          >
            <Shield className="h-6 w-6 mr-4 text-purple-500" />
            <div className="text-left">
              <div className="font-bold">Desarrollador</div>
              <div className="text-xs text-muted-foreground">Acceso total (Config + Ejecución)</div>
            </div>
          </Button>

          <Button 
            variant="outline" 
            className="w-full h-16 text-lg justify-start px-6 hover:border-primary/50 hover:bg-primary/10"
            onClick={() => handleLogin('operator')}
          >
            <User className="h-6 w-6 mr-4 text-blue-500" />
            <div className="text-left">
              <div className="font-bold">Operador</div>
              <div className="text-xs text-muted-foreground">Acceso limitado (Solo Ejecución)</div>
            </div>
          </Button>
        </CardContent>
      </Card>
    </div>
  );
};

export default Login;