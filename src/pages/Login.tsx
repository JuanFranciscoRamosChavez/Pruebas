import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Shield, Lock, LogIn, AlertCircle } from "lucide-react";
import { toast } from "sonner";

// Leemos las credenciales del entorno seguro (.env)
const ENV = import.meta.env;

const CREDENTIALS = {
  admin: [
    { user: ENV.VITE_ADMIN_1_USER, pass: ENV.VITE_ADMIN_1_PASS, name: ENV.VITE_ADMIN_1_NAME },
    { user: ENV.VITE_ADMIN_2_USER, pass: ENV.VITE_ADMIN_2_PASS, name: ENV.VITE_ADMIN_2_NAME },
    { user: ENV.VITE_ADMIN_3_USER, pass: ENV.VITE_ADMIN_3_PASS, name: ENV.VITE_ADMIN_3_NAME }
  ],
  operator: [
    { user: ENV.VITE_OP_1_USER, pass: ENV.VITE_OP_1_PASS, name: ENV.VITE_OP_1_NAME },
    { user: ENV.VITE_OP_2_USER, pass: ENV.VITE_OP_2_PASS, name: ENV.VITE_OP_2_NAME },
    { user: ENV.VITE_OP_3_USER, pass: ENV.VITE_OP_3_PASS, name: ENV.VITE_OP_3_NAME }
  ]
};

interface LoginProps {
  onLogin: (role: 'admin' | 'operator') => void;
}

const Login = ({ onLogin }: LoginProps) => {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  const handleLogin = (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    // 1. Buscar en Admins
    const isAdmin = CREDENTIALS.admin.find(u => u.user === username && u.pass === password);
    if (isAdmin) {
      toast.success(`¡Hola, ${isAdmin.name}!`, { description: "Sesión iniciada como DESARROLLADOR" });
      onLogin('admin');
      return;
    }

    // 2. Buscar en Operadores
    const isOp = CREDENTIALS.operator.find(u => u.user === username && u.pass === password);
    if (isOp) {
      toast.success(`¡Hola, ${isOp.name}!`, { description: "Sesión iniciada como OPERADOR" });
      onLogin('operator');
      return;
    }

    // 3. Fallo
    setError("Credenciales inválidas. Intente de nuevo.");
    toast.error("Acceso Denegado");
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-background p-4 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-slate-900 via-background to-background">
      <Card className="w-full max-w-md border-border/50 shadow-2xl">
        <CardHeader className="text-center space-y-2">
          <div className="mx-auto bg-primary/10 p-4 rounded-full w-fit mb-2 border border-primary/20">
            <Shield className="h-10 w-10 text-primary animate-pulse" />
          </div>
          <CardTitle className="text-3xl font-bold tracking-tight">DataMask ETL</CardTitle>
          <CardDescription>Ingreso Seguro (RBAC)</CardDescription>
        </CardHeader>
        
        <form onSubmit={handleLogin}>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="user">Usuario</Label>
              <div className="relative">
                <Input 
                  id="user" 
                  placeholder="Ingrese su usuario..." 
                  className="pl-10"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                />
                <Shield className="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
              </div>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="pass">Contraseña</Label>
              <div className="relative">
                <Input 
                  id="pass" 
                  type="password" 
                  placeholder="••••••••" 
                  className="pl-10"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                />
                <Lock className="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
              </div>
            </div>

            {error && (
              <div className="text-sm text-red-400 flex items-center gap-2 bg-red-950/30 p-3 rounded border border-red-900/50">
                <AlertCircle className="h-4 w-4" />
                {error}
              </div>
            )}
          </CardContent>
          
          <CardFooter className="flex flex-col gap-4">
            <Button type="submit" className="w-full h-11 text-base">
              <LogIn className="mr-2 h-4 w-4" /> Iniciar Sesión
            </Button>
            
            <div className="text-xs text-muted-foreground text-center">
              <p>Sistema protegido. Acceso monitoreado.</p>
            </div>
          </CardFooter>
        </form>
      </Card>
    </div>
  );
};

export default Login;