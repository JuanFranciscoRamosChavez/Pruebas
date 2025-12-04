import { Button } from "@/components/ui/button";
import { User, LogOut, ShieldCheck, Code } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { toast } from "sonner";

interface HeaderProps {
  title: string;
  description?: string;
}

export function Header({ title, description }: HeaderProps) {
  
  // Recuperar info del usuario
  const userName = localStorage.getItem("userName") || "Usuario";
  const rawRole = localStorage.getItem("userRole");
  // Mapear rol a nombre amigable
  const userRoleDisplay = rawRole === 'admin' ? 'DBA' : 'Desarrollador/Tester';
  const RoleIcon = rawRole === 'admin' ? ShieldCheck : Code;

  const handleLogout = () => {
    toast.info("Cerrando sesión...");
    localStorage.removeItem("userRole");
    localStorage.removeItem("userName");
    // Recargar para volver al Login
    setTimeout(() => window.location.href = "/", 500);
  };

  return (
    <header className="sticky top-0 z-10 bg-background/80 backdrop-blur-sm border-b border-border px-6 py-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-foreground">{title}</h1>
          {description && (
            <p className="text-sm text-muted-foreground mt-1">{description}</p>
          )}
        </div>

        <div className="flex items-center gap-4">
          {/* User Dropdown con Info y Logout */}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="secondary" size="icon" className="rounded-full border border-border">
                <User className="h-5 w-5" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-56">
              <DropdownMenuLabel>
                <div className="flex flex-col space-y-1">
                  <p className="text-sm font-medium leading-none">{userName}</p>
                  <div className="flex items-center gap-1.5 mt-1">
                    <RoleIcon className="h-3 w-3 text-muted-foreground" />
                    <p className="text-xs text-muted-foreground">{userRoleDisplay}</p>
                  </div>
                </div>
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={handleLogout} className="text-red-500 focus:text-red-500 cursor-pointer">
                <LogOut className="mr-2 h-4 w-4" />
                <span>Cerrar Sesión</span>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    </header>
  );
}