import { cn } from "@/lib/utils";
import { NavLink } from "@/components/NavLink";
import { 
  LayoutDashboard, 
  GitBranch, 
  Shield, 
  History, 
  Database, 
  Settings,
  Terminal,
  LogOut 
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

const navItems = [
  { to: "/", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/pipelines", icon: GitBranch, label: "Pipelines" },
  { to: "/masking", icon: Shield, label: "Enmascaramiento" },
  { to: "/history", icon: History, label: "Historial" },
  { to: "/connections", icon: Database, label: "Conexiones" },
  { to: "/settings", icon: Settings, label: "Configuración" },
];

export function Sidebar() {
  
  const handleLogout = () => {
    toast.info("Cerrando sesión...");
    localStorage.removeItem("userRole");
    // Recargar para volver al Login
    setTimeout(() => window.location.href = "/", 500);
  };

  return (
    <aside className="w-64 min-h-screen bg-sidebar border-r border-sidebar-border flex flex-col">
      <div className="p-6 border-b border-sidebar-border">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-primary/20">
            <Terminal className="h-6 w-6 text-primary" />
          </div>
          <div>
            <h1 className="font-bold text-lg text-sidebar-foreground">DataMask</h1>
            <p className="text-xs text-muted-foreground">ETL Pipeline Manager</p>
          </div>
        </div>
      </div>

      <nav className="flex-1 p-4 space-y-1">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            className={cn(
              "flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium",
              "text-sidebar-foreground/70 hover:text-sidebar-foreground",
              "hover:bg-sidebar-accent transition-all duration-200"
            )}
            activeClassName="bg-sidebar-accent text-sidebar-foreground"
          >
            <item.icon className="h-5 w-5" />
            {item.label}
          </NavLink>
        ))}
      </nav>

      <div className="p-4 border-t border-sidebar-border space-y-4">
        <Button 
          variant="destructive" 
          className="w-full justify-start pl-4 bg-red-900/20 hover:bg-red-900/40 text-red-400 border border-red-900/50"
          onClick={handleLogout}
        >
          <LogOut className="h-4 w-4 mr-2" />
          Cerrar Sesión
        </Button>
      </div>
    </aside>
  );
}