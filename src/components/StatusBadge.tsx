import { cn } from "@/lib/utils";
import { PipelineStatus } from "@/types/pipeline";
import { CheckCircle2, XCircle, AlertTriangle, Clock, Loader2 } from "lucide-react";

interface StatusBadgeProps {
  status: PipelineStatus;
  showIcon?: boolean;
  size?: 'sm' | 'md' | 'lg';
}

const statusConfig: Record<PipelineStatus, { label: string; className: string; icon: React.ElementType }> = {
  idle: {
    label: 'Inactivo',
    className: 'bg-muted text-muted-foreground',
    icon: Clock,
  },
  running: {
    label: 'Ejecutando',
    className: 'bg-primary/20 text-primary',
    icon: Loader2,
  },
  success: {
    label: 'Exitoso',
    className: 'bg-success/20 text-success',
    icon: CheckCircle2,
  },
  error: {
    label: 'Error',
    className: 'bg-destructive/20 text-destructive',
    icon: XCircle,
  },
  warning: {
    label: 'Advertencia',
    className: 'bg-warning/20 text-warning',
    icon: AlertTriangle,
  },
};

const sizeClasses = {
  sm: 'text-xs px-2 py-0.5',
  md: 'text-sm px-2.5 py-1',
  lg: 'text-base px-3 py-1.5',
};

export function StatusBadge({ status, showIcon = true, size = 'md' }: StatusBadgeProps) {
  const config = statusConfig[status];
  const Icon = config.icon;

  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-full font-medium',
        config.className,
        sizeClasses[size]
      )}
    >
      {showIcon && (
        <Icon 
          className={cn(
            'shrink-0',
            size === 'sm' && 'h-3 w-3',
            size === 'md' && 'h-4 w-4',
            size === 'lg' && 'h-5 w-5',
            status === 'running' && 'animate-spin'
          )} 
        />
      )}
      {config.label}
    </span>
  );
}
