import { MaskingRule } from "@/types/pipeline";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { 
  Mail, 
  Phone, 
  User, 
  MapPin, 
  CreditCard, 
  KeyRound, 
  Code,
  Table,
  Columns
} from "lucide-react";
import { cn } from "@/lib/utils";

interface MaskingRuleCardProps {
  rule: MaskingRule;
  onToggle?: (id: string, isActive: boolean) => void;
}

const typeIcons: Record<MaskingRule['type'], React.ElementType> = {
  email: Mail,
  phone: Phone,
  name: User,
  address: MapPin,
  ssn: KeyRound,
  credit_card: CreditCard,
  custom: Code,
};

const typeLabels: Record<MaskingRule['type'], string> = {
  email: 'Email',
  phone: 'Teléfono',
  name: 'Nombre',
  address: 'Dirección',
  ssn: 'SSN',
  credit_card: 'Tarjeta',
  custom: 'Custom',
};

export function MaskingRuleCard({ rule, onToggle }: MaskingRuleCardProps) {
  const Icon = typeIcons[rule.type];

  return (
    <Card className={cn(
      "card-gradient border-border/50 transition-all duration-300",
      rule.isActive ? "border-primary/30" : "opacity-60"
    )}>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className={cn(
              "p-2 rounded-lg",
              rule.isActive ? "bg-primary/20 text-primary" : "bg-muted text-muted-foreground"
            )}>
              <Icon className="h-5 w-5" />
            </div>
            <div>
              <CardTitle className="text-base font-semibold text-foreground">
                {rule.name}
              </CardTitle>
              <Badge variant="outline" className="mt-1 text-xs">
                {typeLabels[rule.type]}
              </Badge>
            </div>
          </div>
          <Switch
            checked={rule.isActive}
            onCheckedChange={(checked) => onToggle?.(rule.id, checked)}
          />
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Replacement Preview */}
        <div className="p-2 rounded-md bg-muted/50 border border-border/50">
          <p className="text-xs text-muted-foreground mb-1">Valor enmascarado:</p>
          <code className="text-sm font-mono text-primary">{rule.replacement}</code>
        </div>

        {/* Tables */}
        <div>
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground mb-1.5">
            <Table className="h-3 w-3" />
            Tablas aplicadas:
          </div>
          <div className="flex flex-wrap gap-1">
            {rule.tables.map((table) => (
              <Badge key={table} variant="secondary" className="text-xs font-mono">
                {table}
              </Badge>
            ))}
          </div>
        </div>

        {/* Columns */}
        <div>
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground mb-1.5">
            <Columns className="h-3 w-3" />
            Columnas:
          </div>
          <div className="flex flex-wrap gap-1">
            {rule.columns.map((column) => (
              <Badge key={column} variant="outline" className="text-xs font-mono">
                {column}
              </Badge>
            ))}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
