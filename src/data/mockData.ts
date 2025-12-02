import { Pipeline, MaskingRule, ExecutionLog, DatabaseConnection } from '@/types/pipeline';

// 1. PIPELINES: Solo definimos el real que construimos en Python
export const mockPipelines: Pipeline[] = [
  {
    id: '1',
    name: 'Migración Mensual: Producción -> QA',
    description: 'Extracción de Clientes y Órdenes, aplicación de hashing y carga en Supabase QA.',
    sourceDb: 'supabase-prod',
    targetDb: 'supabase-qa',
    status: 'idle', // Estado inicial: Inactivo
    lastRun: undefined, // Aún no ha corrido en esta sesión
    tablesCount: 2,     // Clientes y Órdenes
    maskingRulesCount: 4, // Nombre, Email, Teléfono, Dirección
  }
];

// 2. REGLAS: Las que definimos en el config.yaml
export const mockMaskingRules: MaskingRule[] = [
  {
    id: '1',
    name: 'Email Hash Determinístico',
    type: 'email',
    replacement: 'hash@anon.com',
    tables: ['clientes'],
    columns: ['email'],
    isActive: true,
  },
  {
    id: '2',
    name: 'Preservar Formato Teléfono',
    type: 'phone',
    replacement: '+52 (XXX) XXX-XXXX',
    tables: ['clientes'],
    columns: ['telefono'],
    isActive: true,
  },
  {
    id: '3',
    name: 'Nombre Sintético (Faker)',
    type: 'name',
    replacement: 'Juan Perez',
    tables: ['clientes'],
    columns: ['nombre'],
    isActive: true,
  },
  {
    id: '4',
    name: 'Redacción de Dirección',
    type: 'address',
    replacement: '****',
    tables: ['clientes'],
    columns: ['direccion'],
    isActive: true,
  },
];

// 3. LOGS: Inicialmente vacío (se llenará desde la BD real)
export const mockExecutionLogs: ExecutionLog[] = [];

// 4. CONEXIONES: Tus 2 proyectos de Supabase
export const mockDatabaseConnections: DatabaseConnection[] = [
  {
    id: 'prod-supabase',
    name: 'Supabase Producción',
    type: 'postgresql',
    host: 'aws-0-us-west-1.pooler.supabase.com',
    port: 5432,
    database: 'postgres',
    isProduction: true,
    status: 'connected',
  },
  {
    id: 'qa-supabase',
    name: 'Supabase QA',
    type: 'postgresql',
    host: 'aws-0-us-west-1.pooler.supabase.com',
    port: 5432,
    database: 'postgres',
    isProduction: false,
    status: 'connected',
  },
];