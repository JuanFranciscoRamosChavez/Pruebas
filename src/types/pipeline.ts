export type PipelineStatus = 'idle' | 'running' | 'success' | 'error' | 'warning';

export interface Pipeline {
  id: string;
  name: string;
  description: string;
  sourceDb: string;
  targetDb: string;
  status: PipelineStatus;
  lastRun?: string;
  nextRun?: string;
  tablesCount: number;
  recordsProcessed?: number;
  maskingRulesCount: number;
}

export interface MaskingRule {
  id: string;
  name: string;
  type: 'email' | 'phone' | 'name' | 'address' | 'ssn' | 'credit_card' | 'custom';
  pattern?: string;
  replacement: string;
  tables: string[];
  columns: string[];
  isActive: boolean;
}

export interface ExecutionLog {
  id: string;
  pipelineId: string;
  pipelineName: string;
  status: PipelineStatus;
  startTime: string;
  endTime?: string;
  duration?: number;
  recordsExtracted: number;
  recordsMasked: number;
  recordsLoaded: number;
  errors: ExecutionError[];
}

export interface ExecutionError {
  id: string;
  timestamp: string;
  severity: 'error' | 'warning';
  message: string;
  table?: string;
  details?: string;
}

export interface DatabaseConnection {
  id: string;
  name: string;
  type: 'postgresql' | 'mysql' | 'sqlserver' | 'oracle';
  host: string;
  port: number;
  database: string;
  isProduction: boolean;
  status: 'connected' | 'disconnected' | 'error';
}
