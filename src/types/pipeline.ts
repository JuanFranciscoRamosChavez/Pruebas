export type PipelineStatus = 'idle' | 'running' | 'success' | 'error' | 'warning';

export interface Pipeline {
  id: string;
  name: string;
  description?: string;
  sourceDb: string;
  targetDb: string;
  status: 'idle' | 'running' | 'success' | 'error';
  lastRun?: string;
  nextRun?: string;
  tablesCount: number;
  maskingRulesCount: number;
  recordsProcessed?: number;
  isActive?: boolean;
}

export interface ExecutionLog {
  id: string;
  pipelineId: string;
  pipelineName: string;
  status: 'success' | 'error' | 'running';
  startTime: string;
  endTime?: string;
  duration?: number;
  recordsProcessed: number; 
  recordsExtracted?: number;
  recordsMasked?: number;
  recordsLoaded?: number;
  errors?: {
    id: string;
    message: string;
    timestamp: string;
    severity: 'warning' | 'error';
  }[];
}

export interface MaskingRule {
  id: string;
  name: string;
  table: string;
  column: string;
  type: string;
  isActive: boolean;
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
