import apiClient from './index';
import { toCamelCase } from './utils';

export interface DailyPickSummaryItem {
  id: number;
  source: string;
  strategyVersion: string;
  generatedAt: string;
  pickCount: number;
  outputCount?: number;
  candidateCount?: number;
  runStatus?: string;
  degraded?: boolean;
  confidence?: string;
  generationLayer?: string;
  errorSummary?: string[];
  topNames: string[];
}

export interface DailyPickRecommendation {
  rank: number;
  programRank?: number | null;
  finalRank?: number | null;
  stockCode?: string | null;
  stockName: string;
  sectorName?: string | null;
  sectorChangePct?: number | null;
  score?: number | null;
  scoreBreakdown?: Record<string, number | string> | null;
  reasonTags?: string[] | null;
  riskTags?: string[] | null;
  entryHint?: string | null;
  stopLossHint?: string | null;
  recommendReason?: string;
  operationAdvice?: string;
  riskWarning?: string;
  confidence?: string;
  riskNote?: string;
  newsConnection?: string;
  signalBreakdown?: {
    technical?: string;
    sentiment?: string;
    capital?: string;
    sector?: string;
  };
  relatedNews?: Array<Record<string, unknown>>;
  quote?: Record<string, unknown>;
}

export interface DailyPickDetail {
  id: number;
  source: string;
  strategyVersion: string;
  generatedAt: string;
  pickCount: number;
  runStatus?: string;
  degraded?: boolean;
  startedAt?: string | null;
  finishedAt?: string | null;
  durationMs?: number | null;
  candidateCount?: number | null;
  outputCount?: number | null;
  confidence?: string | null;
  riskNote?: string | null;
  generationLayer?: string | null;
  generationNote?: string | null;
  errorSummary?: string[];
  sourceSummary?: Record<string, unknown>;
  usedSources?: string[];
  failedSources?: string[];
  marketNews: Array<Record<string, unknown>>;
  sectorRankings: Record<string, unknown>;
  recommendations: DailyPickRecommendation[];
  payload: Record<string, unknown>;
}

export interface DailyPickListResponse {
  total: number;
  page: number;
  limit: number;
  items: DailyPickSummaryItem[];
}

export const dailyPicksApi = {
  async generate(topK = 5): Promise<Record<string, unknown>> {
    const response = await apiClient.post('/api/v1/daily-picks/generate', null, {
      params: { top_k: topK },
      timeout: 600000,
    });
    return toCamelCase<Record<string, unknown>>(response.data);
  },

  async getList(page = 1, limit = 20): Promise<DailyPickListResponse> {
    const response = await apiClient.get('/api/v1/daily-picks', {
      params: { page, limit },
    });
    return toCamelCase<DailyPickListResponse>(response.data);
  },

  async getDetail(recordId: number): Promise<DailyPickDetail> {
    const response = await apiClient.get(`/api/v1/daily-picks/${recordId}`);
    return toCamelCase<DailyPickDetail>(response.data);
  },

  async deleteRun(recordId: number): Promise<void> {
    await apiClient.delete(`/api/v1/daily-picks/${recordId}`);
  },
};
