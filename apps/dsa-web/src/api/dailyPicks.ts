import apiClient from './index';

export interface DailyPickSummaryItem {
  id: number;
  source: string;
  strategyVersion: string;
  generatedAt: string;
  pickCount: number;
  topNames: string[];
}

export interface DailyPickListResponse {
  total: number;
  page: number;
  limit: number;
  items: DailyPickSummaryItem[];
}

export interface DailyPickRecommendation {
  rank: number;
  stockCode?: string | null;
  stockName: string;
  sectorName?: string | null;
  sectorChangePct?: number | null;
  score?: number | null;
  recommendReason?: string;
  operationAdvice?: string;
  riskWarning?: string;
  quote?: Record<string, unknown>;
}

export interface DailyPickDetail {
  id: number;
  source: string;
  strategyVersion: string;
  generatedAt: string;
  pickCount: number;
  marketNews: Array<Record<string, unknown>>;
  sectorRankings: Record<string, unknown>;
  recommendations: DailyPickRecommendation[];
  payload: Record<string, unknown>;
}

export const dailyPicksApi = {
  async generate(topK = 5): Promise<Record<string, unknown>> {
    const response = await apiClient.post('/api/v1/daily-picks/generate', null, {
      params: { top_k: topK },
      timeout: 180000,
    });
    return response.data as Record<string, unknown>;
  },

  async getList(page = 1, limit = 20): Promise<DailyPickListResponse> {
    const response = await apiClient.get('/api/v1/daily-picks', {
      params: { page, limit },
    });
    return response.data as DailyPickListResponse;
  },

  async getDetail(recordId: number): Promise<DailyPickDetail> {
    const response = await apiClient.get(`/api/v1/daily-picks/${recordId}`);
    return response.data as DailyPickDetail;
  },
};
