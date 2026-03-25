import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import DailyPicksPage from '../DailyPicksPage';

const { getListMock, getDetailMock, generateMock } = vi.hoisted(() => ({
  getListMock: vi.fn(),
  getDetailMock: vi.fn(),
  generateMock: vi.fn(),
}));

vi.mock('../../api/dailyPicks', () => ({
  dailyPicksApi: {
    getList: (...args: unknown[]) => getListMock(...args),
    getDetail: (...args: unknown[]) => getDetailMock(...args),
    generate: (...args: unknown[]) => generateMock(...args),
  },
}));

vi.mock('../../api/error', () => ({
  getParsedApiError: (error: unknown) => ({ message: error instanceof Error ? error.message : String(error) }),
}));

vi.mock('../../components/common/AppPage', () => ({
  AppPage: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));

vi.mock('../../components/common', () => ({
  ApiErrorAlert: ({ error }: { error: { message?: string } }) => <div>{error.message}</div>,
  Badge: ({ children }: { children: React.ReactNode }) => <span>{children}</span>,
  Button: ({
    children,
    onClick,
    disabled,
  }: {
    children: React.ReactNode;
    onClick?: () => void;
    disabled?: boolean;
  }) => (
    <button type="button" onClick={onClick} disabled={disabled}>
      {children}
    </button>
  ),
  Card: ({
    title,
    subtitle,
    children,
  }: {
    title?: string;
    subtitle?: string;
    children: React.ReactNode;
  }) => (
    <section>
      {title ? <h2>{title}</h2> : null}
      {subtitle ? <p>{subtitle}</p> : null}
      {children}
    </section>
  ),
  Pagination: () => <div>pagination</div>,
}));

describe('DailyPicksPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    getListMock.mockResolvedValue({
      items: [
        {
          id: 1,
          generatedAt: '2026-03-25T09:00:00',
          runStatus: 'success',
          degraded: false,
          confidence: 'high',
          candidateCount: 20,
          outputCount: 5,
          pickCount: 5,
          topNames: ['算力一号'],
        },
      ],
      total: 1,
      page: 1,
      limit: 10,
    });
    getDetailMock.mockResolvedValue({
      id: 1,
      source: 'manual',
      generatedAt: '2026-03-25T09:00:00',
      runStatus: 'success',
      degraded: false,
      confidence: 'high',
      marketNews: [],
      sourceSummary: {
        news: [],
        sectorRankings: [],
        stockList: [],
      },
      recommendations: [
        {
          rank: 1,
          stockCode: '600001',
          stockName: '算力一号',
          sectorName: '人工智能',
          score: 92.5,
          recommendReason: 'AI 综合判断该股与热门新闻和板块主线联系较强。',
          operationAdvice: '关注回踩承接。',
          riskWarning: '热点退潮时及时止盈止损。',
          newsConnection: '新闻中提到算力和大模型持续升温，算力一号是直接受益方向。',
          signalBreakdown: {
            technical: '涨幅与量比同步走强。',
            sentiment: '人工智能新闻热度最高。',
            capital: '成交额和换手率显示资金活跃。',
            sector: '人工智能板块保持强势。',
          },
          relatedNews: [
            {
              title: '人工智能方向热度升温',
              relationReason: '新闻直接提到了算力和大模型催化。',
            },
          ],
        },
      ],
    });
  });

  it('renders news connection and relation reason for recommendations', async () => {
    render(<DailyPicksPage />);

    await waitFor(() => {
      expect(screen.getByText('新闻关联')).toBeInTheDocument();
    });

    expect(screen.getByText(/新闻中提到算力和大模型持续升温，算力一号是直接受益方向。/)).toBeInTheDocument();
    expect(screen.getByText(/新闻直接提到了算力和大模型催化。/)).toBeInTheDocument();
    expect(screen.getByText(/涨幅与量比同步走强。/)).toBeInTheDocument();
  });
});
