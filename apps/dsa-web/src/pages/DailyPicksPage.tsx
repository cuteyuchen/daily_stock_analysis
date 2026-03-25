import type React from 'react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { dailyPicksApi, type DailyPickDetail, type DailyPickRecommendation, type DailyPickSummaryItem } from '../api/dailyPicks';
import { getParsedApiError, type ParsedApiError } from '../api/error';
import { ApiErrorAlert, Badge, Button, Card, Pagination } from '../components/common';
import { AppPage } from '../components/common/AppPage';

const PAGE_SIZE = 10;

const badgeVariantForStatus = (status?: string) => {
  switch ((status || '').toLowerCase()) {
    case 'success':
      return 'success' as const;
    case 'degraded':
      return 'warning' as const;
    case 'failed':
      return 'danger' as const;
    default:
      return 'info' as const;
  }
};

const badgeVariantForConfidence = (confidence?: string) => {
  switch ((confidence || '').toLowerCase()) {
    case 'high':
      return 'success' as const;
    case 'medium':
      return 'warning' as const;
    case 'low':
      return 'danger' as const;
    default:
      return 'info' as const;
  }
};

const formatDateTime = (value?: string | null) => value?.replace('T', ' ').slice(0, 19) || '--';

const summarizeSourceEvents = (sourceSummary?: Record<string, unknown>, key?: string): string[] => {
  const events = key ? sourceSummary?.[key] : sourceSummary;
  if (!Array.isArray(events)) {
    return [];
  }
  return events.slice(0, 6).map((event) => {
    const item = event as Record<string, unknown>;
    const provider = String(item.provider || item.query || 'unknown');
    const result = String(item.result || 'unknown');
    const count = item.count ?? item.topCount ?? item.bottomCount;
    return `${provider}: ${result}${count !== undefined ? ` (${count})` : ''}`;
  });
};

const quoteMetric = (recommendation: DailyPickRecommendation, key: string) => {
  const quote = recommendation.quote as Record<string, unknown> | undefined;
  return quote?.[key];
};

const DailyPicksPage: React.FC = () => {
  const [items, setItems] = useState<DailyPickSummaryItem[]>([]);
  const [selected, setSelected] = useState<DailyPickDetail | null>(null);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState<ParsedApiError | null>(null);

  const loadList = useCallback(async (nextPage = page) => {
    setLoading(true);
    setError(null);
    try {
      const data = await dailyPicksApi.getList(nextPage, PAGE_SIZE);
      setItems(data.items || []);
      setTotal(data.total || 0);
      if ((data.items || []).length > 0 && !selected) {
        const detail = await dailyPicksApi.getDetail(data.items[0].id);
        setSelected(detail);
      }
    } catch (err) {
      setError(getParsedApiError(err));
    } finally {
      setLoading(false);
    }
  }, [page, selected]);

  const loadDetail = useCallback(async (id: number) => {
    setLoading(true);
    setError(null);
    try {
      const detail = await dailyPicksApi.getDetail(id);
      setSelected(detail);
    } catch (err) {
      setError(getParsedApiError(err));
    } finally {
      setLoading(false);
    }
  }, []);

  const handleGenerate = useCallback(async () => {
    setGenerating(true);
    setError(null);
    try {
      const result = await dailyPicksApi.generate(5);
      const recordId = Number((result as Record<string, unknown>).recordId || 0);
      await loadList(1);
      setPage(1);
      if (recordId > 0) {
        await loadDetail(recordId);
      }
    } catch (err) {
      setError(getParsedApiError(err));
    } finally {
      setGenerating(false);
    }
  }, [loadDetail, loadList]);

  useEffect(() => {
    document.title = '热点推荐 - DSA';
    void loadList(page);
  }, [loadList, page]);

  const latestStatus = useMemo(() => items[0] || null, [items]);
  const newsSummary = useMemo(() => summarizeSourceEvents(selected?.sourceSummary, 'news'), [selected]);
  const sectorSummary = useMemo(() => summarizeSourceEvents(selected?.sourceSummary, 'sectorRankings'), [selected]);
  const stockPoolSummary = useMemo(() => summarizeSourceEvents(selected?.sourceSummary, 'stockList'), [selected]);
  const aiSummary = useMemo(() => summarizeSourceEvents(selected?.sourceSummary, 'aiReasoning'), [selected]);

  return (
    <AppPage className="space-y-6">
      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div>
          <div className="label-uppercase">Opportunities</div>
          <h1 className="mt-2 text-3xl font-semibold text-foreground">每日热点推荐</h1>
          <p className="mt-2 text-sm text-secondary-text">
            以“稳定产出 Top5”为优先目标，自动展示本次是否降级、数据质量、数据源状态与推荐详情。
          </p>
        </div>
        <Button onClick={() => void handleGenerate()} disabled={generating}>
          {generating ? '生成中…' : '立即生成'}
        </Button>
      </div>

      {error ? <ApiErrorAlert error={error} /> : null}

      <div className="grid gap-4 xl:grid-cols-4">
        <Card title="最近运行" subtitle="latest run">
          <div className="space-y-3 text-sm">
            <div className="flex flex-wrap gap-2">
              <Badge variant={badgeVariantForStatus(latestStatus?.runStatus)}>{latestStatus?.runStatus || 'unknown'}</Badge>
              {latestStatus?.degraded ? <Badge variant="warning">degraded</Badge> : <Badge variant="success">normal</Badge>}
              {latestStatus?.confidence ? (
                <Badge variant={badgeVariantForConfidence(latestStatus.confidence)}>{latestStatus.confidence}</Badge>
              ) : null}
            </div>
            <div className="text-secondary-text">生成时间：{formatDateTime(latestStatus?.generatedAt)}</div>
            <div className="text-secondary-text">候选数：{latestStatus?.candidateCount ?? '--'} / 输出数：{latestStatus?.outputCount ?? latestStatus?.pickCount ?? '--'}</div>
          </div>
        </Card>

        <Card title="当前层级" subtitle="generation layer">
          <div className="space-y-3 text-sm text-secondary-text">
            <div>{selected?.generationLayer || '--'}</div>
            <div>耗时：{selected?.durationMs ? `${selected.durationMs} ms` : '--'}</div>
            <div>开始：{formatDateTime(selected?.startedAt)}</div>
            <div>结束：{formatDateTime(selected?.finishedAt)}</div>
          </div>
        </Card>

        <Card title="数据源概览" subtitle="providers">
          <div className="space-y-2 text-sm text-secondary-text">
            <div>成功：{(selected?.usedSources || []).join(', ') || '--'}</div>
            <div>失败：{(selected?.failedSources || []).join(', ') || '--'}</div>
          </div>
        </Card>

        <Card title="风险提示" subtitle="risk note">
          <p className="text-sm text-secondary-text">{selected?.riskNote || '--'}</p>
        </Card>
      </div>

      <div className="grid gap-6 lg:grid-cols-[360px_minmax(0,1fr)]">
        <Card title="历史记录" subtitle="runs" className="min-h-[560px]">
          <div className="space-y-3">
            {items.map((item) => {
              const active = selected?.id === item.id;
              return (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => void loadDetail(item.id)}
                  className={`w-full rounded-2xl border p-4 text-left transition-all ${active ? 'border-cyan/40 bg-cyan/10' : 'border-border/50 bg-elevated/30 hover:border-border hover:bg-hover'}`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-sm font-semibold text-foreground">#{item.id}</div>
                    <div className="flex flex-wrap gap-2">
                      <Badge variant={badgeVariantForStatus(item.runStatus)}>{item.runStatus || 'unknown'}</Badge>
                      {item.degraded ? <Badge variant="warning">degraded</Badge> : null}
                    </div>
                  </div>
                  <div className="mt-2 text-xs text-secondary-text">{formatDateTime(item.generatedAt)}</div>
                  <div className="mt-3 text-xs text-secondary-text">
                    候选 {item.candidateCount ?? '--'} / 输出 {item.outputCount ?? item.pickCount ?? '--'}
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {(item.topNames || []).map((name) => (
                      <Badge key={name} variant="history">{name}</Badge>
                    ))}
                  </div>
                </button>
              );
            })}

            <Pagination
              currentPage={page}
              totalPages={Math.max(1, Math.ceil(total / PAGE_SIZE))}
              onPageChange={(nextPage) => setPage(nextPage)}
            />
          </div>
        </Card>

        <div className="space-y-6">
          <Card title="推荐详情" subtitle="top 5 candidates">
            {!selected ? (
              <div className="text-sm text-secondary-text">{loading ? '加载中…' : '暂无数据'}</div>
            ) : (
              <div className="space-y-5">
                <div className="flex flex-wrap items-center gap-3 text-sm text-secondary-text">
                  <Badge variant={badgeVariantForStatus(selected.runStatus)}>{selected.runStatus || 'unknown'}</Badge>
                  <Badge variant={badgeVariantForConfidence(selected.confidence || undefined)}>{selected.confidence || '--'}</Badge>
                  <span>生成时间：{formatDateTime(selected.generatedAt)}</span>
                  <span>来源：{selected.source}</span>
                </div>

                {selected.degraded || selected.generationNote ? (
                  <div className="rounded-2xl border border-warning/20 bg-warning/8 px-4 py-3 text-sm text-secondary-text">
                    {selected.generationNote || '本次结果包含降级链路。'}
                  </div>
                ) : null}

                {Array.isArray(selected.errorSummary) && selected.errorSummary.length > 0 ? (
                  <div className="rounded-2xl border border-danger/20 bg-danger/8 px-4 py-3 text-sm text-secondary-text">
                    <div className="mb-2 text-xs uppercase tracking-[0.18em] text-muted-text">错误 / 降级摘要</div>
                    <div className="space-y-1">
                      {selected.errorSummary.map((item) => <div key={item}>• {item}</div>)}
                    </div>
                  </div>
                ) : null}

                <div className="grid gap-4 xl:grid-cols-4">
                  <Card variant="bordered" padding="sm" title="新闻链路" subtitle="news">
                    <div className="space-y-2 text-sm text-secondary-text">
                      {newsSummary.length ? newsSummary.map((item) => <div key={item}>{item}</div>) : <div>--</div>}
                    </div>
                  </Card>
                  <Card variant="bordered" padding="sm" title="板块链路" subtitle="sector">
                    <div className="space-y-2 text-sm text-secondary-text">
                      {sectorSummary.length ? sectorSummary.map((item) => <div key={item}>{item}</div>) : <div>--</div>}
                    </div>
                  </Card>
                  <Card variant="bordered" padding="sm" title="股票池链路" subtitle="pool">
                    <div className="space-y-2 text-sm text-secondary-text">
                      {stockPoolSummary.length ? stockPoolSummary.map((item) => <div key={item}>{item}</div>) : <div>--</div>}
                    </div>
                  </Card>
                  <Card variant="bordered" padding="sm" title="AI 增强" subtitle="reasoning">
                    <div className="space-y-2 text-sm text-secondary-text">
                      {aiSummary.length ? aiSummary.map((item) => <div key={item}>{item}</div>) : <div>--</div>}
                    </div>
                  </Card>
                </div>

                {Array.isArray(selected.marketNews) && selected.marketNews.length > 0 ? (
                  <Card variant="bordered" padding="sm" title="热点新闻" subtitle="market news">
                    <div className="space-y-3">
                      {selected.marketNews.slice(0, 6).map((news, index) => {
                        const raw = news as Record<string, unknown>;
                        return (
                          <div key={`${String(raw.url || raw.title || index)}`} className="rounded-xl border border-border/40 px-3 py-3 text-sm">
                            <div className="font-medium text-foreground">{String(raw.title || '未命名新闻')}</div>
                            <div className="mt-1 text-xs text-secondary-text">
                              {String(raw.source || '--')} · {String(raw.publishedDate || raw.published_date || '--')}
                            </div>
                            {raw.snippet ? <p className="mt-2 text-secondary-text">{String(raw.snippet)}</p> : null}
                          </div>
                        );
                      })}
                    </div>
                  </Card>
                ) : null}

                <div className="grid gap-4 md:grid-cols-2">
                  {(selected.recommendations || []).map((item) => (
                    <Card key={`${item.rank}-${item.stockCode || item.stockName}`} variant="bordered" padding="sm">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="text-xs text-secondary-text">#{item.rank}</div>
                          <div className="mt-1 text-lg font-semibold text-foreground">{item.stockName}</div>
                          <div className="mt-1 text-xs text-secondary-text">{item.stockCode || '--'} · {item.sectorName || '--'}</div>
                        </div>
                        <div className="flex flex-col items-end gap-2">
                          <Badge variant="success">{item.score ?? '--'}</Badge>
                          {item.confidence ? <Badge variant={badgeVariantForConfidence(item.confidence)}>{item.confidence}</Badge> : null}
                        </div>
                      </div>
                      <div className="mt-4 grid gap-2 text-xs text-secondary-text sm:grid-cols-2">
                        <div>涨跌幅：{String(quoteMetric(item, 'changePercent') ?? quoteMetric(item, 'change_percent') ?? '--')}</div>
                        <div>成交额：{String(quoteMetric(item, 'amount') ?? '--')}</div>
                        <div>换手率：{String(quoteMetric(item, 'turnoverRate') ?? quoteMetric(item, 'turnover_rate') ?? '--')}</div>
                        <div>量比：{String(quoteMetric(item, 'volumeRatio') ?? quoteMetric(item, 'volume_ratio') ?? '--')}</div>
                      </div>
                      <div className="mt-4 space-y-3 text-sm">
                        <div>
                          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">新闻关联</div>
                          <p className="mt-1 text-secondary-text">{item.newsConnection || '--'}</p>
                        </div>
                        <div>
                          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">推荐理由</div>
                          <p className="mt-1 text-secondary-text">{item.recommendReason || '--'}</p>
                        </div>
                        {item.signalBreakdown ? (
                          <div>
                            <div className="text-xs uppercase tracking-[0.18em] text-muted-text">信号拆解</div>
                            <div className="mt-1 space-y-1 text-secondary-text">
                              <div>技术面：{item.signalBreakdown.technical || '--'}</div>
                              <div>情绪面：{item.signalBreakdown.sentiment || '--'}</div>
                              <div>资金面：{item.signalBreakdown.capital || '--'}</div>
                              <div>板块面：{item.signalBreakdown.sector || '--'}</div>
                            </div>
                          </div>
                        ) : null}
                        <div>
                          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">操作建议</div>
                          <p className="mt-1 text-secondary-text">{item.operationAdvice || '--'}</p>
                        </div>
                        <div>
                          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">风险提示</div>
                          <p className="mt-1 text-secondary-text">{item.riskWarning || '--'}</p>
                          {item.riskNote ? <p className="mt-1 text-secondary-text">{item.riskNote}</p> : null}
                        </div>
                        {Array.isArray(item.relatedNews) && item.relatedNews.length > 0 ? (
                          <div>
                            <div className="text-xs uppercase tracking-[0.18em] text-muted-text">相关新闻</div>
                            <div className="mt-1 space-y-1 text-secondary-text">
                              {item.relatedNews.slice(0, 3).map((news, index) => {
                                const raw = news as Record<string, unknown>;
                                return (
                                  <div key={`${item.rank}-${index}`} className="rounded-xl border border-border/40 px-3 py-2">
                                    <div>{String(raw.title || raw.source || 'news')}</div>
                                    {raw.relationReason || raw.relation_reason ? (
                                      <div className="mt-1 text-xs text-muted-text">
                                        关联说明：{String(raw.relationReason || raw.relation_reason)}
                                      </div>
                                    ) : null}
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                        ) : null}
                      </div>
                    </Card>
                  ))}
                </div>
              </div>
            )}
          </Card>
        </div>
      </div>
    </AppPage>
  );
};

export default DailyPicksPage;
