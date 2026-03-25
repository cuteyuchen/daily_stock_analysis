import type React from 'react';
import { useCallback, useEffect, useState } from 'react';
import { dailyPicksApi, type DailyPickDetail, type DailyPickSummaryItem } from '../api/dailyPicks';
import { getParsedApiError, type ParsedApiError } from '../api/error';
import { ApiErrorAlert, Badge, Button, Card, Pagination } from '../components/common';
import { AppPage } from '../components/common/AppPage';

const PAGE_SIZE = 10;

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
      const recordId = Number((result as Record<string, unknown>).record_id || 0);
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

  return (
    <AppPage className="space-y-6">
      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div>
          <div className="label-uppercase">Opportunities</div>
          <h1 className="mt-2 text-3xl font-semibold text-foreground">每日热点推荐</h1>
          <p className="mt-2 text-sm text-secondary-text">聚合热点板块、市场新闻与候选个股，生成每日 5 只关注标的。</p>
        </div>
        <Button onClick={() => void handleGenerate()} disabled={generating}>
          {generating ? '生成中…' : '立即生成一版'}
        </Button>
      </div>

      {error ? <ApiErrorAlert error={error} /> : null}

      <div className="grid gap-6 lg:grid-cols-[360px_minmax(0,1fr)]">
        <Card title="历史推荐" subtitle="runs" className="min-h-[560px]">
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
                    <Badge variant="info">{item.pickCount} picks</Badge>
                  </div>
                  <div className="mt-2 text-xs text-secondary-text">{item.generatedAt?.replace('T', ' ').slice(0, 19)}</div>
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
              <div className="space-y-4">
                <div className="flex flex-wrap items-center gap-3 text-sm text-secondary-text">
                  <Badge variant="info">{selected.strategyVersion}</Badge>
                  <span>生成时间：{selected.generatedAt?.replace('T', ' ').slice(0, 19)}</span>
                  <span>来源：{selected.source}</span>
                  <span>数量：{selected.pickCount}</span>
                </div>
                {'generationNote' in selected.payload ? (
                  <div className="rounded-2xl border border-warning/20 bg-warning/8 px-4 py-3 text-sm text-secondary-text">
                    {(selected.payload as Record<string, unknown>).generationNote as string}
                  </div>
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
                        <Badge variant="success">{item.score ?? '--'}</Badge>
                      </div>
                      <div className="mt-4 space-y-3 text-sm">
                        <div>
                          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">推荐理由</div>
                          <p className="mt-1 text-secondary-text">{item.recommendReason || '--'}</p>
                        </div>
                        <div>
                          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">操作建议</div>
                          <p className="mt-1 text-secondary-text">{item.operationAdvice || '--'}</p>
                        </div>
                        <div>
                          <div className="text-xs uppercase tracking-[0.18em] text-muted-text">风险提示</div>
                          <p className="mt-1 text-secondary-text">{item.riskWarning || '--'}</p>
                        </div>
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
