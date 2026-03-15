import { apiGet, apiPost, isMockMode } from "./client";
import type {
  MarketOverview,
  SectorPerformance,
  TopMover,
  HistoricalDataPoint,
  TechnicalIndicator,
} from "@/lib/types/market-data";
import {
  MOCK_MARKET_OVERVIEW,
  MOCK_SECTOR_PERFORMANCE,
  MOCK_TOP_GAINERS,
  MOCK_TOP_LOSERS,
} from "@/lib/mock-data/market";

export async function fetchMarketOverview(): Promise<
  readonly MarketOverview[]
> {
  if (isMockMode()) return MOCK_MARKET_OVERVIEW;
  return apiGet<MarketOverview[]>("/api/v1/market-data/overview");
}

export async function fetchSectorPerformance(): Promise<
  readonly SectorPerformance[]
> {
  if (isMockMode()) return MOCK_SECTOR_PERFORMANCE;
  return apiGet<SectorPerformance[]>("/api/v1/market-data/sectors");
}

export async function fetchTopGainers(): Promise<readonly TopMover[]> {
  if (isMockMode()) return MOCK_TOP_GAINERS;
  return apiGet<TopMover[]>("/api/v1/market-data/top-gainers");
}

export async function fetchTopLosers(): Promise<readonly TopMover[]> {
  if (isMockMode()) return MOCK_TOP_LOSERS;
  return apiGet<TopMover[]>("/api/v1/market-data/top-losers");
}

export async function fetchHistoricalData(
  symbol: string,
  startDate: string,
  endDate: string,
  interval: string = "1d",
): Promise<readonly HistoricalDataPoint[]> {
  if (isMockMode()) return generateMockHistorical(symbol, 90);
  const res = await apiPost<HistoricalDataPoint[]>(
    "/api/v1/market-data/historical",
    { symbol, start_date: startDate, end_date: endDate, interval },
  );
  return res;
}

export async function fetchTechnicalIndicators(
  symbol: string,
): Promise<readonly TechnicalIndicator[]> {
  if (isMockMode()) return generateMockIndicators(symbol);
  const res = await apiGet<{
    indicators?: TechnicalIndicator[];
    data?: { indicators?: TechnicalIndicator[] };
  }>(`/api/v1/market-data/current/${symbol}?include_indicators=true`);
  return res.indicators ?? res.data?.indicators ?? [];
}

function generateMockHistorical(
  symbol: string,
  days: number,
): HistoricalDataPoint[] {
  const seed = symbol.charCodeAt(0) + symbol.charCodeAt(1);
  let price = 100 + seed;
  return Array.from({ length: days }, (_, i) => {
    price = price * (1 + Math.sin(seed + i * 0.3) * 0.015 + 0.002);
    const d = new Date();
    d.setDate(d.getDate() - (days - i));
    return {
      date: d.toISOString().slice(0, 10),
      open: Math.round(price * 0.995 * 100) / 100,
      high: Math.round(price * 1.01 * 100) / 100,
      low: Math.round(price * 0.99 * 100) / 100,
      close: Math.round(price * 100) / 100,
      volume: Math.round(Math.random() * 50_000_000 + 5_000_000),
    };
  });
}

function generateMockIndicators(symbol: string): TechnicalIndicator[] {
  const s = symbol.charCodeAt(0) + symbol.charCodeAt(1);
  const rsi = 30 + ((s * 7) % 50);
  const macd = ((s % 20) - 10) * 0.3;
  return [
    {
      name: "RSI",
      value: rsi,
      signal: rsi < 30 ? "buy" : rsi > 70 ? "sell" : "neutral",
    },
    {
      name: "MACD",
      value: macd,
      signal: macd > 1 ? "buy" : macd < -1 ? "sell" : "neutral",
    },
    { name: "SMA_20", value: 100 + s * 0.5, signal: "neutral" },
    { name: "SMA_50", value: 100 + s * 0.45, signal: "neutral" },
    { name: "EMA_20", value: 100 + s * 0.48, signal: "neutral" },
    { name: "BOLLINGER_UPPER", value: (100 + s) * 1.05, signal: "neutral" },
    { name: "BOLLINGER_LOWER", value: (100 + s) * 0.95, signal: "neutral" },
  ];
}
