"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { CardSkeleton } from "@/components/shared/card-skeleton";
import { MOCK_MARKET_OVERVIEW } from "@/lib/mock-data/market";
import {
  useMarketOverview,
  useTechnicalIndicators,
} from "@/lib/hooks/use-market-data";
import { cn } from "@/lib/utils";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";

function getIndicators(
  symbol: string,
  overview: readonly { readonly symbol: string; readonly price: number }[],
) {
  const seed = symbol.charCodeAt(0) + symbol.charCodeAt(1);
  const price = overview.find((m) => m.symbol === symbol)?.price ?? 100;
  return {
    rsi: 30 + ((seed * 7) % 50),
    macd: ((seed % 20) - 10) * 0.3,
    signal: ((seed % 16) - 8) * 0.3,
    sma20: price * (0.9 + (seed % 10) * 0.02),
    sma50: price * (0.85 + (seed % 8) * 0.02),
    bbUpper: price * 1.05,
    bbLower: price * 0.95,
    adx: 15 + (seed % 40),
    cci: -100 + ((seed * 3) % 200),
    stochK: 10 + (seed % 80),
  };
}

function SignalBadge({
  signal,
}: {
  readonly signal: "buy" | "sell" | "neutral";
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-[11px] font-semibold",
        signal === "buy"
          ? "bg-positive/10 text-positive"
          : signal === "sell"
            ? "bg-negative/10 text-negative"
            : "bg-muted text-muted-foreground",
      )}
    >
      {signal === "buy" ? (
        <TrendingUp className="h-3 w-3" />
      ) : signal === "sell" ? (
        <TrendingDown className="h-3 w-3" />
      ) : (
        <Minus className="h-3 w-3" />
      )}
      {signal.charAt(0).toUpperCase() + signal.slice(1)}
    </span>
  );
}

function rsiSignal(rsi: number): "buy" | "sell" | "neutral" {
  if (rsi < 30) return "buy";
  if (rsi > 70) return "sell";
  return "neutral";
}

function macdSignal(macd: number, signal: number): "buy" | "sell" | "neutral" {
  if (macd > signal + 0.5) return "buy";
  if (macd < signal - 0.5) return "sell";
  return "neutral";
}

function smaSignal(price: number, sma: number): "buy" | "sell" | "neutral" {
  const diff = (price - sma) / sma;
  if (diff > 0.01) return "buy";
  if (diff < -0.01) return "sell";
  return "neutral";
}

function RsiGauge({ value }: { readonly value: number }) {
  const pct = Math.min(Math.max(value, 0), 100);
  const color =
    pct < 30 ? "bg-positive" : pct > 70 ? "bg-negative" : "bg-yellow-400";
  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between text-[11px]">
        <span className="text-muted-foreground">Oversold</span>
        <span className="font-bold text-[13px]">{value.toFixed(1)}</span>
        <span className="text-muted-foreground">Overbought</span>
      </div>
      <div className="h-2 rounded-full bg-muted overflow-hidden relative">
        <div className="absolute left-[30%] top-0 bottom-0 w-px bg-border/60" />
        <div className="absolute left-[70%] top-0 bottom-0 w-px bg-border/60" />
        <div
          className={cn("h-full rounded-full transition-all", color)}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

export default function AnalysisPage() {
  const [symbol, setSymbol] = useState("AAPL");

  const { data: apiOverview } = useMarketOverview();
  const { data: apiIndicators, isLoading: indLoading } =
    useTechnicalIndicators(symbol);

  const overview = apiOverview ?? MOCK_MARKET_OVERVIEW;
  const symbols = overview.map((m) => m.symbol);

  const fallback = getIndicators(symbol, overview);
  const current = overview.find((m) => m.symbol === symbol);
  const price = current?.price ?? 100;

  // Overlay API indicator values onto fallback when available
  const apiMap: Record<string, number> = {};
  if (apiIndicators?.length) {
    for (const i of apiIndicators) apiMap[i.name] = i.value;
  }
  const ind = {
    rsi: apiMap.RSI ?? fallback.rsi,
    macd: apiMap.MACD ?? fallback.macd,
    signal: fallback.signal,
    sma20: apiMap.SMA_20 ?? fallback.sma20,
    sma50: apiMap.SMA_50 ?? fallback.sma50,
    bbUpper: apiMap.BOLLINGER_UPPER ?? fallback.bbUpper,
    bbLower: apiMap.BOLLINGER_LOWER ?? fallback.bbLower,
    adx: fallback.adx,
    cci: fallback.cci,
    stochK: fallback.stochK,
  };

  const INDICATORS = [
    {
      name: "RSI (14)",
      value: `${ind.rsi.toFixed(1)}`,
      signal: rsiSignal(ind.rsi),
      desc:
        ind.rsi < 30
          ? "Oversold — potential reversal"
          : ind.rsi > 70
            ? "Overbought — potential pullback"
            : "Neutral zone",
    },
    {
      name: "MACD",
      value: `${ind.macd.toFixed(2)}`,
      signal: macdSignal(ind.macd, ind.signal),
      desc: `Signal: ${ind.signal.toFixed(2)} · Histogram: ${(ind.macd - ind.signal).toFixed(2)}`,
    },
    {
      name: "SMA 20",
      value: `$${ind.sma20.toFixed(2)}`,
      signal: smaSignal(price, ind.sma20),
      desc: `Price ${price > ind.sma20 ? "above" : "below"} 20-day average`,
    },
    {
      name: "SMA 50",
      value: `$${ind.sma50.toFixed(2)}`,
      signal: smaSignal(price, ind.sma50),
      desc: `Price ${price > ind.sma50 ? "above" : "below"} 50-day average`,
    },
    {
      name: "Bollinger Bands",
      value: `${(((price - ind.bbLower) / (ind.bbUpper - ind.bbLower)) * 100).toFixed(0)}%B`,
      signal:
        price > ind.bbUpper
          ? ("sell" as const)
          : price < ind.bbLower
            ? ("buy" as const)
            : ("neutral" as const),
      desc: `Upper: $${ind.bbUpper.toFixed(2)} · Lower: $${ind.bbLower.toFixed(2)}`,
    },
    {
      name: "ADX (14)",
      value: `${ind.adx.toFixed(1)}`,
      signal: "neutral" as const,
      desc: ind.adx > 25 ? "Strong trend in place" : "Weak or no trend",
    },
    {
      name: "CCI (20)",
      value: `${ind.cci.toFixed(0)}`,
      signal:
        ind.cci > 100
          ? ("sell" as const)
          : ind.cci < -100
            ? ("buy" as const)
            : ("neutral" as const),
      desc:
        ind.cci > 100
          ? "Overbought"
          : ind.cci < -100
            ? "Oversold"
            : "Normal range",
    },
    {
      name: "Stoch %K",
      value: `${ind.stochK.toFixed(1)}`,
      signal:
        ind.stochK > 80
          ? ("sell" as const)
          : ind.stochK < 20
            ? ("buy" as const)
            : ("neutral" as const),
      desc: `Momentum: ${ind.stochK > 50 ? "positive" : "negative"}`,
    },
  ];

  const buys = INDICATORS.filter((i) => i.signal === "buy").length;
  const sells = INDICATORS.filter((i) => i.signal === "sell").length;
  const neutrals = INDICATORS.filter((i) => i.signal === "neutral").length;
  const overallSignal: "buy" | "sell" | "neutral" =
    buys > sells + 1 ? "buy" : sells > buys + 1 ? "sell" : "neutral";

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">
          Technical Analysis
        </h1>
        <p className="text-sm text-muted-foreground mt-1">
          Indicators and signals for informed trade decisions.
        </p>
      </div>

      {/* Symbol selector */}
      <div className="flex flex-wrap items-center gap-1.5">
        {symbols.map((s) => (
          <button
            key={s}
            onClick={() => setSymbol(s)}
            className={cn(
              "rounded-xl px-3 py-1.5 text-xs font-semibold transition-all active:scale-[0.95]",
              symbol === s
                ? "bg-primary text-primary-foreground shadow-sm"
                : "bg-muted text-muted-foreground hover:text-foreground hover:bg-accent",
            )}
          >
            {s}
          </button>
        ))}
      </div>

      {/* Overall signal + RSI gauge */}
      {indLoading && !apiIndicators ? (
        <div className="grid gap-4 sm:grid-cols-2">
          <CardSkeleton />
          <CardSkeleton />
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2">
          <Card>
            <CardContent className="p-5">
              <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground mb-3">
                Overall Signal · {symbol}
              </p>
              <div className="flex items-center gap-3 mb-3">
                <SignalBadge signal={overallSignal} />
                <span className="text-[13px] text-muted-foreground">
                  {buys}B / {neutrals}N / {sells}S from {INDICATORS.length}{" "}
                  indicators
                </span>
              </div>
              <div className="flex items-center gap-2">
                <div className="h-2 flex-1 rounded-full bg-muted overflow-hidden">
                  <div className="h-full flex">
                    <div
                      className="bg-positive"
                      style={{ width: `${(buys / INDICATORS.length) * 100}%` }}
                    />
                    <div
                      className="bg-muted-foreground/30"
                      style={{
                        width: `${(neutrals / INDICATORS.length) * 100}%`,
                      }}
                    />
                    <div
                      className="bg-negative"
                      style={{ width: `${(sells / INDICATORS.length) * 100}%` }}
                    />
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="p-5">
              <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground mb-3">
                RSI (14)
              </p>
              <RsiGauge value={ind.rsi} />
              <p className="text-[11px] text-muted-foreground mt-2">
                {ind.rsi < 30
                  ? "Oversold — potential reversal opportunity"
                  : ind.rsi > 70
                    ? "Overbought — consider taking profit"
                    : "Neutral — no extreme reading"}
              </p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Indicator grid */}
      <Card>
        <CardHeader className="pb-2 pt-5 px-5">
          <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
            Indicator Readings
          </CardTitle>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          {indLoading && !apiIndicators ? (
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              {Array.from({ length: 8 }).map((_, i) => (
                <CardSkeleton key={i} />
              ))}
            </div>
          ) : (
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              {INDICATORS.map((indicator) => (
                <div
                  key={indicator.name}
                  className="rounded-xl border border-border/60 bg-muted/30 p-4"
                >
                  <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground mb-2">
                    {indicator.name}
                  </p>
                  <p className="text-[15px] font-bold mb-2">
                    {indicator.value}
                  </p>
                  <SignalBadge signal={indicator.signal} />
                  <p className="text-[10px] text-muted-foreground mt-2 leading-relaxed">
                    {indicator.desc}
                  </p>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
