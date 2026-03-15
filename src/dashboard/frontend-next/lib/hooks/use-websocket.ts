"use client";

import { useEffect, useRef, useState, useCallback } from "react";
import type { MarketDataUpdate } from "@/lib/types/market-data";
import type { SystemMetrics } from "@/lib/types/system";

const WS_BASE = process.env.NEXT_PUBLIC_WS_BASE_URL ?? "ws://localhost:8000";

const MAX_RECONNECT_DELAY = 30_000;

interface UseMarketWebSocketResult {
  readonly prices: ReadonlyMap<string, MarketDataUpdate>;
  readonly isConnected: boolean;
}

export function useMarketWebSocket(
  symbols: readonly string[],
): UseMarketWebSocketResult {
  const [prices, setPrices] = useState<Map<string, MarketDataUpdate>>(
    new Map(),
  );
  const [isConnected, setIsConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(`${WS_BASE}/ws/market-data`);
    wsRef.current = ws;

    ws.onopen = () => {
      setIsConnected(true);
      retryRef.current = 0;
      if (symbols.length > 0) {
        ws.send(JSON.stringify({ type: "subscribe", symbols: [...symbols] }));
      }
    };

    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (msg.type === "market_data" && msg.symbol && msg.data) {
          setPrices((prev) => {
            const next = new Map(prev);
            next.set(msg.symbol, msg.data as MarketDataUpdate);
            return next;
          });
        }
      } catch {
        // ignore malformed messages
      }
    };

    ws.onclose = () => {
      setIsConnected(false);
      const delay = Math.min(1000 * 2 ** retryRef.current, MAX_RECONNECT_DELAY);
      retryRef.current += 1;
      setTimeout(connect, delay);
    };

    ws.onerror = () => ws.close();
  }, [symbols]);

  useEffect(() => {
    connect();
    return () => {
      wsRef.current?.close();
      wsRef.current = null;
    };
  }, [connect]);

  // Re-subscribe when symbols change
  useEffect(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN && symbols.length > 0) {
      wsRef.current.send(
        JSON.stringify({ type: "subscribe", symbols: [...symbols] }),
      );
    }
  }, [symbols]);

  return { prices, isConnected };
}

interface UseSystemWebSocketResult {
  readonly metrics: SystemMetrics | null;
  readonly isConnected: boolean;
}

export function useSystemWebSocket(): UseSystemWebSocketResult {
  const [metrics, setMetrics] = useState<SystemMetrics | null>(null);
  const [isConnected, setIsConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(`${WS_BASE}/ws/system-metrics`);
    wsRef.current = ws;

    ws.onopen = () => {
      setIsConnected(true);
      retryRef.current = 0;
    };

    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (msg.type === "system_metrics" && msg.data) {
          setMetrics(msg.data as SystemMetrics);
        }
      } catch {
        // ignore
      }
    };

    ws.onclose = () => {
      setIsConnected(false);
      const delay = Math.min(1000 * 2 ** retryRef.current, MAX_RECONNECT_DELAY);
      retryRef.current += 1;
      setTimeout(connect, delay);
    };

    ws.onerror = () => ws.close();
  }, []);

  useEffect(() => {
    connect();
    return () => {
      wsRef.current?.close();
      wsRef.current = null;
    };
  }, [connect]);

  return { metrics, isConnected };
}
