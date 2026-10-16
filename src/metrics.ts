export interface RelayMetrics {
  activeHosts: number;
  activePairs: number;
  pendingPairs: number;
  clientSlots: number;
  closingPairs: number;
  openSockets: number;
  forwardedMessages: number;
  forwardedBytes: number;
  supersededRegistrations: number;
  rejectedConnections: number;
  rejectedConnectionsByReason: Record<string, number>;
  draining: boolean;
  metricsSchemaVersion: 2;
  runtime: "node";
  nodeVersion: string;
  rssBytes: number;
  heapUsedBytes: number;
  heapTotalBytes: number;
  externalBytes: number;
  arrayBufferBytes: number;
  activeResources: number;
  bufferedBytes: number;
  eventLoopDelayP99Ms: number;
  eventLoopDelayMaxMs: number;
  dataBufferBytesPerSocket: number;
  topHosts?: {
    endpoint: string;
    bytesIn: number;
    bytesOut: number;
    pairs: number;
  }[];
}

export interface Metrics extends RelayMetrics {
  openConnections: number;
  openConnectionIPs: number;
  rateLimiterEntries: number;
  maxConnectionsPerClient: number;
}
