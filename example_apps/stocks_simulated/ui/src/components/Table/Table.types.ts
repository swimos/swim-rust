export interface TableProps {
  search: string;
}

export type PriceChangeState = "falling" | "rising" | null | undefined;

export type Stock = {
  price: number;
  volume?: number;
  movement?: number;
  timestamp: number;
  state: PriceChangeState;
};

// for use determining correct row style to apply
export type StockMeta = {
  timer: NodeJS.Timeout | null;
  prevDisplayedPrice?: number;
  priceLastUpdated?: number;
}

export type StockRow = Stock & { key: string };
