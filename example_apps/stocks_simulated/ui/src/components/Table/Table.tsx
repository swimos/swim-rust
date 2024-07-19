import { FC, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Form } from "@swim/structure";
import { AgGridReact } from "ag-grid-react";
import { CellStyle, ColDef, GridOptions, RowStyle } from "ag-grid-community";
import { throttle } from "lodash-es";
import { TableProps, Stock, StockRow, PriceChangeState, StockMeta } from "./Table.types";
import { numValueFormatter, pctValueFormatter, volumeValueFormatter } from "../../lib/helpers/formatting";
import { StockForm } from "./StockForm";
import { useMapDownlink } from "../../lib/hooks/useMapDownlink";
import "ag-grid-community/styles/ag-grid.css";

const NEW_STOCK_METADATA: StockMeta = { timer: null, priceLastUpdated: 0, prevDisplayedPrice: 0 };
const UPDATED_ROW_STYLE_DURATION_MS = 2000;
const MAX_UI_REFRESH_INTERVAL_MS = 16; // ~60/sec
const hostUri: string = (() => {
  const urlParams = new URLSearchParams(window.location.search);
  let uri = urlParams.get("host");

  if (!uri) {
    const protocol = window.location.protocol.startsWith("https") ? "warps:" : "warp:";
    let hostFragment = window.location.host;

    /* If the UI is being served from localhost and no host is explicitly provided,
       then fall back to the default Swim server port, localhost:9001 */
    if (hostFragment.startsWith("localhost:") || hostFragment.startsWith("127.0.0.1:")) {
      hostFragment = "localhost:9001";
    }

    uri = `${protocol}//${hostFragment}`;
  }

  return uri
})();

const getRowStyle: GridOptions<StockRow>["getRowStyle"] = (params) => {
  const isDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
  const styles: RowStyle = {
    backgroundColor: params.rowIndex % 2 === 0 ? `var(--app-bg${isDark ? "-dark" : ""})` : `var(--row-bg-secondary${isDark ? "-dark" : ""})`,
  };
  if (params?.data?.state != null) {
    styles.color = params.data.state === "falling" ? "var(--red-alert)" : "var(--green-alert)";
    styles.fontWeight = 500;
  }
  return styles;
};

const cellStyle: CellStyle = {
  height: "100%",
  display: "flex ",
  justifyContent: "center",
  alignItems: "center ",
};
const headerClass = "text-center text-secondary dark:text-secondary-dark text-sm font-medium";
const COLUMN_DEFS: ColDef[] = [
  { field: "key", headerName: "Symbol", cellStyle, headerClass },
  { field: "price", headerName: "Price", valueFormatter: numValueFormatter, cellStyle, headerClass, getQuickFilterText: () => '' },
  { field: "movement", headerName: "Movement", valueFormatter: pctValueFormatter, cellStyle, headerClass, getQuickFilterText: () => '' },
  { field: "volume", headerName: "Volume (Millions)", valueFormatter: volumeValueFormatter, cellStyle, headerClass, getQuickFilterText: () => '' },
];

export const Table: FC<TableProps> = (props) => {
  const { search } = props;

  /* Row data for passing to AGGridReact element; derived from stocksRef
     Updates to this value trigger a rerender */
  const [rowData, setRowData] = useState<StockRow[]>([]);
  // Stock data for display in table; updates to this value do not trigger rerenders
  const stocksRef = useRef<Record<string, StockRow>>({});
  // Stock metadata to help with styling; updates to this value do not trigger rerenders
  const stocksMetaRef = useRef<Record<string, StockMeta>>({});

  const lastRowDataUpdatedAt = useRef<number>(0);

  const updateRowData: () => void = useMemo(
    () => (
      throttle(
        // fire at most every MAX_UI_REFRESH_INTERVAL_MS, including both the leading and trailing invocation
        () => {
          lastRowDataUpdatedAt.current = Date.now();
          setRowData(Object.values(stocksRef.current));
        },
        MAX_UI_REFRESH_INTERVAL_MS,
        { leading: true, trailing: true }
      ) as () => void
    ),
    []
  );

  // callback which handles individual Stock updates
  const didUpdate: (key: string, newStock: Stock | undefined, oldStock: Stock | undefined) => void = useCallback(
    (key, newStock) => {
      // handle update in unexpected format
      if (!newStock) {
        return;
      }

      let state: PriceChangeState = null;

      if (stocksRef.current[key]) {
        const newStockMeta: StockMeta = stocksMetaRef.current[key] ?? { ...NEW_STOCK_METADATA };
        // if present, cancel existing timeout for resetting row styles
        if (newStockMeta.timer != null) {
          clearTimeout(newStockMeta.timer);
          newStockMeta.timer = null;
        }

        /* Ensure correct price is being used for determining direction of price change. Must use last
           *displayed* price since we're batching updates, instead of simply last price on record */
        if (newStockMeta.priceLastUpdated! < lastRowDataUpdatedAt.current) {
          newStockMeta.prevDisplayedPrice = stocksRef.current[key].price;
        }

        // determine direction of price change, if any
        if (newStock.price && newStockMeta.prevDisplayedPrice) {
          if (newStock.price > newStockMeta.prevDisplayedPrice) {
            state = "rising";
          } else if (newStock.price < newStockMeta.prevDisplayedPrice) {
            state = "falling";
          }
        }

        // define callback for resetting row styles
        const resetRowStyle = () => {
          stocksRef.current[key] = {
            ...stocksRef.current[key],
            state: null,
          }
          stocksMetaRef.current[key].timer = null;

          updateRowData();
        };

        // clear row styles after a delay; set newStock metadata
        newStockMeta.timer = setTimeout(resetRowStyle, UPDATED_ROW_STYLE_DURATION_MS);
        newStockMeta.priceLastUpdated = newStock.timestamp;
        stocksMetaRef.current[key] = newStockMeta;
      }


      // Update data for this newStock key in stocksRef. This will not trigger a rerender.
      stocksRef.current[key] = {
        ...newStock,
        key,
        state,
      };

      updateRowData();
    },
    [updateRowData]
  );

  // callback which handles individual Stock updates
  const didRemove: (key: string, oldStock: Stock | undefined) => void = useCallback(
    (key, stock) => {
      // handle invalid message
      if (!stock) {
        return;
      }

      // Delete key for this stock key in stocksRef and stocksMetaRef. This will not trigger a rerender.
      delete stocksMetaRef.current[key];
      delete stocksRef.current[key];

      updateRowData();
    },
    [updateRowData]
  );

  // open downlink on component mount
  const downlink = useMapDownlink<string, Stock | undefined>({
    hostUri,
    nodeUri: "/symbols",
    laneUri: "stocks",
    keyForm: Form.forString(),
    valueForm: new StockForm(), // coerces content of WARP message to strongly-typed JS object
    didUpdate,
    didRemove,
  });

  useEffect(() => {
    downlink.open();

    return () => {
      downlink?.close();
    };
  }, [downlink]);

  return (
    <div className="h-full px-4 lg:px-4 justify-center">
      <AgGridReact
        rowData={rowData}
        rowHeight={44}
        getRowStyle={getRowStyle}
        columnDefs={COLUMN_DEFS}
        getRowId={(params) => params.data.key}
        deltaSort
        quickFilterText={search}
        autoSizeStrategy={{
          type: "fitGridWidth",
          defaultMinWidth: 80,
          columnLimits: [
            {
              colId: "key",
              maxWidth: 160,
            },
          ],
        }}
      />
    </div>
  );
};
