import {ValueFormatterFunc} from "ag-grid-community";
import {StockRow} from "../../components/Table";

const DATA_PLACHOLDER = "--";

export const numValueFormatter: ValueFormatterFunc<StockRow, number | undefined> = (param) =>
    param.value ? param.value.toFixed(2) : DATA_PLACHOLDER;

export const pctValueFormatter: ValueFormatterFunc<StockRow, number | undefined> = (param) => `${numValueFormatter(param)}%`;

export const volumeValueFormatter: ValueFormatterFunc<StockRow, number | undefined> = (param) =>
    param.value ? (param.value / 1_000_000).toFixed(4) : DATA_PLACHOLDER;
