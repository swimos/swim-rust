import { ValueFormatterFunc } from "ag-grid-community";
import { Stock } from "../../components/Table";

const DATA_PLACHOLDER = "--";

export const numValueFormatter: ValueFormatterFunc<Stock, number | undefined> = (param) =>
  param.value ? param.value.toFixed(2) : DATA_PLACHOLDER;

export const pctValueFormatter: ValueFormatterFunc<Stock, number | undefined> = (param) => `${numValueFormatter(param)}%`;

export const volumeValueFormatter: ValueFormatterFunc<Stock, number | undefined> = (param) =>
  param.value ? (param.value / 1_000_000).toFixed(4) : DATA_PLACHOLDER;
