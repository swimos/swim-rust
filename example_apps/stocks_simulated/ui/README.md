# Stocks Demo

This UI displays a real-time table of a simulated collection of stocks that one might find listed on an exchange. Their
key properties such as current price and daily movement are simulated in the Rust Swim application located in the `/src`
directory of this repository. A single MapDownlink is opened by the UI to the `stocks` lane of the backend application's
`SymbolsAgent`. This downlink syncs with the lane's state containing pricing for all entries and then receives follow-on
price updates until the downlink is closed. With each update received, UI local state and table content is updated.

The UI in this folder was bootstrapped with React + TypeScript + Vite and uses `ag-grid-react` for its table component.

