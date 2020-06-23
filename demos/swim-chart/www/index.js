// import * as swim_charts from "@swim/chart";
// import * as swim_mesh from "@swim/mesh";
// import * as swim_view from "@swim/view";
// import * as swim_color from "@swim/color";

// let link = null;
// let chart = null;
// let plot = null;

// link = swim_mesh
//   .nodeRef("ws://127.0.0.1:9001/", "/unit/foo")
//   .downlinkMap()
//   .laneUri("random")
//   .didUpdate((time, newValue, _oldValue) => {
//     plot.insertDatum({ x: time.numberValue(), y: newValue.numberValue(), opacity: 1 });
//   })
//   .didRemove((time, _value) => {
//     plot.removeDatum(time.numberValue());
//   })
//   .open();

// const chartPanel = new swim_view.HtmlAppView(
//   document.getElementById(`avgChart`)
// );
// const chartCanvas = chartPanel.append("canvas");
// const clr = "#fff";
// const lightColor = swim_color.Color.rgb(255, 210, 63);

// chart = new swim_charts.ChartView()
//   .bottomAxis("time")
//   .leftAxis("linear")
//   .bottomGesture(false)
//   .leftDomainPadding([0, 0])
//   .topGutter(0)
//   .bottomGutter(20)
//   .leftGutter(25)
//   .rightGutter(0)
//   .font('12px "Open Sans"')
//   .domainColor(clr)
//   .tickMarkColor(clr)
//   .textColor(clr);

// plot = new swim_charts.LineGraphView().strokeWidth(2);

// plot.stroke(lightColor);
// chart.addPlot(plot);
// chartCanvas.append(chart);

import * as swim_charts from "@swim/chart";
import * as swim_mesh from "@swim/mesh";
import * as swim_view from "@swim/view";
import * as swim_color from "@swim/color";
import * as swim_wasm from "swim-wasm";

let chart = null;
let plot = null;

const swim_client = new swim_wasm.RustClient(
  // On sync
  (records) => {
    records.forEach((e) => {
      plot.insertDatum({
        x: e.key,
        y: e.value,
        opacity: 1,
      });
    })
  },
  // On update
  (e) => {
    plot.insertDatum({
      x: e.key,
      y: e.value,
      opacity: 1,
    });
  },
  // Did remove
  (key) => {
    plot.removeDatum(parseInt(key));
  }
);

const chartPanel = new swim_view.HtmlAppView(
  document.getElementById(`avgChart`)
);
const chartCanvas = chartPanel.append("canvas");
const clr = "#fff";
const lightColor = swim_color.Color.rgb(255, 210, 63);

chart = new swim_charts.ChartView()
  .bottomAxis("time")
  .leftAxis("linear")
  .bottomGesture(false)
  .leftDomainPadding([0, 0])
  .topGutter(0)
  .bottomGutter(20)
  .leftGutter(25)
  .rightGutter(0)
  .font('12px "Open Sans"')
  .domainColor(clr)
  .tickMarkColor(clr)
  .textColor(clr);

plot = new swim_charts.LineGraphView().strokeWidth(2);

plot.stroke(lightColor);
chart.addPlot(plot);
chartCanvas.append(chart);
