import * as swim_charts from "@swim/chart";
import * as swim_mesh from "@swim/mesh";
import * as swim_view from "@swim/view";
import * as swim_color from "@swim/color";
import * as swim_wasm from "swim-wasm";

let link = null;
let chart = null;
let plot = null;

initialise();

async function initialise() {
  const swim_client = await new swim_wasm.RustClient(
    // On update
    (time, newValue, _oldValue) => {
    //   plot.insertDatum({
    //     x: time.numberValue(),
    //     y: newValue.numberValue(),
    //     opacity: 1,
    //   });
    },
    // Did remove
    (time, _value) => {
      plot.removeDatum(time.numberValue());
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
}
