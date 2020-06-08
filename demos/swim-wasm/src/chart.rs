use plotters::prelude::*;
use web_sys::HtmlCanvasElement;

use crate::DataPoint;
use plotters::data::fitting_range;

pub fn draw(element: HtmlCanvasElement, data: Vec<DataPoint>) {
    let time_min = data.iter().map(|dp| dp.duration).min().unwrap();
    let time_max = data.iter().map(|dp| dp.duration).max().unwrap();

    let backend = CanvasBackend::with_canvas_object(element).unwrap();
    let root = backend.into_drawing_area();
    let font: FontDesc = ("sans-serif", 20.0).into();

    root.fill(&WHITE).unwrap();

    let data_range: Vec<f64> = data.iter().map(|dp| dp.data).collect();

    let mut chart = ChartBuilder::on(&root)
        .caption("Average over time", font)
        .build_ranged(time_max..time_min, fitting_range(data_range.iter()))
        .expect("Failed to build chart axis");

    chart
        .configure_mesh()
        .x_labels(20)
        .y_labels(10)
        .x_label_formatter(&|v| format!("{:.1}", v))
        .y_label_formatter(&|v| format!("{:.1}", v))
        .draw()
        .unwrap();

    chart
        .draw_series(LineSeries::new(
            data.iter().map(|dp| (dp.duration, dp.data)),
            &RED,
        ))
        .expect("Failed to draw series");

    root.present().expect("Failed to draw chart");
}
