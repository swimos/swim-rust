import { Chart } from "swim-wasm";

const canvas = document.getElementById("canvas");

let chart = null;

setupUI();
setupCanvas();

function setupUI() {
    window.addEventListener("resize", setupCanvas);
}

function setupCanvas() {
    const dpr = window.devicePixelRatio || 1;
    const aspectRatio = canvas.width / canvas.height;
    const size = Math.min(canvas.width, canvas.parentNode.offsetWidth);

    canvas.style.width = size + "px";
    canvas.style.height = size / aspectRatio + "px";
    canvas.width = size * dpr;
    canvas.height = size / aspectRatio * dpr;
    canvas.getContext("2d").scale(dpr, dpr);

    setupChart();
}

function setupChart() {
    Chart.init(canvas);
}
