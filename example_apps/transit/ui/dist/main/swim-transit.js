(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('@swimos/length'), require('@swimos/view'), require('@swimos/typeset'), require('@swimos/pie'), require('@swimos/color'), require('@swimos/transition'), require('mapbox-gl'), require('@swimos/mapbox'), require('@swimos/map'), require('@swimos/chart')) :
        typeof define === 'function' && define.amd ? define(['exports', '@swimos/length', '@swimos/view', '@swimos/typeset', '@swimos/pie', '@swimos/color', '@swimos/transition', 'mapbox-gl', '@swimos/mapbox', '@swimos/map', '@swimos/chart'], factory) :
            (global = global || self, factory((global.swim = global.swim || {}, global.swim.transit = global.swim.transit || {}), global.swim, global.swim, global.swim, global.swim, global.swim, global.swim, global.mapboxgl, global.swim, global.swim, global.swim));
}(this, function (exports, length, view, typeset, pie, color, transition, mapboxgl, mapbox, map, chart) {
    'use strict';

    /*! *****************************************************************************
    Copyright (c) Microsoft Corporation.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose with or without fee is hereby granted.

    THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
    REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
    AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
    INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
    LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
    OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
    PERFORMANCE OF THIS SOFTWARE.
    ***************************************************************************** */
    /* global Reflect, Promise */

    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({__proto__: []} instanceof Array && function (d, b) {
                d.__proto__ = b;
            }) ||
            function (d, b) {
                for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
            };
        return extendStatics(d, b);
    };

    function __extends(d, b) {
        extendStatics(d, b);

        function __() {
            this.constructor = d;
        }

        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    }

    function __decorate(decorators, target, key, desc) {
        var c = arguments.length,
            r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
        if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
        else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
        return c > 3 && r && Object.defineProperty(target, key, r), r;
    }

    var KpiViewController = (function (_super) {
        __extends(KpiViewController, _super);

        function KpiViewController() {
            var _this = _super.call(this) || this;
            _this._updateTimer = 0;
            return _this;
        }

        Object.defineProperty(KpiViewController.prototype, "kpiTitle", {
            get: function () {
                return this._kpiTitle;
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(KpiViewController.prototype, "pieTitle", {
            get: function () {
                return this._pieTitle;
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(KpiViewController.prototype, "pieView", {
            get: function () {
                return this._pieView;
            },
            enumerable: false,
            configurable: true
        });
        KpiViewController.prototype.didSetView = function (view) {
            var primaryColor = this.primaryColor;
            view.display("flex")
                .flexDirection("column")
                .padding(8)
                .fontFamily("\"Open Sans\", sans-serif")
                .fontSize(12);
            var header = view.append("div")
                .display("flex")
                .justifyContent("space-between")
                .textTransform("uppercase")
                .color(primaryColor);
            var headerLeft = header.append("div");
            this._kpiTitle = headerLeft.append("span").display("block").text(" -- ");
            var headerRight = header.append("div");
            headerRight.append("span").text("Real-Time");
            var body = view.append("div").key("body").position("relative").flexGrow(1).width("100%");
            var bodyCanvas = body.append("canvas").key("canvas");
            this._pieView = new pie.PieView()
                .key("pie")
                .innerRadius(length.Length.pct(34))
                .outerRadius(length.Length.pct(40))
                .cornerRadius(length.Length.pct(50))
                .tickRadius(length.Length.pct(45))
                .font("12px \"Open Sans\", sans-serif")
                .textColor(primaryColor);
            bodyCanvas.append(this._pieView);
            this._pieTitle = new typeset.TextRunView()
                .font("36px \"Open Sans\", sans-serif")
                .textColor(primaryColor);
            this._pieView.title(this._pieTitle);
        };
        KpiViewController.prototype.viewDidMount = function (view) {
            this._updateTimer = setInterval(this.updateKpi.bind(this), 1000);
            this.updateKpi();
        };
        KpiViewController.prototype.viewWillUnmount = function (view) {
            clearInterval(this._updateTimer);
            this._updateTimer = 0;
        };
        return KpiViewController;
    }(view.HtmlViewController));

    var Kpi1ViewController = (function (_super) {
        __extends(Kpi1ViewController, _super);

        function Kpi1ViewController(nodeRef, transitMapView) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._transitMapView = transitMapView;
            return _this;
        }

        Object.defineProperty(Kpi1ViewController.prototype, "primaryColor", {
            get: function () {
                return color.Color.parse("#00a6ed");
            },
            enumerable: false,
            configurable: true
        });
        Kpi1ViewController.prototype.updateKpi = function () {
            this.kpiTitle.text('speed (km/h)');
        };
        return Kpi1ViewController;
    }(KpiViewController));

    var Kpi2ViewController = (function (_super) {
        __extends(Kpi2ViewController, _super);

        function Kpi2ViewController(nodeRef, trafficMapView) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._trafficMapView = trafficMapView;
            return _this;
        }

        Object.defineProperty(Kpi2ViewController.prototype, "primaryColor", {
            get: function () {
                return color.Color.parse("#00a6ed");
            },
            enumerable: false,
            configurable: true
        });
        Kpi2ViewController.prototype.updateKpi = function () {
            this.kpiTitle.text('speed (km/h)');
        };
        Kpi2ViewController.prototype.updateData = function (key, value) {
            var sliceColors = [color.Color.parse("#00a6ed"), color.Color.parse("#7ed321"),
                color.Color.parse("#c200fb"), color.Color.parse("#50e3c2"),
                color.Color.parse("#57b8ff"), color.Color.parse("#5aff15"),
                color.Color.parse("#55dde0"), color.Color.parse("#f7aef8")];
            var tween = transition.Transition.duration(1000);
            var id = key.get("id").stringValue() || '';
            var index = key.get("index").numberValue() || 0;
            var sliceColor = sliceColors[index % 8];
            var sliceValue = value.numberValue() || 0;
            var pie$1 = this.pieView;
            if (sliceValue > 0) {
                var slice = pie$1.getChildView(id);
                if (slice) {
                    slice.value(sliceValue, tween);
                } else {
                    slice = new pie.SliceView()
                        .value(sliceValue)
                        .sliceColor(sliceColor)
                        .label(sliceValue.toFixed())
                        .legend(id);
                    pie$1.setChildView(id, slice);
                }
            }
        };
        Kpi2ViewController.prototype.linkData = function () {
            this._nodeRef.downlinkMap()
                .nodeUri("/state/US/S-CA")
                .laneUri("agencySpeed")
                .didUpdate(this.updateData.bind(this))
                .open();
        };
        return Kpi2ViewController;
    }(KpiViewController));

    var Kpi3ViewController = (function (_super) {
        __extends(Kpi3ViewController, _super);

        function Kpi3ViewController(nodeRef, trafficMapView) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._trafficMapView = trafficMapView;
            return _this;
        }

        Object.defineProperty(Kpi3ViewController.prototype, "primaryColor", {
            get: function () {
                return color.Color.parse("#00a6ed");
            },
            enumerable: false,
            configurable: true
        });
        Kpi3ViewController.prototype.updateKpi = function () {
            this.kpiTitle.text('Total Count');
        };
        Kpi3ViewController.prototype.updateData = function (k, v) {
        };
        Kpi3ViewController.prototype.linkData = function () {
            this._nodeRef.downlinkMap()
                .laneUri("count")
                .didUpdate(this.updateData.bind(this))
                .open();
        };
        return Kpi3ViewController;
    }(KpiViewController));

    var TransitMapView = (function (_super) {
        __extends(TransitMapView, _super);

        function TransitMapView() {
            var _this = _super.call(this) || this;
            _this.agencyMarkerColor.setState(color.Color.parse("#5aff15"));
            _this.vehicleMarkerColor.setState(color.Color.parse("#00a6ed"));
            return _this;
        }

        Object.defineProperty(TransitMapView.prototype, "viewController", {
            get: function () {
                return this._viewController;
            },
            enumerable: false,
            configurable: true
        });
        __decorate([
            view.MemberAnimator(color.Color)
        ], TransitMapView.prototype, "agencyMarkerColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color)
        ], TransitMapView.prototype, "vehicleMarkerColor", void 0);
        return TransitMapView;
    }(map.MapGraphicView));

    var AgencyMapView = (function (_super) {
        __extends(AgencyMapView, _super);

        function AgencyMapView() {
            return _super !== null && _super.apply(this, arguments) || this;
        }

        Object.defineProperty(AgencyMapView.prototype, "viewController", {
            get: function () {
                return this._viewController;
            },
            enumerable: false,
            configurable: true
        });
        __decorate([
            view.MemberAnimator(color.Color, {inherit: true})
        ], AgencyMapView.prototype, "agencyMarkerColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color, {inherit: true})
        ], AgencyMapView.prototype, "vehicleMarkerColor", void 0);
        return AgencyMapView;
    }(map.MapGraphicView));

    var AgencyPopoverViewController = (function (_super) {
        __extends(AgencyPopoverViewController, _super);

        function AgencyPopoverViewController(info, nodeRef) {
            var _this = _super.call(this) || this;
            _this._info = info;
            _this._nodeRef = nodeRef;
            return _this;
        }

        AgencyPopoverViewController.prototype.didSetView = function (view) {
            view.width(240)
                .height(360)
                .borderRadius(5)
                .padding(10)
                .backgroundColor(color.Color.parse("#071013").alpha(0.9))
                .backdropFilter("blur(2px)");
            var agency = this._info;
            var container = view.append("div").color("#ffffff");
            container.append("span").key("name").text("" + agency.id);
        };
        return AgencyPopoverViewController;
    }(view.PopoverViewController));

    var VehicleMapView = (function (_super) {
        __extends(VehicleMapView, _super);

        function VehicleMapView() {
            var _this = _super.call(this) || this;
            _this.fill.setState(color.Color.transparent());
            _this.radius.setState(length.Length.px(5));
            return _this;
        }

        Object.defineProperty(VehicleMapView.prototype, "viewController", {
            get: function () {
                return this._viewController;
            },
            enumerable: false,
            configurable: true
        });
        __decorate([
            view.MemberAnimator(color.Color, {inherit: true})
        ], VehicleMapView.prototype, "vehicleMarkerColor", void 0);
        return VehicleMapView;
    }(map.MapCircleView));

    var VehiclePopoverViewController = (function (_super) {
        __extends(VehiclePopoverViewController, _super);

        function VehiclePopoverViewController(info, nodeRef) {
            var _this = _super.call(this) || this;
            _this._info = info;
            _this._nodeRef = nodeRef;
            _this._colorRage = [
                '#00A6ED',
                '#7ED321',
                '#57B8FF',
                '#50E3C2',
                '#C200FB',
                '#5AFF15',
                '#55DDE0',
                '#F7AEF8'
            ];
            return _this;
        }

        VehiclePopoverViewController.prototype.didSetView = function (view) {
            view.width(240)
                .height(360)
                .borderRadius(5)
                .padding(10)
                .backgroundColor(color.Color.parse("#071013").alpha(0.9))
                .backdropFilter("blur(2px)");
            var vehicle = this._info;
            var agencyIndex = vehicle.index % 8;
            var colorAgency = this._colorRage[agencyIndex];
            var busPopover = view.append("div").color(color.Color.parse("#ffffff").alpha(0.9));
            busPopover.className('busPopover');
            var placardHeader = busPopover.append("div").color(colorAgency);
            placardHeader.className('placardHeader');
            var ledIcon = placardHeader.append("div");
            ledIcon.className('ledIcon');
            ledIcon.backgroundColor(colorAgency);
            var ledLabel = ledIcon.append("h3").text(vehicle.routeTag);
            ledLabel.className('ledLabel');
            var placardLabel = placardHeader.append('h2');
            placardLabel.className('placardLabel');
            placardLabel.text("bus #".concat(vehicle.id));
            var popoverMeter = placardHeader.append("div");
            popoverMeter.className("popover-meter");
            popoverMeter.borderColor(colorAgency);
            var meterFill = popoverMeter.append("div");
            meterFill.className('fill');
            meterFill.backgroundColor(colorAgency)
                .height("".concat((vehicle.speed / 130) * 100, "%"));
            busPopover.append("div")
                .className("placardSubheader");
            var placardSubheader = busPopover.append("div");
            placardSubheader.className("placardSubheader");
            this._speedItem = placardSubheader.append("div")
                .text("".concat(vehicle.speed, " km/h"))
                .backgroundColor(colorAgency);
            this._speedItem.className("placardSubheaderItem");
            placardSubheader.append("div")
                .text("".concat(vehicle.dirId))
                .backgroundColor(colorAgency)
                .className("placardSubheaderItem");
            placardSubheader.append("div")
                .text("".concat(vehicle.heading))
                .backgroundColor(colorAgency)
                .className("placardSubheaderItem");
            busPopover.append("div").text(vehicle.routeTitle)
                .className("placard-route");
            busPopover.append("div").text(vehicle.agency)
                .paddingTop(10)
                .className("placard-agency");
            var chartsContainer = busPopover.append("div");
            chartsContainer.className('busCharts');
            var speedChartTitle = chartsContainer.append("h3").text("Speed");
            speedChartTitle.className("chartTitle");
            var speedChartContainer = chartsContainer.append("div");
            speedChartContainer.className('chartContainer');
            var speedChartCanvas = speedChartContainer.append("canvas").height(200);
            this._speedChart = new chart.ChartView()
                .bottomAxis("time")
                .leftAxis("linear")
                .bottomGesture(false)
                .leftDomainPadding([0.1, 0.1])
                .topGutter(0)
                .rightGutter(0)
                .bottomGutter(0)
                .leftGutter(-1)
                .domainColor(color.Color.transparent(0))
                .tickMarkColor(color.Color.transparent(0));
            speedChartCanvas.append(this._speedChart);
            this._speedPlot = new chart.LineGraphView()
                .stroke(colorAgency)
                .strokeWidth(1);
            this._speedChart.addPlot(this._speedPlot);
            var accelChartTitle = chartsContainer.append("h3").text("Acceleration");
            accelChartTitle.className("chartTitle");
            var accelChartContainer = chartsContainer.append("div");
            accelChartContainer.className('chartContainer');
            var accelChartCanvas = accelChartContainer.append("canvas").height(200);
            var accelerationChart = new chart.ChartView()
                .bottomAxis("time")
                .leftAxis("linear")
                .bottomGesture(false)
                .leftDomainPadding([0.1, 0.1])
                .topGutter(0)
                .rightGutter(0)
                .bottomGutter(0)
                .leftGutter(-1)
                .domainColor(color.Color.transparent(0))
                .tickMarkColor(color.Color.transparent(0));
            accelChartCanvas.append(accelerationChart);
            this._accelerationPlot = new chart.LineGraphView()
                .stroke(colorAgency)
                .strokeWidth(1);
            accelerationChart.addPlot(this._accelerationPlot);
        };
        VehiclePopoverViewController.prototype.popoverDidShow = function (view) {
            this.linkSpeedHistory();
            this.linkAccelHistory();
        };
        VehiclePopoverViewController.prototype.popoverDidHide = function (view) {
            this.unlinkSpeedHistory();
            this.unlinkAccelHistory();
        };
        VehiclePopoverViewController.prototype.linkSpeedHistory = function () {
            if (!this._linkSpeedHistory) {
                this._linkSpeedHistory = this._nodeRef.downlinkMap()
                    .nodeUri(this._info.uri)
                    .laneUri("speeds")
                    .didUpdate(this.didUpdateSpeedHistory.bind(this))
                    .didRemove(this.didRemoveSpeedHistory.bind(this))
                    .open();
            }
        };
        VehiclePopoverViewController.prototype.unlinkSpeedHistory = function () {
            if (this._linkSpeedHistory) {
                this._linkSpeedHistory.close();
                this._linkSpeedHistory = undefined;
            }
        };
        VehiclePopoverViewController.prototype.didUpdateSpeedHistory = function (k, v) {
            this._speedItem.text("".concat(v.numberValue(), " km/h"));
            this._speedPlot.insertDatum({x: k.numberValue(), y: v.numberValue()});
        };
        VehiclePopoverViewController.prototype.didRemoveSpeedHistory = function (k, v) {
            this._speedPlot.removeDatum({x: k.numberValue(), y: v.numberValue()});
        };
        VehiclePopoverViewController.prototype.linkAccelHistory = function () {
            if (!this._linkAccelHistory) {
                this._linkAccelHistory = this._nodeRef.downlinkMap()
                    .nodeUri(this._info.uri)
                    .laneUri("accelerations")
                    .didUpdate(this.didUpdateAccelHistory.bind(this))
                    .didRemove(this.didRemoveAccelHistory.bind(this))
                    .open();
            }
        };
        VehiclePopoverViewController.prototype.unlinkAccelHistory = function () {
            if (this._linkAccelHistory) {
                this._linkAccelHistory.close();
                this._linkAccelHistory = undefined;
            }
        };
        VehiclePopoverViewController.prototype.didUpdateAccelHistory = function (k, v) {
            this._accelerationPlot.insertDatum({x: k.numberValue(), y: v.numberValue()});
        };
        VehiclePopoverViewController.prototype.didRemoveAccelHistory = function (k, v) {
            this._accelerationPlot.removeDatum({x: k.numberValue(), y: v.numberValue()});
        };
        return VehiclePopoverViewController;
    }(view.PopoverViewController));

    var VehicleMapViewController = (function (_super) {
        __extends(VehicleMapViewController, _super);

        function VehicleMapViewController(info, nodeRef) {
            var _this = _super.call(this) || this;
            _this._info = info;
            _this._nodeRef = nodeRef;
            return _this;
        }

        VehicleMapViewController.prototype.setInfo = function (info) {
            this._info = info;
            this.updateVehicle();
        };
        VehicleMapViewController.prototype.updateVehicle = function () {
            var view = this._view;
            var info = this._info;
            if (info.longitude && info.latitude) {
                var oldCenter = view.center.value;
                var newCenter = new map.LngLat(info.longitude, info.latitude);
                if (!oldCenter.equals(newCenter)) {
                    view.center.setState(newCenter, transition.Transition.duration(10000));
                    this.ripple(this._view.vehicleMarkerColor.value);
                }
            }
            view.fill(view.vehicleMarkerColor.value, transition.Transition.duration(500));
        };
        VehicleMapViewController.prototype.didSetView = function (view) {
            view.on("click", this.onClick.bind(this));
            var info = this._info;
            if (info.longitude && info.latitude) {
                this._view.center.setState(new map.LngLat(info.longitude, info.latitude));
            }
            view.fill(view.vehicleMarkerColor.value);
        };
        VehicleMapViewController.prototype.ripple = function (color) {
            var info = this._info;
            if (document.hidden || this.culled) {
                return;
            }
            var ripple = new map.MapCircleView()
                .center(new map.LngLat(info.longitude, info.latitude))
                .radius(0)
                .fill(null)
                .stroke(color.alpha(1))
                .strokeWidth(1);
            this.appendChildView(ripple);
            var radius = Math.min(this.bounds.width, this.bounds.height) / 8;
            var tween = transition.Transition.duration(5000);
            ripple.stroke(color.alpha(0), tween)
                .radius(radius, tween.onEnd(function () {
                    ripple.remove();
                }));
        };
        VehicleMapViewController.prototype.onClick = function (event) {
            event.stopPropagation();
            if (!this._popoverView) {
                this._popoverView = new view.PopoverView();
                var popoverViewController = new VehiclePopoverViewController(this._info, this._nodeRef);
                this._popoverView.setViewController(popoverViewController);
                this._popoverView.setSource(this._view);
                this._popoverView.hidePopover();
            }
            this.appView.togglePopover(this._popoverView, {multi: event.altKey});
        };
        return VehicleMapViewController;
    }(map.MapGraphicViewController));

    var VEHICLE_ZOOM = 11;
    var AgencyMapViewController = (function (_super) {
        __extends(AgencyMapViewController, _super);

        function AgencyMapViewController(info, nodeRef) {
            var _this = _super.call(this) || this;
            _this._info = info;
            _this._nodeRef = nodeRef;
            _this._boundingBoxLink = null;
            _this._vehiclesLink = null;
            _this._popoverView = null;
            return _this;
        }

        AgencyMapViewController.prototype.didSetBoundingBox = function (value) {
            if (this._reduced === true) {
                this.ripple(this._view.agencyMarkerColor.value);
            }
        };
        AgencyMapViewController.prototype.didUpdateVehicle = function (key, value) {
            var vehicleInfo = value.toAny();
            var vehicleId = "" + vehicleInfo.id;
            vehicleInfo.agencyInfo = this._info;
            var vehicleMapView = this.getChildView(vehicleId);
            if (!vehicleMapView) {
                var vehicleNodeUri = key.stringValue();
                var vehicleNodeRef = this._nodeRef.nodeRef(vehicleNodeUri);
                vehicleMapView = new VehicleMapView();
                var vehcileMapViewController = new VehicleMapViewController(vehicleInfo, vehicleNodeRef);
                vehicleMapView.setViewController(vehcileMapViewController);
                this.setChildView(vehicleId, vehicleMapView);
            } else {
                var vehicleMapViewController = vehicleMapView.viewController;
                vehicleMapViewController.setInfo(vehicleInfo);
            }
        };
        AgencyMapViewController.prototype.initMarkerView = function () {
            var markerView = this.getChildView("marker");
            if (!markerView && this._boundingBoxLink) {
                var boundingBox = this._boundingBoxLink.get().toAny();
                if (boundingBox) {
                    var lng = (boundingBox.minLng + boundingBox.maxLng) / 2;
                    var lat = (boundingBox.minLat + boundingBox.maxLat) / 2;
                    markerView = new map.MapCircleView();
                    markerView.center.setState(new map.LngLat(lng, lat));
                    markerView.radius.setState(length.Length.px(10));
                    markerView.fill.setState(this._view.agencyMarkerColor.value);
                    markerView.on("click", this.onMarkerClick.bind(this));
                    this.setChildView("marker", markerView);
                }
            }
        };
        AgencyMapViewController.prototype.onMarkerClick = function (event) {
            event.stopPropagation();
            if (!this._popoverView) {
                this._popoverView = new view.PopoverView();
                var popoverViewController = new AgencyPopoverViewController(this._info, this._nodeRef);
                this._popoverView.setViewController(popoverViewController);
                this._popoverView.hidePopover();
            }
            this._popoverView.setSource(this.getChildView("marker"));
            this.appView.togglePopover(this._popoverView, {multi: event.altKey});
        };
        AgencyMapViewController.prototype.ripple = function (color) {
            if (document.hidden || this.culled || !this._boundingBoxLink) {
                return;
            }
            var boundingBox = this._boundingBoxLink.get().toAny();
            var lng = (boundingBox.minLng + boundingBox.maxLng) / 2;
            var lat = (boundingBox.minLat + boundingBox.maxLat) / 2;
            var ripple = new map.MapCircleView()
                .center(new map.LngLat(lng, lat))
                .radius(0)
                .fill(null)
                .stroke(color.alpha(0.25))
                .strokeWidth(1);
            this.appendChildView(ripple);
            var radius = Math.min(this.bounds.width, this.bounds.height) / 8;
            var tween = transition.Transition.duration(2000);
            ripple.stroke(color.alpha(0), tween)
                .radius(radius, tween.onEnd(function () {
                    ripple.remove();
                }));
        };
        AgencyMapViewController.prototype.viewDidMount = function (view) {
            this.linkBoundingBox();
        };
        AgencyMapViewController.prototype.viewWillUnmount = function (view) {
            this.unlinkBoundingBox();
            this.unlinkVehicles();
        };
        AgencyMapViewController.prototype.viewDidProject = function (viewContext, view) {
            var boundingBox = this._boundingBoxLink.get().toAny();
            if (boundingBox) {
                var _a = viewContext.projection.bounds, sw = _a[0], ne = _a[1];
                var culled = !(boundingBox.minLng <= ne.lng && sw.lng <= boundingBox.maxLng
                    && boundingBox.minLat <= ne.lat && sw.lat <= boundingBox.maxLat);
                view.setCulled(culled);
            } else {
                view.setCulled(true);
            }
            this.updateLevelOfDetail();
        };
        AgencyMapViewController.prototype.updateLevelOfDetail = function () {
            if (this._reduced !== false && !this.culled && this.zoom >= VEHICLE_ZOOM) {
                this._reduced = false;
                this.removeAll();
                this.linkVehicles();
            } else if (this._reduced !== true && !this.culled && this.zoom < VEHICLE_ZOOM) {
                this._reduced = true;
                this.unlinkVehicles();
                this.removeAll();
                this.initMarkerView();
            }
        };
        AgencyMapViewController.prototype.linkBoundingBox = function () {
            if (!this._boundingBoxLink) {
                this._boundingBoxLink = this._nodeRef.downlinkValue()
                    .laneUri("boundingBox")
                    .didSet(this.didSetBoundingBox.bind(this))
                    .open();
            }
        };
        AgencyMapViewController.prototype.unlinkBoundingBox = function () {
            if (this._boundingBoxLink) {
                this._boundingBoxLink.close();
                this._boundingBoxLink = null;
            }
        };
        AgencyMapViewController.prototype.linkVehicles = function () {
            if (!this._vehiclesLink) {
                this._vehiclesLink = this._nodeRef.downlinkMap()
                    .laneUri("vehicles")
                    .didUpdate(this.didUpdateVehicle.bind(this))
                    .open();
            }
        };
        AgencyMapViewController.prototype.unlinkVehicles = function () {
            if (this._vehiclesLink) {
                this._vehiclesLink.close();
                this._vehiclesLink = null;
            }
        };
        return AgencyMapViewController;
    }(map.MapGraphicViewController));

    var TransitMapViewController = (function (_super) {
        __extends(TransitMapViewController, _super);

        function TransitMapViewController(nodeRef) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._agenciesLink = null;
            return _this;
        }

        TransitMapViewController.prototype.viewDidMount = function (view) {
            this.linkAgencies();
        };
        TransitMapViewController.prototype.viewWillUnmount = function (view) {
            this.unlinkAgencies();
        };
        TransitMapViewController.prototype.linkAgencies = function () {
            if (!this._agenciesLink) {
                this._agenciesLink = this._nodeRef.downlinkMap()
                    .laneUri("agencies")
                    .didUpdate(this.didUpdateAgency.bind(this))
                    .open();
            }
        };
        TransitMapViewController.prototype.unlinkAgencies = function () {
            if (this._agenciesLink) {
                this._agenciesLink.close();
                this._agenciesLink = null;
            }
        };
        TransitMapViewController.prototype.didUpdateAgency = function (key, value) {
            var agencyInfo = value.toAny();
            var agencyId = "" + agencyInfo.id;
            var agencyMapView = this.getChildView(agencyId);
            if (!agencyMapView) {
                var agencyNodeUri = key.stringValue();
                var agencyNodeRef = this._nodeRef.nodeRef(agencyNodeUri);
                agencyMapView = new AgencyMapView();
                var agencyMapViewController = new AgencyMapViewController(agencyInfo, agencyNodeRef);
                agencyMapView.setViewController(agencyMapViewController);
                this.setChildView(agencyId, agencyMapView);
            }
        };
        return TransitMapViewController;
    }(map.MapGraphicViewController));

    var TransitViewController = (function (_super) {
        __extends(TransitViewController, _super);

        function TransitViewController(nodeRef) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._map = null;
            _this._canvasView = null;
            return _this;
        }

        Object.defineProperty(TransitViewController.prototype, "canvasView", {
            get: function () {
                return this._canvasView;
            },
            enumerable: false,
            configurable: true
        });
        TransitViewController.prototype.didSetView = function (view) {
            this._map = new mapboxgl.Map({
                container: view.node,
                style: "mapbox://styles/swimit/cjs5h20wh0fyf1gocidkpmcvm",
                center: {lng: -97.922211, lat: 39.381266},
                pitch: 45,
                zoom: 4,
            });
            var mapboxView = new mapbox.MapboxView(this._map);
            this._canvasView = mapboxView.overlayCanvas();
            var transitMapView = new TransitMapView();
            var transitMapViewController = new TransitMapViewController(this._nodeRef);
            transitMapView.setViewController(transitMapViewController);
            mapboxView.setChildView("map", transitMapView);
            var header = view.append("div")
                .key("header")
                .pointerEvents("none")
                .zIndex(10);
            var logo = header.append("div")
                .key("logo")
                .position("absolute")
                .left(8)
                .top(8)
                .width(156)
                .height(68);
            logo.append(this.createLogo());
        };
        TransitViewController.prototype.viewDidResize = function () {
        };
        TransitViewController.prototype.createKpiStack = function (transitMapView) {
            var kpiStack = view.HtmlView.create("div")
                .key("kpiStack")
                .position("absolute")
                .right(0)
                .top(0)
                .bottom(0)
                .zIndex(9)
                .pointerEvents("none");
            var vehicleFlowKpi = kpiStack.append("div")
                .key("vehicleFlowKpi")
                .position("absolute")
                .borderRadius(8)
                .boxSizing("border-box")
                .backgroundColor(color.Color.parse("#070813").alpha(0.33))
                .backdropFilter("blur(2px)")
                .pointerEvents("auto");
            var vehicleFlowKpiViewController = new Kpi1ViewController(this._nodeRef, transitMapView);
            vehicleFlowKpi.setViewController(vehicleFlowKpiViewController);
            var vehicleBackupKpi = kpiStack.append("div")
                .key("vehicleBackupKpi")
                .position("absolute")
                .borderRadius(8)
                .boxSizing("border-box")
                .backgroundColor(color.Color.parse("#070813").alpha(0.33))
                .backdropFilter("blur(2px)")
                .pointerEvents("auto");
            var vehicleBackupKpiViewController = new Kpi2ViewController(this._nodeRef, transitMapView);
            vehicleBackupKpi.setViewController(vehicleBackupKpiViewController);
            var pedestrianBackupKpi = kpiStack.append("div")
                .key("pedestrianBackupKpi")
                .position("absolute")
                .borderRadius(8)
                .boxSizing("border-box")
                .backgroundColor(color.Color.parse("#070813").alpha(0.33))
                .backdropFilter("blur(2px)")
                .pointerEvents("auto");
            var pedestrianBackupKpiViewController = new Kpi3ViewController(this._nodeRef, transitMapView);
            pedestrianBackupKpi.setViewController(pedestrianBackupKpiViewController);
            return kpiStack;
        };
        TransitViewController.prototype.layoutKpiStack = function () {
            var kpiMargin = 16;
            var view = this._view;
            var kpiStack = view.getChildView("kpiStack");
            var kpiViews = kpiStack.childViews;
            var kpiViewCount = kpiViews.length;
            var kpiViewHeight = (view.node.offsetHeight - kpiMargin * (kpiViewCount + 1)) / (kpiViewCount || 1);
            var kpiViewWidth = 1.5 * kpiViewHeight;
            var kpiStackWidth = kpiViewWidth + 2 * kpiMargin;
            kpiStack.width(kpiStackWidth);
            for (var i = 0; i < kpiViewCount; i += 1) {
                var kpiView = kpiViews[i];
                kpiView.right(kpiMargin)
                    .top(kpiViewHeight * i + kpiMargin * (i + 1))
                    .width(kpiViewWidth)
                    .height(kpiViewHeight);
            }
            if (kpiStackWidth > 240 && view.node.offsetWidth >= 2 * kpiStackWidth) {
                kpiStack.display("block");
            } else {
                kpiStack.display("none");
            }
        };
        TransitViewController.prototype.createLogo = function () {
            var logo = view.SvgView.create("svg").width(156).height(68).viewBox("0 0 156 68");
            logo.append("polygon").fill("#008ac5").points("38.262415 60.577147 45.7497674 67.9446512 41.4336392 66.3395349");
            logo.append("polygon").fill("#004868").points("30.6320304 56.7525259 35.7395349 55.9824716 41.4304178 66.3390957");
            logo.append("polygon").fill("#1db0ef").points("45.8577521 43.5549215 35.7395349 55.9813953 30.895331 54.807418");
            logo.append("polygon").fill("#008ac5").points("45.8604651 43.5549215 41.760398 48.5902277 50.5169298 42.7788873");
            logo.append("polygon").fill("#008ac5").points("26.3271825 57.4036063 35.7395349 55.9813953 19.2251161 51.7639132");
            logo.append("polygon").fill("#008ac5").points("21.8522892 56.6610604 26.3302326 57.4059108 24.8674419 60.4330233");
            logo.append("polygon").fill("#46c7ff").points("25.8602519 50.1512126 21.9749543 52.4673682 26.296682 53.5765239");
            logo.append("polygon").fill("#004868").points("8.22304588 54.4 26.3302326 57.4046512 10.7431892 45.0311035");
            logo.append("polygon").fill("#008ac5").points("13.8293387 33.5634807 4.10372093 35.5972093 8.22325581 54.4");
            logo.append("polygon").fill("#004868").points("8.22099482 54.4011969 0.0941817913 35.6005706 4.10375191 35.6005706");
            logo.append("polygon").fill("#004a6a").points("29.0049687 26.1913296 29.4164365 29.8907151 13.8293023 33.5651163 4.10372093 35.5972093");
            logo.append("polygon").fill("#46c7ff").points("29.0062121 26.1911555 4.10372093 35.5972093 18.5813833 12.4933152");
            logo.append("polygon").fill("#008ac5").points("11.2792478 19.5948645 18.5832775 12.4930233 4.10372093 35.5972093");
            logo.append("polygon").fill("#0076a9").points("0.0976972893 35.6005706 11.6715695 18.9158345 4.10403655 35.6005706");
            logo.append("polygon").fill("#46c7ff").points("24.2487055 31.1058385 29.4139535 29.8876265 26.2195349 45.5916279");
            logo.append("polygon").fill("#004868").points("34.5718675 26.2202296 36.2830885 37.7346648 31.9099624 28.1109897");
            logo.append("polygon").fill("#00557a").points("29.0034364 26.1884581 42.3712871 20.6741037 29.4139535 29.8883721");
            logo.append("polygon").fill("#008ac5").points("26.68 13.7746077 29.0062121 26.1911555 18.5813953 12.4930233");
            logo.append("polygon").fill("#004868").points("11.6715695 2.42510699 18.5775948 12.4930233 11.083087 19.8858773");
            logo.append("polygon").fill("#46c7ff").points("9.61188076 0 26.6831512 13.7746077 18.5788496 12.4908901");
            logo.append("path").fill("#0076a9").d("M26.6778731,13.7746077 L38.8232486,18.8943879 L37.5870837,22.6477526 L29.0035088,26.187907 L26.6778731,13.7746077 Z M35.3244186,20.5976744 C35.7065226,20.5976744 36.0162791,20.2879179 36.0162791,19.905814 C36.0162791,19.52371 35.7065226,19.2139535 35.3244186,19.2139535 C34.9423146,19.2139535 34.6325581,19.52371 34.6325581,19.905814 C34.6325581,20.2879179 34.9423146,20.5976744 35.3244186,20.5976744 Z");
            logo.append("polygon").fill("#46c7ff").points("38.8232558 18.9003694 64.0007929 11.9315264 37.5863839 22.647813");
            var text = logo.append("text").fill("#ffffff").fontFamily("Orbitron").fontSize(32);
            text.append("tspan").x(58).y(42).text("swim");
            return logo;
        };
        return TransitViewController;
    }(view.HtmlViewController));

    exports.Kpi1ViewController = Kpi1ViewController;
    exports.Kpi2ViewController = Kpi2ViewController;
    exports.Kpi3ViewController = Kpi3ViewController;
    exports.KpiViewController = KpiViewController;
    exports.TransitViewController = TransitViewController;

    Object.defineProperty(exports, '__esModule', {value: true});

}));
//# sourceMappingURL=swimos-transit.js.map
