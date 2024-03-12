(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('@swim/color'), require('@swim/view'), require('@swim/map'), require('@swim/length'), require('@swim/transition'), require('@swim/chart'), require('@swim/typeset'), require('@swim/gauge'), require('@swim/pie'), require('mapbox-gl'), require('@swim/mapbox')) :
    typeof define === 'function' && define.amd ? define(['exports', '@swim/color', '@swim/view', '@swim/map', '@swim/length', '@swim/transition', '@swim/chart', '@swim/typeset', '@swim/gauge', '@swim/pie', 'mapbox-gl', '@swim/mapbox'], factory) :
    (global = global || self, factory((global.swim = global.swim || {}, global.swim.traffic = global.swim.traffic || {}), global.swim, global.swim, global.swim, global.swim, global.swim, global.swim, global.swim, global.swim, global.swim, global.mapboxgl, global.swim));
}(this, function (exports, color, view, map, length, transition, chart, typeset, gauge, pie, mapboxgl, mapbox) { 'use strict';

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

    var extendStatics = function(d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };

    function __extends(d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    }

    function __decorate(decorators, target, key, desc) {
        var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
        if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
        else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
        return c > 3 && r && Object.defineProperty(target, key, r), r;
    }

    var TrafficMapView = (function (_super) {
        __extends(TrafficMapView, _super);
        function TrafficMapView() {
            var _this = _super.call(this) || this;
            _this.intersectionMarkerColor.setState(color.Color.parse("#00a6ed"));
            _this.pedestrianMarkerColor.setState(color.Color.parse("#c200fa"));
            _this.redLightColor.setState(color.Color.parse("#a50f21"));
            _this.yellowLightColor.setState(color.Color.parse("#fccf20"));
            _this.greenLightColor.setState(color.Color.parse("#54e218"));
            return _this;
        }
        Object.defineProperty(TrafficMapView.prototype, "viewController", {
            get: function () {
                return this._viewController;
            },
            enumerable: false,
            configurable: true
        });
        __decorate([
            view.MemberAnimator(color.Color)
        ], TrafficMapView.prototype, "intersectionMarkerColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color)
        ], TrafficMapView.prototype, "pedestrianMarkerColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color)
        ], TrafficMapView.prototype, "redLightColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color)
        ], TrafficMapView.prototype, "yellowLightColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color)
        ], TrafficMapView.prototype, "greenLightColor", void 0);
        return TrafficMapView;
    }(map.MapGraphicView));

    var IntersectionMapView = (function (_super) {
        __extends(IntersectionMapView, _super);
        function IntersectionMapView() {
            return _super !== null && _super.apply(this, arguments) || this;
        }
        Object.defineProperty(IntersectionMapView.prototype, "viewController", {
            get: function () {
                return this._viewController;
            },
            enumerable: false,
            configurable: true
        });
        IntersectionMapView.prototype.onCull = function () {
            _super.prototype.onCull.call(this);
            if (this._hitBounds === null) {
                this.setCulled(true);
            }
        };
        __decorate([
            view.MemberAnimator(color.Color, "inherit")
        ], IntersectionMapView.prototype, "intersectionMarkerColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color, "inherit")
        ], IntersectionMapView.prototype, "pedestrianMarkerColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color, "inherit")
        ], IntersectionMapView.prototype, "redLightColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color, "inherit")
        ], IntersectionMapView.prototype, "yellowLightColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color, "inherit")
        ], IntersectionMapView.prototype, "greenLightColor", void 0);
        return IntersectionMapView;
    }(map.MapGraphicView));

    var IntersectionPopoverViewController = (function (_super) {
        __extends(IntersectionPopoverViewController, _super);
        function IntersectionPopoverViewController(info, nodeRef) {
            var _this = _super.call(this) || this;
            _this._info = info;
            _this._nodeRef = nodeRef;
            _this._chartChildView = {};
            return _this;
        }
        IntersectionPopoverViewController.prototype.didSetView = function (view) {
            view.width(240)
                .height(360)
                .display('flex')
                .flexDirection('column')
                .borderRadius(5)
                .padding(10)
                .backgroundColor(color.Color.parse("#071013").alpha(0.9))
                .backdropFilter("blur(2px)")
                .color("#ffffff");
            var intersection = this._info;
            var header = view.append("header")
                .display('flex')
                .alignItems('center');
            header.append("div")
                .borderRadius(20)
                .backgroundColor('#00a6ed')
                .padding([3, 6, 3, 6])
                .marginRight(5)
                .fontSize(15)
                .color("#000000")
                .text("".concat(intersection.id));
            header.append("h2").key("name")
                .margin(0)
                .fontSize(15)
                .color("#00a6ed")
                .text(intersection.name);
            var status = view.append('ul')
                .display('flex')
                .alignItems('center')
                .padding(0)
                .textAlign('center')
                .color('#000000');
            this._latencyView = status.append('li')
                .display('inline-block')
                .width(50)
                .backgroundColor('#00a6ed')
                .fontSize(11)
                .lineHeight('1.5em')
                .borderRadius('20px')
                .marginRight(10)
                .text('746ms');
            this._latencyView.setStyle('list-style', 'none');
            this._modeView = status.append('li')
                .display('inline-block')
                .width(50)
                .backgroundColor('#00a6ed')
                .fontSize(11)
                .lineHeight('1.5em')
                .borderRadius('20px')
                .marginRight(10)
                .text('--');
            this._modeView.setStyle('list-style', 'none');
            this._contentView = view.append('div')
                .display('flex')
                .flexDirection('column')
                .flexGrow(1)
                .overflow('auto');
        };
        IntersectionPopoverViewController.prototype.popoverDidShow = function (view) {
            this.linkLatency();
            this.linkMode();
            this.linkPhase();
            this.linkHistory();
            this.linkFuture();
        };
        IntersectionPopoverViewController.prototype.popoverDidHide = function (view) {
            this.unlinkLatency();
            this.unlinkMode();
            this.unlinkPhase();
            this.unlinkHistory();
            this.unlinkFuture();
        };
        IntersectionPopoverViewController.prototype.didUpdateLatency = function (v) {
            var tsg = v.get('tsg').numberValue() || 0;
            var tm = v.get('tm').numberValue() || 0;
            var latency = Math.abs(tsg - tm) || 0;
            this._latencyView.text("".concat(latency, " ms"));
        };
        IntersectionPopoverViewController.prototype.linkLatency = function () {
            if (!this._linkLatency) {
                this._linkLatency = this._nodeRef.downlinkValue()
                    .laneUri("intersection/latency")
                    .didSet(this.didUpdateLatency.bind(this))
                    .open();
            }
        };
        IntersectionPopoverViewController.prototype.unlinkLatency = function () {
            if (this._linkLatency) {
                this._linkLatency.close();
                this._linkLatency = undefined;
            }
        };
        IntersectionPopoverViewController.prototype.didUpdateMode = function (v) {
            this._modeView.text("".concat(v.getItem(0).stringValue() || '--'));
        };
        IntersectionPopoverViewController.prototype.linkMode = function () {
            if (!this._linkMode) {
                this._linkMode = this._nodeRef.downlinkValue()
                    .laneUri("intersection/mode")
                    .didSet(this.didUpdateMode.bind(this))
                    .open();
            }
        };
        IntersectionPopoverViewController.prototype.unlinkMode = function () {
            if (this._linkMode) {
                this._linkMode.close();
                this._linkMode = undefined;
            }
        };
        IntersectionPopoverViewController.prototype.didUpdatePhase = function (k, v) {
            var key = k.numberValue();
            if (!this._chartChildView[key]) {
                this._contentView.append('h3')
                    .fontWeight('normal')
                    .text("Phase ".concat(key));
                var canvas = this._contentView.append('div')
                    .height(50)
                    .append('canvas')
                    .position('relative');
                var chart$1 = new chart.ChartView()
                    .bottomAxis("time")
                    .leftAxis("linear")
                    .bottomGesture(true)
                    .leftDomainPadding([0.1, 0.1])
                    .topGutter(0)
                    .rightGutter(0)
                    .bottomGutter(20)
                    .leftGutter(-1)
                    .domainColor("#4a4a4a")
                    .tickMarkColor("#4a4a4a")
                    .font("12px sans-serif")
                    .textColor("#4a4a4a");
                canvas.append(chart$1);
                var futureColor = color.Color.rgb('#6c6c6c').alpha(0.2);
                var plot0 = new chart.AreaGraphView()
                    .fill(futureColor);
                chart$1.addPlot(plot0);
                var plot1 = new chart.LineGraphView()
                    .stroke(futureColor)
                    .strokeWidth(1);
                chart$1.addPlot(plot1);
                var plot2 = new chart.LineGraphView()
                    .stroke("#00a6ed")
                    .strokeWidth(1);
                chart$1.addPlot(plot2);
                this._chartChildView[key] = {
                    chartVew: chart$1,
                    plot0View: plot0,
                    plot1View: plot1,
                    plot2View: plot2,
                };
            }
        };
        IntersectionPopoverViewController.prototype.linkPhase = function () {
            if (!this._linkPhase) {
                this._linkPhase = this._nodeRef.downlinkMap()
                    .laneUri("phase/state")
                    .didUpdate(this.didUpdatePhase.bind(this))
                    .open();
            }
        };
        IntersectionPopoverViewController.prototype.unlinkPhase = function () {
            if (this._linkPhase) {
                this._linkPhase.close();
                this._linkPhase = undefined;
            }
        };
        IntersectionPopoverViewController.prototype.didUpdateHistory = function (k, v) {
            for (var id in this._chartChildView) {
                var phaseSample = v.get('signalPhases').get(+id).get('red').numberValue() || 0;
                this._chartChildView[id].plot2View.insertDatum({ x: k.numberValue(), y: phaseSample });
            }
        };
        IntersectionPopoverViewController.prototype.didRemoveHistory = function (k, v) {
            for (var id in this._chartChildView) {
                this._chartChildView[id].plot2View.removeDatum(k.numberValue());
            }
        };
        IntersectionPopoverViewController.prototype.linkHistory = function () {
            if (!this._linkHistory) {
                this._linkHistory = this._nodeRef.downlinkMap()
                    .laneUri("intersection/history")
                    .didUpdate(this.didUpdateHistory.bind(this))
                    .didRemove(this.didRemoveHistory.bind(this))
                    .open();
            }
        };
        IntersectionPopoverViewController.prototype.unlinkHistory = function () {
            if (this._linkHistory) {
                this._linkHistory.close();
                this._linkHistory = undefined;
            }
        };
        IntersectionPopoverViewController.prototype.didUpdateFuture = function (k, v) {
            for (var id in this._chartChildView) {
                var prediction = v.get('signalPhases').get(+id).get('red').numberValue() || 0;
                var clamped = Math.round(prediction);
                this._chartChildView[id].plot0View.insertDatum({ x: k.numberValue(), y: prediction, dy: clamped });
                this._chartChildView[id].plot1View.insertDatum({ x: k.numberValue(), y: prediction });
            }
        };
        IntersectionPopoverViewController.prototype.didRemoveFuture = function (k, v) {
            for (var id in this._chartChildView) {
                this._chartChildView[id].plot0View.removeDatum(k.numberValue());
                this._chartChildView[id].plot1View.removeDatum(k.numberValue());
            }
        };
        IntersectionPopoverViewController.prototype.linkFuture = function () {
            if (!this._linkFuture) {
                this._linkFuture = this._nodeRef.downlinkMap()
                    .laneUri("intersection/future")
                    .didUpdate(this.didUpdateFuture.bind(this))
                    .didRemove(this.didRemoveFuture.bind(this))
                    .open();
            }
        };
        IntersectionPopoverViewController.prototype.unlinkFuture = function () {
            if (this._linkFuture) {
                this._linkFuture.close();
                this._linkFuture = undefined;
            }
        };
        return IntersectionPopoverViewController;
    }(view.PopoverViewController));

    var ApproachMapView = (function (_super) {
        __extends(ApproachMapView, _super);
        function ApproachMapView() {
            var _this = _super.call(this) || this;
            _this.fill.setState(color.Color.transparent());
            return _this;
        }
        Object.defineProperty(ApproachMapView.prototype, "viewController", {
            get: function () {
                return this._viewController;
            },
            enumerable: false,
            configurable: true
        });
        __decorate([
            view.MemberAnimator(color.Color, "inherit")
        ], ApproachMapView.prototype, "redLightColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color, "inherit")
        ], ApproachMapView.prototype, "yellowLightColor", void 0);
        __decorate([
            view.MemberAnimator(color.Color, "inherit")
        ], ApproachMapView.prototype, "greenLightColor", void 0);
        return ApproachMapView;
    }(map.MapPolygonView));

    var ApproachPopoverViewController = (function (_super) {
        __extends(ApproachPopoverViewController, _super);
        function ApproachPopoverViewController(info, nodeRef) {
            var _this = _super.call(this) || this;
            _this._curPhases = 0;
            _this._info = info;
            _this._nodeRef = nodeRef;
            return _this;
        }
        ApproachPopoverViewController.prototype.didSetView = function (view) {
            view.width(240)
                .height(360)
                .display('flex')
                .flexDirection('column')
                .borderRadius(5)
                .padding(10)
                .backgroundColor(color.Color.parse("#071013").alpha(0.9))
                .backdropFilter("blur(2px)")
                .color("#ffffff");
            var approach = this._info;
            var intersection = approach.intersection;
            var header = view.append("header")
                .display('flex')
                .alignItems('center');
            header.append("div")
                .borderRadius(20)
                .backgroundColor('#00a6ed')
                .padding([3, 6, 3, 6])
                .marginRight(5)
                .fontSize(15)
                .color("#000000")
                .text("".concat(intersection.id));
            header.append("h2").key("name")
                .margin(0)
                .fontSize(15)
                .color("#00a6ed")
                .text(intersection.name);
            var status = view.append('ul')
                .display('flex')
                .alignItems('center')
                .padding(0)
                .textAlign('center')
                .color('#000000');
            this._latencyView = status.append('li')
                .display('inline-block')
                .width(50)
                .backgroundColor('#00a6ed')
                .fontSize(11)
                .lineHeight('1.5em')
                .borderRadius('20px')
                .marginRight(10)
                .text('-- ms');
            this._latencyView.setStyle('list-style', 'none');
            this._modeView = status.append('li')
                .display('inline-block')
                .width(50)
                .backgroundColor('#00a6ed')
                .fontSize(11)
                .lineHeight('1.5em')
                .borderRadius('20px')
                .marginRight(10)
                .text('--');
            this._modeView.setStyle('list-style', 'none');
            var content = view.append('div')
                .display('flex')
                .flexGrow(1)
                .flexDirection('column')
                .alignItems('center')
                .overflow('auto');
            var boxSide = 80;
            var boxFontSize = 23;
            this._redView = content.append('div')
                .width(boxSide)
                .height(boxSide)
                .display('flex')
                .justifyContent('center')
                .alignItems('center')
                .margin(5)
                .borderRadius(boxSide / 2)
                .fontSize(boxFontSize)
                .opacity(0.2)
                .text('')
                .backgroundColor('#a50f21');
            this._yellowView = content.append('div')
                .width(boxSide)
                .height(boxSide)
                .display('flex')
                .justifyContent('center')
                .alignItems('center')
                .margin(5)
                .borderRadius(boxSide / 2)
                .fontSize(boxFontSize)
                .opacity(0.2)
                .text('')
                .backgroundColor('#fccf20');
            this._greenView = content.append('div')
                .width(boxSide)
                .height(boxSide)
                .display('flex')
                .justifyContent('center')
                .alignItems('center')
                .margin(5)
                .borderRadius(boxSide / 2)
                .fontSize(boxFontSize)
                .opacity(0.2)
                .text('')
                .backgroundColor('#54e218');
        };
        ApproachPopoverViewController.prototype.popoverDidShow = function (view) {
            this.linkphases();
            this.linkLatency();
            this.linkMode();
            this.linkPhaseEvent();
        };
        ApproachPopoverViewController.prototype.popoverDidHide = function (view) {
            this.unlinkphases();
            this.unlinkLatency();
            this.unlinkMode();
            this.unlinkPhaseEvent();
        };
        ApproachPopoverViewController.prototype.didUpdatephases = function (k, v) {
            if (k.numberValue() === this._info.phase) {
                this._curPhases = v.numberValue();
                this.updateLight();
            }
        };
        ApproachPopoverViewController.prototype.linkphases = function () {
            if (!this._linkphases) {
                this._linkphases = this._nodeRef.downlinkMap()
                    .laneUri("phase/state")
                    .didUpdate(this.didUpdatephases.bind(this))
                    .open();
            }
        };
        ApproachPopoverViewController.prototype.unlinkphases = function () {
            if (this._linkphases) {
                this._linkphases.close();
                this._linkphases = undefined;
            }
        };
        ApproachPopoverViewController.prototype.didUpdateLatency = function (v) {
            var tsg = v.get('tsg').numberValue() || 0;
            var tm = v.get('tm').numberValue() || 0;
            var latency = Math.abs(tsg - tm) || 0;
            this._latencyView.text("".concat(latency, " ms"));
        };
        ApproachPopoverViewController.prototype.linkLatency = function () {
            if (!this._linkLatency) {
                this._linkLatency = this._nodeRef.downlinkValue()
                    .laneUri("intersection/latency")
                    .didSet(this.didUpdateLatency.bind(this))
                    .open();
            }
        };
        ApproachPopoverViewController.prototype.unlinkLatency = function () {
            if (this._linkLatency) {
                this._linkLatency.close();
                this._linkLatency = undefined;
            }
        };
        ApproachPopoverViewController.prototype.didUpdateMode = function (v) {
            this._modeView.text("".concat(v.getItem(0).stringValue() || '--'));
        };
        ApproachPopoverViewController.prototype.linkMode = function () {
            if (!this._linkMode) {
                this._linkMode = this._nodeRef.downlinkValue()
                    .laneUri("intersection/mode")
                    .didSet(this.didUpdateMode.bind(this))
                    .open();
            }
        };
        ApproachPopoverViewController.prototype.unlinkMode = function () {
            if (this._linkMode) {
                this._linkMode.close();
                this._linkMode = undefined;
            }
        };
        ApproachPopoverViewController.prototype.countDown = function (element, nextPhase) {
            var now = new Date();
            var phaseDate = new Date(nextPhase);
            var countdown = Math.round((phaseDate.getTime() - now.getTime()) / 1000);
            if (countdown < 0) {
                countdown = 0;
            }
            element.text("".concat(countdown));
        };
        ApproachPopoverViewController.prototype.updateLight = function (nextPhase) {
            var _this = this;
            var element = null;
            switch (this._curPhases) {
                case 1:
                    element = this._redView;
                    break;
                case 2:
                    element = this._yellowView;
                    break;
                case 3:
                    element = this._greenView;
                    break;
            }
            this._redView.opacity(0.2).text('');
            this._yellowView.opacity(0.2).text('');
            this._greenView.opacity(0.2).text('');
            clearInterval(this._countDown);
            this._countDown = undefined;
            element.opacity(1);
            if (!nextPhase) {
                element.text('0');
            }
            else {
                this.countDown(element, nextPhase);
                this._countDown = setInterval(function () {
                    _this.countDown(element, nextPhase);
                }, 100);
            }
        };
        ApproachPopoverViewController.prototype.changeLight = function (nextPhase) {
            this.updateLight(nextPhase);
        };
        ApproachPopoverViewController.prototype.didUpdatePhaseEvent = function (k, v) {
            if (this._info.phase === k.numberValue()) {
                var nextPhase = v.get('clk').numberValue();
                this.changeLight(nextPhase);
            }
        };
        ApproachPopoverViewController.prototype.linkPhaseEvent = function () {
            if (!this._phaseEvent) {
                this._phaseEvent = this._nodeRef.downlinkMap()
                    .laneUri("phase/event")
                    .didUpdate(this.didUpdatePhaseEvent.bind(this))
                    .open();
            }
        };
        ApproachPopoverViewController.prototype.unlinkPhaseEvent = function () {
            if (this._phaseEvent) {
                this._phaseEvent.close();
                this._phaseEvent = undefined;
            }
        };
        return ApproachPopoverViewController;
    }(view.PopoverViewController));

    var ApproachMapViewController = (function (_super) {
        __extends(ApproachMapViewController, _super);
        function ApproachMapViewController(info, nodeRef) {
            var _this = _super.call(this) || this;
            _this._info = info;
            _this._nodeRef = nodeRef;
            _this._phase = 1;
            _this._occupied = true;
            _this._popoverView = null;
            return _this;
        }
        ApproachMapViewController.prototype.setInfo = function (info) {
            if (info.coords) {
                this._view.setCoords(info.coords);
            }
        };
        ApproachMapViewController.prototype.setPhase = function (phase) {
            this._phase = phase;
            this.updateApproach();
        };
        ApproachMapViewController.prototype.setOccupied = function (occupied) {
            this._occupied = occupied;
            this.updateApproach();
        };
        ApproachMapViewController.prototype.updateApproach = function () {
            var view = this._view;
            var signalColor;
            if (this._phase === 1) {
                signalColor = view.redLightColor.value;
            }
            else if (this._phase === 2) {
                signalColor = view.yellowLightColor.value;
            }
            else if (this._phase === 3) {
                signalColor = view.greenLightColor.value;
            }
            else {
                signalColor = color.Color.transparent();
            }
            if (this._occupied === false) {
                signalColor = signalColor.alpha(0.25);
            }
            view.fill(signalColor, transition.Transition.duration(500));
        };
        ApproachMapViewController.prototype.didSetView = function (view) {
            view.on("click", this.onClick.bind(this));
        };
        ApproachMapViewController.prototype.onClick = function (event) {
            event.stopPropagation();
            if (!this._popoverView) {
                this._popoverView = new view.PopoverView();
                var popoverViewController = new ApproachPopoverViewController(this._info, this._nodeRef);
                this._popoverView.setViewController(popoverViewController);
                this._popoverView.setSource(this._view);
                this._popoverView.hidePopover();
            }
            this.appView.togglePopover(this._popoverView, { multi: event.altKey });
        };
        return ApproachMapViewController;
    }(map.MapGraphicViewController));

    var APPROACH_ZOOM = 14;
    var IntersectionMapViewController = (function (_super) {
        __extends(IntersectionMapViewController, _super);
        function IntersectionMapViewController(info, nodeRef) {
            var _this = _super.call(this) || this;
            _this._info = info;
            _this._nodeRef = nodeRef;
            _this._schematicLink = null;
            _this._phasesLink = null;
            _this._detectorsLink = null;
            _this._pedCallLink = null;
            _this._popoverView = null;
            _this._pedCall = false;
            return _this;
        }
        IntersectionMapViewController.prototype.initMarkerView = function () {
            var markerView = this.getChildView("marker");
            if (!markerView) {
                markerView = new map.MapCircleView();
                markerView.center.setState(new map.LngLat(this._info.lng, this._info.lat));
                markerView.radius.setState(length.Length.px(5));
                markerView.fill.setState(this._view.intersectionMarkerColor.value);
                markerView.on("click", this.onMarkerClick.bind(this));
                this.setChildView("marker", markerView);
            }
        };
        IntersectionMapViewController.prototype.onMarkerClick = function (event) {
            event.stopPropagation();
            if (!this._popoverView) {
                this._popoverView = new view.PopoverView();
                var popoverViewController = new IntersectionPopoverViewController(this._info, this._nodeRef);
                this._popoverView.setViewController(popoverViewController);
                this._popoverView.setSource(this.getChildView("marker"));
                this._popoverView.hidePopover();
            }
            this.appView.togglePopover(this._popoverView, { multi: event.altKey });
        };
        IntersectionMapViewController.prototype.didSetSchematic = function (value) {
            value.forEach(function (item) {
                if (item.tag() === "approach") {
                    this.didUpdateApproach(item.toAny());
                }
            }, this);
        };
        IntersectionMapViewController.prototype.didUpdateApproach = function (approachInfo) {
            var approachId = "approach-" + approachInfo.id;
            approachInfo.intersection = this._info;
            var approachMapView = this.getChildView(approachId);
            if (!approachMapView) {
                approachMapView = new ApproachMapView();
                var intersectionMapViewController = new ApproachMapViewController(approachInfo, this._nodeRef);
                approachMapView.setViewController(intersectionMapViewController);
                this.setChildView(approachId, approachMapView);
            }
            var approachMapViewController = approachMapView.viewController;
            approachMapViewController.setInfo(approachInfo);
        };
        IntersectionMapViewController.prototype.didUpdatePhase = function (key, value) {
            var phaseId = key.toAny();
            var phase = value.numberValue();
            var childViews = this.childViews;
            for (var i = 0, n = childViews.length; i < n; i += 1) {
                var childView = childViews[i];
                if (childView instanceof ApproachMapView) {
                    var approachMapViewController = childView.viewController;
                    if (approachMapViewController._info.phase === phaseId) {
                        approachMapViewController.setPhase(phase);
                    }
                }
            }
        };
        IntersectionMapViewController.prototype.didUpdateDetector = function (key, value) {
            var detectorId = key.toAny();
            var occupied = value.booleanValue(false);
            var childViews = this.childViews;
            for (var i = 0, n = childViews.length; i < n; i += 1) {
                var childView = childViews[i];
                if (childView instanceof ApproachMapView) {
                    var approachMapViewController = childView.viewController;
                    if (approachMapViewController._info.detector === detectorId) {
                        approachMapViewController.setOccupied(occupied);
                    }
                }
            }
            if (occupied) {
                this.ripple(this._view.intersectionMarkerColor.value);
            }
        };
        IntersectionMapViewController.prototype.didSetPedCall = function (key, value) {
            var view = this._view;
            this._pedCall = value.isDefined();
            var marker = this.getChildView("marker");
            if (marker) {
                marker.fill.setState(this._pedCall ? view.pedestrianMarkerColor.value : view.intersectionMarkerColor.value, transition.Transition.duration(500));
            }
            if (this._pedCall) {
                this.ripple(view.pedestrianMarkerColor.value);
            }
        };
        IntersectionMapViewController.prototype.ripple = function (color) {
            if (document.hidden || this.culled) {
                return;
            }
            var ripple = new map.MapCircleView()
                .center(new map.LngLat(this._info.lng, this._info.lat))
                .radius(0)
                .fill(null)
                .stroke(color.alpha(1))
                .strokeWidth(1);
            this.appendChildView(ripple);
            var radius = Math.min(this.bounds.width, this.bounds.height) / 8;
            var tween = transition.Transition.duration(5000);
            ripple.stroke(color.alpha(0), tween)
                .radius(radius, tween.onEnd(function () { ripple.remove(); }));
        };
        IntersectionMapViewController.prototype.viewDidMount = function (view) {
            this.initMarkerView();
            this.linkSchematic();
        };
        IntersectionMapViewController.prototype.viewWillUnmount = function (view) {
            this.unlinkSchematic();
            this.unlinkPhases();
            this.unlinkDetectors();
            this.unlinkPedCall();
        };
        IntersectionMapViewController.prototype.viewDidSetZoom = function (newZoom, oldZoom, view) {
            var childViews = this.childViews;
            for (var i = 0, n = childViews.length; i < n; i += 1) {
                var childView = childViews[i];
                if (childView instanceof ApproachMapView) {
                    childView.setHidden(newZoom < APPROACH_ZOOM);
                }
            }
        };
        IntersectionMapViewController.prototype.viewDidSetCulled = function (culled, view) {
            if (culled || this.zoom < APPROACH_ZOOM) {
                this.unlinkPhases();
            }
            else if (view._hitBounds !== null) {
                this.linkPhases();
            }
            if (culled) {
                this.unlinkDetectors();
                this.unlinkPedCall();
            }
            else if (view._hitBounds !== null) {
                this.linkDetectors();
                this.linkPedCall();
            }
        };
        IntersectionMapViewController.prototype.linkSchematic = function () {
            if (!this._schematicLink) {
                this._schematicLink = this._nodeRef.downlinkValue()
                    .laneUri("intersection/schematic")
                    .didSet(this.didSetSchematic.bind(this))
                    .open();
            }
        };
        IntersectionMapViewController.prototype.unlinkSchematic = function () {
            if (this._schematicLink) {
                this._schematicLink.close();
                this._schematicLink = null;
            }
        };
        IntersectionMapViewController.prototype.linkPhases = function () {
            if (!this._phasesLink) {
                this._phasesLink = this._nodeRef.downlinkMap()
                    .laneUri("phase/state")
                    .didUpdate(this.didUpdatePhase.bind(this))
                    .open();
            }
        };
        IntersectionMapViewController.prototype.unlinkPhases = function () {
            if (this._phasesLink) {
                this._phasesLink.close();
                this._phasesLink = null;
            }
        };
        IntersectionMapViewController.prototype.linkDetectors = function () {
            if (!this._detectorsLink) {
                this._detectorsLink = this._nodeRef.downlinkMap()
                    .laneUri("detector/state")
                    .didUpdate(this.didUpdateDetector.bind(this))
                    .open();
            }
        };
        IntersectionMapViewController.prototype.unlinkDetectors = function () {
            if (this._detectorsLink) {
                this._detectorsLink.close();
                this._detectorsLink = null;
            }
        };
        IntersectionMapViewController.prototype.linkPedCall = function () {
            if (!this._pedCallLink) {
                this._pedCallLink = this._nodeRef.downlinkValue()
                    .laneUri("pedCall")
                    .didSet(this.didSetPedCall.bind(this))
                    .open();
            }
        };
        IntersectionMapViewController.prototype.unlinkPedCall = function () {
            if (this._pedCallLink) {
                this._pedCallLink.close();
                this._pedCallLink = null;
            }
        };
        return IntersectionMapViewController;
    }(map.MapGraphicViewController));

    var TrafficMapViewController = (function (_super) {
        __extends(TrafficMapViewController, _super);
        function TrafficMapViewController(nodeRef) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._intersectionsLink = null;
            return _this;
        }
        TrafficMapViewController.prototype.didUpdateIntersection = function (key, value) {
            var intersectionInfo = value.toAny();
            var intersectionId = "" + intersectionInfo.id;
            var intersectionMapView = this.getChildView(intersectionId);
            if (!intersectionMapView) {
                var intersectionNodeUri = key.stringValue();
                var intersectionNodeRef = this._nodeRef.nodeRef(intersectionNodeUri);
                intersectionMapView = new IntersectionMapView();
                var intersectionMapViewController = new IntersectionMapViewController(intersectionInfo, intersectionNodeRef);
                intersectionMapView.setViewController(intersectionMapViewController);
                this.setChildView(intersectionId, intersectionMapView);
            }
        };
        TrafficMapViewController.prototype.viewDidMount = function (view) {
            this.linkIntersections();
        };
        TrafficMapViewController.prototype.viewWillUnmount = function (view) {
            this.unlinkIntersections();
        };
        TrafficMapViewController.prototype.linkIntersections = function () {
            if (!this._intersectionsLink) {
                this._intersectionsLink = this._nodeRef.downlinkMap()
                    .laneUri("intersections")
                    .didUpdate(this.didUpdateIntersection.bind(this))
                    .open();
            }
        };
        TrafficMapViewController.prototype.unlinkIntersections = function () {
            if (this._intersectionsLink) {
                this._intersectionsLink.close();
                this._intersectionsLink = null;
            }
        };
        return TrafficMapViewController;
    }(map.MapGraphicViewController));

    (function (SignalPhase) {
        SignalPhase[SignalPhase["Unknown"] = 0] = "Unknown";
        SignalPhase[SignalPhase["Red"] = 1] = "Red";
        SignalPhase[SignalPhase["Yellow"] = 2] = "Yellow";
        SignalPhase[SignalPhase["Green"] = 3] = "Green";
    })(exports.SignalPhase || (exports.SignalPhase = {}));

    var TrafficKpiViewController = (function (_super) {
        __extends(TrafficKpiViewController, _super);
        function TrafficKpiViewController() {
            var _this = _super.call(this) || this;
            _this._updateTimer = 0;
            return _this;
        }
        Object.defineProperty(TrafficKpiViewController.prototype, "title", {
            get: function () {
                return this._title;
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(TrafficKpiViewController.prototype, "subtitle", {
            get: function () {
                return this._subtitle;
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(TrafficKpiViewController.prototype, "meterLegend", {
            get: function () {
                return this._meterLegend;
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(TrafficKpiViewController.prototype, "clearLegend", {
            get: function () {
                return this._clearLegend;
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(TrafficKpiViewController.prototype, "pieView", {
            get: function () {
                return this._view.getChildView("body").getChildView("canvas").getChildView("pie");
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(TrafficKpiViewController.prototype, "titleView", {
            get: function () {
                return this.pieView.title();
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(TrafficKpiViewController.prototype, "meterView", {
            get: function () {
                return this.pieView.getChildView("meter");
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(TrafficKpiViewController.prototype, "emptyView", {
            get: function () {
                return this.pieView.getChildView("empty");
            },
            enumerable: false,
            configurable: true
        });
        TrafficKpiViewController.prototype.didSetView = function (view) {
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
            this._title = headerLeft.append("span").display("block").text("Palo Alto — Pedestrian Backup");
            this._subtitle = headerLeft.append("span").display("block").text("@ Crosswalks");
            var headerRight = header.append("div");
            headerRight.append("span").text("Real-Time");
            var body = view.append("div").key("body").position("relative").flexGrow(1).width("100%");
            var bodyCanvas = body.append("canvas").key("canvas");
            var gauge$1 = new gauge.GaugeView();
            bodyCanvas.append(gauge$1);
            var innerDial = new gauge.DialView()
                .arrangement("manual")
                .innerRadius(length.Length.pct(34))
                .outerRadius(length.Length.pct(37))
                .dialColor(primaryColor.alpha(0.25));
            gauge$1.append(innerDial);
            var outerDial = new gauge.DialView()
                .arrangement("manual")
                .innerRadius(length.Length.pct(37))
                .outerRadius(length.Length.pct(40))
                .dialColor(primaryColor.alpha(0.15));
            gauge$1.append(outerDial);
            var pie$1 = new pie.PieView()
                .key("pie")
                .innerRadius(length.Length.pct(34))
                .outerRadius(length.Length.pct(40))
                .cornerRadius(length.Length.pct(50))
                .tickRadius(length.Length.pct(45))
                .font("12px \"Open Sans\", sans-serif")
                .textColor(primaryColor);
            bodyCanvas.append(pie$1);
            var title = new typeset.TextRunView()
                .font("36px \"Open Sans\", sans-serif")
                .textColor(primaryColor);
            pie$1.title(title);
            var meter = new pie.SliceView()
                .key("meter")
                .sliceColor(primaryColor)
                .tickColor(primaryColor);
            pie$1.append(meter);
            this._meterLegend = new typeset.TextRunView("Waiting").textColor(primaryColor);
            meter.legend(this._meterLegend);
            var empty = new pie.SliceView()
                .key("empty")
                .sliceColor(color.Color.transparent())
                .tickColor(primaryColor.darker(3));
            pie$1.append(empty);
            this._clearLegend = new typeset.TextRunView("Clear").textColor(primaryColor.darker(3));
            empty.legend(this._clearLegend);
        };
        TrafficKpiViewController.prototype.viewDidMount = function (view) {
            requestAnimationFrame(function () { view.cascadeResize(); });
            this._updateTimer = setInterval(this.updateKpi.bind(this), 1000);
            this.updateKpi();
        };
        TrafficKpiViewController.prototype.viewWillUnmount = function (view) {
            clearInterval(this._updateTimer);
            this._updateTimer = 0;
        };
        return TrafficKpiViewController;
    }(view.HtmlViewController));

    var VehicleFlowKpiViewController = (function (_super) {
        __extends(VehicleFlowKpiViewController, _super);
        function VehicleFlowKpiViewController(nodeRef, trafficMapView) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._trafficMapView = trafficMapView;
            return _this;
        }
        Object.defineProperty(VehicleFlowKpiViewController.prototype, "primaryColor", {
            get: function () {
                return color.Color.parse("#5aff15");
            },
            enumerable: false,
            configurable: true
        });
        VehicleFlowKpiViewController.prototype.updateKpi = function () {
            var meterValue = 0;
            var spaceValue = 0;
            var intersectionMapViews = this._trafficMapView.childViews;
            for (var i = 0; i < intersectionMapViews.length; i += 1) {
                var intersectionMapView = intersectionMapViews[i];
                if (intersectionMapView instanceof IntersectionMapView && !intersectionMapView.culled) {
                    var approachMapViews = intersectionMapView.childViews;
                    for (var j = 0; j < approachMapViews.length; j += 1) {
                        var approachMapView = approachMapViews[j];
                        if (approachMapView instanceof ApproachMapView) {
                            var approachMapViewController = approachMapView.viewController;
                            if (approachMapViewController._phase === 3) {
                                if (approachMapViewController._occupied) {
                                    meterValue += 1;
                                }
                                else {
                                    spaceValue += 1;
                                }
                            }
                        }
                    }
                }
            }
            var title = this.titleView;
            var meter = this.meterView;
            var empty = this.emptyView;
            var tween = transition.Transition.duration(1000);
            this.title.text('Palo Alto - VEHICLE Flow');
            this.subtitle.text('@ GREEN LIGHTS');
            meter.value(meterValue, tween);
            empty.value(spaceValue, tween);
            this.meterLegend.text("Flowing (" + meterValue + ")");
            this.clearLegend.text("Clear (" + spaceValue + ")");
            title.text(Math.round(100 * meterValue / ((meterValue + spaceValue) || 1)) + "%");
        };
        return VehicleFlowKpiViewController;
    }(TrafficKpiViewController));

    var VehicleBackupKpiViewController = (function (_super) {
        __extends(VehicleBackupKpiViewController, _super);
        function VehicleBackupKpiViewController(nodeRef, trafficMapView) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._trafficMapView = trafficMapView;
            return _this;
        }
        Object.defineProperty(VehicleBackupKpiViewController.prototype, "primaryColor", {
            get: function () {
                return color.Color.parse("#d90c25");
            },
            enumerable: false,
            configurable: true
        });
        VehicleBackupKpiViewController.prototype.updateKpi = function () {
            var meterValue = 0;
            var spaceValue = 0;
            var intersectionMapViews = this._trafficMapView.childViews;
            for (var i = 0; i < intersectionMapViews.length; i += 1) {
                var intersectionMapView = intersectionMapViews[i];
                if (intersectionMapView instanceof IntersectionMapView && !intersectionMapView.culled) {
                    var approachMapViews = intersectionMapView.childViews;
                    for (var j = 0; j < approachMapViews.length; j += 1) {
                        var approachMapView = approachMapViews[j];
                        if (approachMapView instanceof ApproachMapView) {
                            var approachMapViewController = approachMapView.viewController;
                            if (approachMapViewController._phase === 1) {
                                if (approachMapViewController._occupied) {
                                    meterValue += 1;
                                }
                                else {
                                    spaceValue += 1;
                                }
                            }
                        }
                    }
                }
            }
            var title = this.titleView;
            var meter = this.meterView;
            var empty = this.emptyView;
            var tween = transition.Transition.duration(1000);
            this.title.text('Palo Alto - VEHICLE BACKUP');
            this.subtitle.text('@ RED LIGHTS');
            meter.value(meterValue, tween);
            empty.value(spaceValue, tween);
            this.meterLegend.text("Waiting (" + meterValue + ")");
            this.clearLegend.text("Clear (" + spaceValue + ")");
            title.text(Math.round(100 * meterValue / ((meterValue + spaceValue) || 1)) + "%");
        };
        return VehicleBackupKpiViewController;
    }(TrafficKpiViewController));

    var PedestrianBackupKpiViewController = (function (_super) {
        __extends(PedestrianBackupKpiViewController, _super);
        function PedestrianBackupKpiViewController(nodeRef, trafficMapView) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._trafficMapView = trafficMapView;
            return _this;
        }
        Object.defineProperty(PedestrianBackupKpiViewController.prototype, "primaryColor", {
            get: function () {
                return color.Color.parse("#c200fa");
            },
            enumerable: false,
            configurable: true
        });
        PedestrianBackupKpiViewController.prototype.updateKpi = function () {
            var meterValue = 0;
            var spaceValue = 0;
            var intersectionMapViews = this._trafficMapView.childViews;
            for (var i = 0; i < intersectionMapViews.length; i += 1) {
                var intersectionMapView = intersectionMapViews[i];
                if (intersectionMapView instanceof IntersectionMapView && !intersectionMapView.culled) {
                    var intersectionMapViewController = intersectionMapView.viewController;
                    if (intersectionMapViewController._pedCall) {
                        meterValue += 1;
                    }
                    else {
                        spaceValue += 1;
                    }
                }
            }
            var title = this.titleView;
            var meter = this.meterView;
            var empty = this.emptyView;
            var tween = transition.Transition.duration(1000);
            this.title.text('Palo Alto - PEDESTRIAN BACKUP');
            this.subtitle.text('@ CROSSWALKS');
            meter.value(meterValue, tween);
            empty.value(spaceValue, tween);
            this.meterLegend.text("Waiting (" + meterValue + ")");
            this.clearLegend.text("Clear (" + spaceValue + ")");
            title.text(Math.round(100 * meterValue / ((meterValue + spaceValue) || 1)) + "%");
        };
        return PedestrianBackupKpiViewController;
    }(TrafficKpiViewController));

    var TrafficViewController = (function (_super) {
        __extends(TrafficViewController, _super);
        function TrafficViewController(nodeRef) {
            var _this = _super.call(this) || this;
            _this._nodeRef = nodeRef;
            _this._map = null;
            return _this;
        }
        TrafficViewController.prototype.didSetView = function (view) {
            this._map = new mapboxgl.Map({
                container: view.node,
                style: "mapbox://styles/swimit/cjs5h20wh0fyf1gocidkpmcvm",
                center: { lng: -122.16, lat: 37.445 },
                pitch: 70,
                zoom: 15.5,
            });
            var mapboxView = new mapbox.MapboxView(this._map);
            mapboxView.overlayCanvas();
            var trafficMapView = new TrafficMapView();
            var trafficMapViewController = new TrafficMapViewController(this._nodeRef);
            trafficMapView.setViewController(trafficMapViewController);
            mapboxView.setChildView("map", trafficMapView);
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
            view.append(this.createKpiStack(trafficMapView));
            this.layoutKpiStack();
        };
        TrafficViewController.prototype.viewDidResize = function () {
            this.layoutKpiStack();
        };
        TrafficViewController.prototype.createKpiStack = function (trafficMapView) {
            var kpiStack = view.HtmlView.fromTag("div")
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
            var vehicleFlowKpiViewController = new VehicleFlowKpiViewController(this._nodeRef, trafficMapView);
            vehicleFlowKpi.setViewController(vehicleFlowKpiViewController);
            var vehicleBackupKpi = kpiStack.append("div")
                .key("vehicleBackupKpi")
                .position("absolute")
                .borderRadius(8)
                .boxSizing("border-box")
                .backgroundColor(color.Color.parse("#070813").alpha(0.33))
                .backdropFilter("blur(2px)")
                .pointerEvents("auto");
            var vehicleBackupKpiViewController = new VehicleBackupKpiViewController(this._nodeRef, trafficMapView);
            vehicleBackupKpi.setViewController(vehicleBackupKpiViewController);
            var pedestrianBackupKpi = kpiStack.append("div")
                .key("pedestrianBackupKpi")
                .position("absolute")
                .borderRadius(8)
                .boxSizing("border-box")
                .backgroundColor(color.Color.parse("#070813").alpha(0.33))
                .backdropFilter("blur(2px)")
                .pointerEvents("auto");
            var pedestrianBackupKpiViewController = new PedestrianBackupKpiViewController(this._nodeRef, trafficMapView);
            pedestrianBackupKpi.setViewController(pedestrianBackupKpiViewController);
            return kpiStack;
        };
        TrafficViewController.prototype.layoutKpiStack = function () {
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
            }
            else {
                kpiStack.display("none");
            }
        };
        TrafficViewController.prototype.createLogo = function () {
            var logo = view.SvgView.fromTag("svg").width(156).height(68).viewBox("0 0 156 68");
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
        return TrafficViewController;
    }(view.HtmlViewController));

    exports.ApproachMapView = ApproachMapView;
    exports.ApproachMapViewController = ApproachMapViewController;
    exports.ApproachPopoverViewController = ApproachPopoverViewController;
    exports.IntersectionMapView = IntersectionMapView;
    exports.IntersectionMapViewController = IntersectionMapViewController;
    exports.IntersectionPopoverViewController = IntersectionPopoverViewController;
    exports.PedestrianBackupKpiViewController = PedestrianBackupKpiViewController;
    exports.TrafficKpiViewController = TrafficKpiViewController;
    exports.TrafficMapView = TrafficMapView;
    exports.TrafficMapViewController = TrafficMapViewController;
    exports.TrafficViewController = TrafficViewController;
    exports.VehicleBackupKpiViewController = VehicleBackupKpiViewController;
    exports.VehicleFlowKpiViewController = VehicleFlowKpiViewController;

    Object.defineProperty(exports, '__esModule', { value: true });

}));
//# sourceMappingURL=swim-traffic.js.map
