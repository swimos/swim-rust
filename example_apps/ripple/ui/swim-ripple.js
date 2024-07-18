(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('@swim/util'), require('@swim/color'), require('@swim/structure'), require('@swim/interpolate'), require('@swim/view'), require('@swim/transition'), require('@swim/recon')) :
        typeof define === 'function' && define.amd ? define(['exports', '@swim/util', '@swim/color', '@swim/structure', '@swim/interpolate', '@swim/view', '@swim/transition', '@swim/recon'], factory) :
            (global = global || self, factory(global.swim = global.swim || {}, global.swim, global.swim, global.swim, global.swim, global.swim, global.swim, global.swim));
}(this, function (exports, util, color, structure, interpolate, view, transition, recon) {
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

    var MirrorMode = {
        default: {
            minRipples: 2,
            maxRipples: 5,
            rippleDuration: 5000,
            rippleSpread: 300,
        },
    };
    var MirrorView = (function (_super) {
        __extends(MirrorView, _super);

        function MirrorView(id, mode) {
            if (mode === void 0) {
                mode = MirrorMode.default;
            }
            var _this = _super.call(this) || this;
            if (id === void 0) {
                id = structure.Text.from(structure.Data.random(6).toBase64());
            }
            _this.onMouseDown = _this.onMouseDown.bind(_this);
            _this.onMouseMove = _this.onMouseMove.bind(_this);
            _this.onMouseUp = _this.onMouseUp.bind(_this);
            _this.onTouchStart = _this.onTouchStart.bind(_this);
            _this.onTouchMove = _this.onTouchMove.bind(_this);
            _this.onTouchCancel = _this.onTouchCancel.bind(_this);
            _this.onTouchEnd = _this.onTouchEnd.bind(_this);
            _this.id = id;
            _this.mode = mode;
            _this.color = color.Color.fromAny(MirrorView.colors[Math.floor(MirrorView.colors.length * Math.random())]);
            _this.captive = false;
            _this._touchCount = 0;
            _this._presses = {};
            return _this;
        }

        Object.defineProperty(MirrorView.prototype, "viewController", {
            get: function () {
                return this._viewController;
            },
            enumerable: false,
            configurable: true
        });
        Object.defineProperty(MirrorView.prototype, "canvasView", {
            get: function () {
                var parentView = this.parentView;
                while (parentView) {
                    if (parentView instanceof view.CanvasView) {
                        return parentView;
                    } else {
                        parentView = parentView.parentView;
                    }
                }
                throw new Error("not mounted");
            },
            enumerable: false,
            configurable: true
        });
        MirrorView.prototype.onMount = function () {
            var canvasView = this.canvasView;
            canvasView.cursor("default");
            if (typeof TouchEvent !== "undefined") {
                canvasView.on("touchstart", this.onTouchStart);
            }
            canvasView.on("mousedown", this.onMouseDown);
        };
        MirrorView.prototype.onUnmount = function () {
            var canvasView = this.canvasView;
            if (typeof TouchEvent !== "undefined") {
                canvasView.off("touchstart", this.onTouchStart);
                canvasView.off("touchmove", this.onTouchMove);
                canvasView.off("touchcancel", this.onTouchCancel);
                canvasView.off("touchend", this.onTouchEnd);
            }
            canvasView.off("mousedown", this.onMouseDown);
            canvasView.off("mousemove", this.onMouseMove);
            canvasView.off("mouseup", this.onMouseUp);
        };
        MirrorView.prototype.onAnimate = function (viewContext) {
        };
        MirrorView.prototype.onRender = function (viewContext) {
            var context = viewContext.renderingContext;
            context.save();
            var bounds = this._bounds;
            this.renderBonds(context, bounds);
            context.restore();
        };
        MirrorView.prototype.renderBonds = function (context, bounds) {
            var childViews = this.childViews;
            var chargeViews = [];
            var chargeCount = 0;
            for (var i = 0, n = childViews.length; i < n; i += 1) {
                var childView = childViews[i];
                if (childView instanceof MirrorView.ChargeView && childView.isPressed()) {
                    chargeViews.push(childView);
                    chargeCount += 1;
                }
            }
            if (chargeCount >= 2) {
                chargeViews.sort(function (a, b) {
                    return util.Objects.compare(a.t0, b.t0);
                });
                var now = Date.now();
                var a = chargeViews[0];
                for (var i = 1; i < chargeCount; i += 1) {
                    var b = chargeViews[i];
                    this.renderBond(context, bounds, a, b, now);
                }
            }
        };
        MirrorView.prototype.renderBond = function (context, bounds, a, b, now) {
            var chargeOpacity = this.mode.chargeOpacity !== void 0 ? this.mode.chargeOpacity : 1;
            var chargeDarken = this.mode.chargeDarken !== void 0 ? this.mode.chargeDarken : 0;
            var ax = a.centerX.value * bounds.width;
            var ay = a.centerY.value * bounds.height;
            var aColor = a.chargeColor.value.darker(chargeDarken).alpha(chargeOpacity);
            var bx = b.centerX.value * bounds.width;
            var by = b.centerY.value * bounds.height;
            var bColor = b.chargeColor.value.darker(chargeDarken).alpha(chargeOpacity);
            var dt = now - a.t0;
            var pulseWidth = 2000;
            var halfPulse = pulseWidth / 2;
            var pulsePhase = (dt % pulseWidth) / halfPulse;
            if (pulsePhase > 1) {
                pulsePhase = 2 - pulsePhase;
            }
            context.lineWidth = 4;
            var gradient = context.createLinearGradient(ax, ay, bx, by);
            gradient.addColorStop(0, aColor.alpha(0).toString());
            if (pulsePhase !== 0 && pulsePhase !== 1) {
                var abColor = interpolate.Interpolator.color(aColor, bColor).interpolate(pulsePhase);
                gradient.addColorStop(pulsePhase, abColor.toString());
            }
            gradient.addColorStop(1, bColor.alpha(0).toString());
            context.strokeStyle = gradient;
            context.beginPath();
            context.moveTo(ax, ay);
            context.lineTo(bx, by);
            context.stroke();
        };
        MirrorView.prototype.didRender = function () {
            var childViews = this.childViews;
            var i = 0;
            while (i < childViews.length) {
                var childView = childViews[i];
                if (childView instanceof MirrorView.ChargeView && !childView.isActive() && !childView.isPressed()) {
                    this.removeChildView(childView);
                } else {
                    i += 1;
                }
            }
        };
        MirrorView.prototype.createCharge = function (id, t0, clientX, clientY, phases, chargeMode) {
            var canvasView = this.canvasView;
            var clientRect = canvasView.node.getBoundingClientRect();
            var originX = (clientX - clientRect.left) / (clientRect.width || 1);
            var originY = (clientY - clientRect.top) / (clientRect.height || 1);
            var mode = this.mode;
            var rippleCount = mode.minRipples + Math.round((mode.maxRipples - mode.minRipples) * Math.random());
            if (phases === void 0) {
                phases = new Array(rippleCount);
                phases[0] = 0;
                for (var i = 1; i < rippleCount; i += 1) {
                    phases[i] = Math.random();
                }
            }
            if (chargeMode === void 0) {
                chargeMode = MirrorView.ChargeMode.from();
                chargeMode.chargeColor = this.color;
                chargeMode.rippleColor = this.color;
            }
            if (mode.chargeOpacity !== void 0) {
                chargeMode.chargeOpacity = mode.chargeOpacity;
            }
            if (mode.chargeDarken !== void 0) {
                chargeMode.chargeDarken = mode.chargeDarken;
            }
            if (mode.chargeRadius !== void 0) {
                chargeMode.chargeRadius = mode.chargeRadius;
            }
            if (mode.chargeJitterRadius !== void 0) {
                chargeMode.chargeJitterRadius = mode.chargeJitterRadius;
            }
            if (mode.rippleOpacity !== void 0) {
                chargeMode.rippleOpacity = mode.rippleOpacity;
            }
            if (mode.rippleDarken !== void 0) {
                chargeMode.rippleDarken = mode.rippleDarken;
            }
            return new MirrorView.ChargeView(id, t0, originX, originY, phases, chargeMode);
        };
        MirrorView.prototype.onPressDown = function (charge) {
            this.didObserve(function (viewObserver) {
                if (viewObserver.mirrorDidPressDown) {
                    viewObserver.mirrorDidPressDown(charge, this);
                }
            });
        };
        MirrorView.prototype.onPressHold = function (charge) {
            this.didObserve(function (viewObserver) {
                if (viewObserver.mirrorDidPressHold) {
                    viewObserver.mirrorDidPressHold(charge, this);
                }
            });
        };
        MirrorView.prototype.onPressMove = function (charge, clientX, clientY) {
            var canvasView = this.canvasView;
            var clientRect = canvasView.node.getBoundingClientRect();
            var centerX = (clientX - clientRect.left) / (clientRect.width || 1);
            var centerY = (clientY - clientRect.top) / (clientRect.height || 1);
            charge.onPressMove(centerX, centerY);
            if (charge.isPressed()) {
                this.didObserve(function (viewObserver) {
                    if (viewObserver.mirrorDidPressMove) {
                        viewObserver.mirrorDidPressMove(charge, this);
                    }
                });
            }
        };
        MirrorView.prototype.onPressUp = function (charge) {
            var pressed = charge.isPressed();
            charge.onPressUp();
            if (pressed) {
                this.didObserve(function (viewObserver) {
                    if (viewObserver.mirrorDidPressUp) {
                        viewObserver.mirrorDidPressUp(charge, this);
                    }
                });
            }
        };
        MirrorView.prototype.onMouseDown = function (event) {
            if (event.button !== 0 || this._presses["mouse"]) {
                return;
            }
            document.body.addEventListener("mousemove", this.onMouseMove);
            document.body.addEventListener("mouseup", this.onMouseUp);
            var charge = this.createCharge(structure.Record.of(structure.Attr.of("User"), this.id, "mouse"), Date.now(), event.clientX, event.clientY);
            this._presses["mouse"] = charge;
            this.appendChildView(charge);
            this.onPressDown(charge);
        };
        MirrorView.prototype.onMouseMove = function (event) {
            var charge = this._presses["mouse"];
            if (charge) {
                this.onPressMove(charge, event.clientX, event.clientY);
            }
        };
        MirrorView.prototype.onMouseUp = function (event) {
            var charge = this._presses["mouse"];
            if (charge) {
                delete this._presses["mouse"];
                this.onPressUp(charge);
            }
            document.body.removeEventListener("mousemove", this.onMouseMove);
            document.body.removeEventListener("mouseup", this.onMouseUp);
        };
        MirrorView.prototype.onTouchStart = function (event) {
            if (this.captive) {
                event.preventDefault();
            }
            var touches = event.changedTouches;
            for (var i = 0, n = touches.length; i < n; i += 1) {
                var touch = touches[i];
                var pressId = "touch" + touch.identifier;
                var charge = this._presses[pressId];
                if (charge === void 0) {
                    if (this._touchCount === 0) {
                        var canvasView = this.canvasView;
                        canvasView.on("touchmove", this.onTouchMove);
                        canvasView.on("touchcancel", this.onTouchCancel);
                        canvasView.on("touchend", this.onTouchEnd);
                    }
                    charge = this.createCharge(structure.Record.of(structure.Attr.of("User"), this.id, pressId), Date.now(), touch.clientX, touch.clientY);
                    this._presses[pressId] = charge;
                    this._touchCount += 1;
                    this.appendChildView(charge);
                    this.onPressDown(charge);
                }
            }
        };
        MirrorView.prototype.onTouchMove = function (event) {
            var touches = event.changedTouches;
            for (var i = 0, n = touches.length; i < n; i += 1) {
                var touch = touches[i];
                var pressId = "touch" + touch.identifier;
                var charge = this._presses[pressId];
                if (charge) {
                    this.onPressMove(charge, touch.clientX, touch.clientY);
                }
            }
        };
        MirrorView.prototype.onTouchCancel = function (event) {
            var touches = event.changedTouches;
            for (var i = 0, n = touches.length; i < n; i += 1) {
                var touch = touches[i];
                var pressId = "touch" + touch.identifier;
                var charge = this._presses[pressId];
                if (charge) {
                    delete this._presses[pressId];
                    this._touchCount -= 1;
                    this.onPressUp(charge);
                }
            }
            if (this._touchCount === 0) {
                var canvasView = this.canvasView;
                canvasView.off("touchmove", this.onTouchMove);
                canvasView.off("touchcancel", this.onTouchCancel);
                canvasView.off("touchend", this.onTouchEnd);
            }
        };
        MirrorView.prototype.onTouchEnd = function (event) {
            var touches = event.changedTouches;
            for (var i = 0, n = touches.length; i < n; i += 1) {
                var touch = touches[i];
                var pressId = "touch" + touch.identifier;
                var charge = this._presses[pressId];
                if (charge) {
                    delete this._presses[pressId];
                    this._touchCount -= 1;
                    this.onPressUp(charge);
                }
            }
            if (this._touchCount === 0) {
                var canvasView = this.canvasView;
                canvasView.off("touchmove", this.onTouchMove);
                canvasView.off("touchcancel", this.onTouchCancel);
                canvasView.off("touchend", this.onTouchEnd);
            }
        };
        MirrorView.colors = ["#80dc1a", "#56dbb6", "#c200fa"];
        return MirrorView;
    }(view.GraphicView));

    var MirrorViewController = (function (_super) {
        __extends(MirrorViewController, _super);

        function MirrorViewController() {
            return _super !== null && _super.apply(this, arguments) || this;
        }

        MirrorViewController.prototype.mirrorDidPressDown = function (charge, view) {
        };
        MirrorViewController.prototype.mirrorDidPressHold = function (charge, view) {
        };
        MirrorViewController.prototype.mirrorDidPressMove = function (charge, view) {
        };
        MirrorViewController.prototype.mirrorDidPressUp = function (charge, view) {
        };
        return MirrorViewController;
    }(view.GraphicViewController));

    var ChargeMode = {
        from: function (chargeColor, chargeOpacity, chargeDarken, chargeRadius, chargeJitterRadius, rippleColor, rippleOpacity, rippleDarken, rippleDuration, rippleSpread, pressDelay) {
            if (chargeColor === void 0) {
                chargeColor = color.Color.rgb("#80dc1a");
            }
            if (chargeOpacity === void 0) {
                chargeOpacity = 1;
            }
            if (chargeDarken === void 0) {
                chargeDarken = 0;
            }
            if (chargeRadius === void 0) {
                chargeRadius = 40;
            }
            if (chargeJitterRadius === void 0) {
                chargeJitterRadius = 4;
            }
            if (rippleColor === void 0) {
                rippleColor = color.Color.rgb("#80dc1a");
            }
            if (rippleOpacity === void 0) {
                rippleOpacity = 1;
            }
            if (rippleDarken === void 0) {
                rippleDarken = 0;
            }
            if (rippleDuration === void 0) {
                rippleDuration = 5000;
            }
            if (rippleSpread === void 0) {
                rippleSpread = 300;
            }
            if (pressDelay === void 0) {
                pressDelay = 500;
            }
            return {
                chargeColor: chargeColor,
                chargeOpacity: chargeOpacity,
                chargeDarken: chargeDarken,
                chargeRadius: chargeRadius,
                chargeJitterRadius: chargeJitterRadius,
                rippleColor: rippleColor,
                rippleOpacity: rippleOpacity,
                rippleDarken: rippleDarken,
                rippleDuration: rippleDuration,
                rippleSpread: rippleSpread,
                pressDelay: pressDelay,
            };
        }
    };
    MirrorView.ChargeMode = ChargeMode;
    var ChargeView = (function (_super) {
        __extends(ChargeView, _super);

        function ChargeView(id, t0, originX, originY, phases, mode) {
            if (mode === void 0) {
                mode = ChargeMode.from();
            }
            var _this = _super.call(this) || this;
            _this.onPressDown = _this.onPressDown.bind(_this);
            _this.onChargeJitter = _this.onChargeJitter.bind(_this);
            _this.id = id;
            _this.t0 = t0;
            _this.originX.setState(originX);
            _this.originY.setState(originY);
            _this.centerX.setState(originX);
            _this.centerY.setState(originY);
            _this.chargeColor.setState(color.Color.fromAny(mode.chargeColor));
            _this.chargeOpacity.setState(mode.chargeOpacity);
            _this.chargeDarken.setState(mode.chargeDarken);
            _this.chargeRadius.setState(mode.chargeRadius);
            _this.chargeJitter.setState(0);
            _this.chargeJitterRadius.setState(mode.chargeJitterRadius);
            _this.rippleColor.setState(color.Color.fromAny(mode.rippleColor));
            _this.rippleOpacity.setState(mode.rippleOpacity);
            _this.rippleDarken.setState(mode.rippleDarken);
            var rippleCount = phases.length;
            var ripples = new Array(rippleCount);
            var _loop_1 = function (i) {
                var phase = phases[i];
                var ripple = new view.NumberMemberAnimator(this_1, 0);
                var ease = function (t) {
                    return Math.min(Math.max(0, 2 * t - (mode.rippleSpread / mode.rippleDuration) * phase), 1);
                };
                ripple.setState(1, transition.Transition.from(2 * mode.rippleDuration, ease));
                ripples[i] = ripple;
            };
            var this_1 = this;
            for (var i = 0; i < rippleCount; i += 1) {
                _loop_1(i);
            }
            _this.ripples = ripples;
            _this._phases = phases;
            if (mode.pressDelay > 0) {
                _this._pressed = false;
                _this._pressTimer = setTimeout(_this.onPressDown, mode.pressDelay);
            } else if (mode.pressDelay === 0) {
                _this._pressed = true;
                _this._pressTimer = 0;
                _this.onChargeJitter();
            } else {
                _this._pressed = false;
                _this._pressTimer = 0;
            }
            return _this;
        }

        Object.defineProperty(ChargeView.prototype, "viewController", {
            get: function () {
                return this._viewController;
            },
            enumerable: false,
            configurable: true
        });
        ChargeView.prototype.isActive = function () {
            return this.ripples.length !== 0;
        };
        ChargeView.prototype.isPressed = function () {
            return this._pressed;
        };
        ChargeView.prototype.setPressed = function (pressed) {
            if (this._pressed !== pressed) {
                this._pressed = pressed;
                if (this._pressed) {
                    this.onChargeJitter();
                }
            }
        };
        ChargeView.prototype.onUnmount = function () {
            if (this._pressTimer) {
                clearTimeout(this._pressTimer);
                this._pressTimer = 0;
            }
        };
        ChargeView.prototype.onAnimate = function (viewContext) {
            var t = viewContext.updateTime;
            this.originX.onFrame(t);
            this.originY.onFrame(t);
            this.centerX.onFrame(t);
            this.centerY.onFrame(t);
            this.chargeColor.onFrame(t);
            this.chargeOpacity.onFrame(t);
            this.chargeDarken.onFrame(t);
            this.chargeRadius.onFrame(t);
            this.chargeJitter.onFrame(t);
            this.chargeJitterRadius.onFrame(t);
            this.rippleColor.onFrame(t);
            this.rippleOpacity.onFrame(t);
            this.rippleDarken.onFrame(t);
            var ripples = this.ripples;
            for (var i = 0, n = ripples.length; i < n; i += 1) {
                ripples[i].onFrame(t);
            }
        };
        ChargeView.prototype.onRender = function (viewContext) {
            var context = viewContext.renderingContext;
            context.save();
            var bounds = this._bounds;
            if (this._pressed) {
                this.renderCharge(context, bounds);
            }
            this.renderRipples(context, bounds);
            context.restore();
        };
        ChargeView.prototype.renderCharge = function (context, bounds) {
            var centerX = this.centerX.value * bounds.width;
            var centerY = this.centerY.value * bounds.height;
            var chargeColor = this.chargeColor.value;
            var radius = Math.max(0, this.chargeRadius.value + this.chargeJitter.value * this.chargeJitterRadius.value);
            context.beginPath();
            context.arc(centerX, centerY, radius, 0, 2 * Math.PI);
            context.fillStyle = chargeColor.darker(this.chargeDarken.value).alpha(this.chargeOpacity.value).toString();
            context.fill();
        };
        ChargeView.prototype.renderRipples = function (context, bounds) {
            var originX = this.originX.value * bounds.width;
            var originY = this.originY.value * bounds.height;
            var rippleColor = this.rippleColor.value;
            var rippleOpacity = this.rippleOpacity.value;
            var rippleDarken = this.rippleDarken.value;
            var maxRadius = Math.max(bounds.width, bounds.height) / 2;
            var ripples = this.ripples;
            var i = 0;
            while (i < ripples.length) {
                var ripple = ripples[i];
                var phase = ripple.value;
                if (0 < phase && phase < 1) {
                    var radius = phase * maxRadius;
                    context.beginPath();
                    context.arc(originX, originY, radius, 0, 2 * Math.PI);
                    context.strokeStyle = rippleColor.darker(rippleDarken).alpha(rippleOpacity - rippleOpacity * phase).toString();
                    context.lineWidth = 1;
                    context.stroke();
                } else if (phase >= 1) {
                    ripples.splice(i, 1);
                    continue;
                }
                i += 1;
            }
        };
        ChargeView.prototype.onPressDown = function () {
            this._pressTimer = 0;
            this._pressed = true;
            var chargeRadius = this.chargeRadius.value;
            this.chargeRadius.setState(0);
            this.chargeRadius.setState(chargeRadius, transition.Transition.duration(300));
            this.onChargeJitter();
            var parentView = this.parentView;
            if (parentView instanceof MirrorView) {
                parentView.onPressHold(this);
            }
        };
        ChargeView.prototype.onChargeJitter = function () {
            var jitter = 0.5 - Math.random();
            this.chargeJitter.setState(jitter, transition.Transition.duration(10).onEnd(this.onChargeJitter));
        };
        ChargeView.prototype.onPressMove = function (centerX, centerY) {
            this.centerX.setState(centerX);
            this.centerY.setState(centerY);
        };
        ChargeView.prototype.onPressUp = function () {
            if (this._pressTimer) {
                clearTimeout(this._pressTimer);
                this._pressTimer = 0;
            }
            this._pressed = false;
        };
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "originX", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "originY", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "centerX", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "centerY", void 0);
        __decorate([
            view.MemberAnimator(color.Color)
        ], ChargeView.prototype, "chargeColor", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "chargeOpacity", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "chargeDarken", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "chargeRadius", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "chargeJitter", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "chargeJitterRadius", void 0);
        __decorate([
            view.MemberAnimator(color.Color)
        ], ChargeView.prototype, "rippleColor", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "rippleOpacity", void 0);
        __decorate([
            view.MemberAnimator(Number)
        ], ChargeView.prototype, "rippleDarken", void 0);
        return ChargeView;
    }(view.GraphicView));
    MirrorView.ChargeView = ChargeView;

    var SwimMirrorViewController = (function (_super) {
        __extends(SwimMirrorViewController, _super);

        function SwimMirrorViewController(nodeRef) {
            var _this = _super.call(this) || this;
            _this.nodeRef = nodeRef;
            _this.ripplesDownlink = null;
            _this.chargesDownlink = null;
            return _this;
        }

        SwimMirrorViewController.prototype.viewDidMount = function (view) {
            this.ripplesDownlink = this.nodeRef.downlink()
                .laneUri("ripples")
                .observe(this)
                .open();
            this.chargesDownlink = this.nodeRef.downlinkMap()
                .laneUri("charges")
                .observe(this)
                .open();
        };
        SwimMirrorViewController.prototype.viewDidUnmount = function (view) {
            if (this.ripplesDownlink) {
                this.ripplesDownlink.close();
                this.ripplesDownlink = null;
            }
            if (this.chargesDownlink) {
                this.chargesDownlink.close();
                this.chargesDownlink = null;
            }
        };
        SwimMirrorViewController.prototype.mirrorDidPressDown = function (charge, view) {
            var id = charge.id;
            var x = Math.round(charge.originX.state * 10000) / 10000;
            var y = Math.round(charge.originY.state * 10000) / 10000;
            var phaseCount = charge._phases.length;
            var phases = structure.Record.create(phaseCount);
            for (var i = 1; i < phaseCount; i += 1) {
                var phase = Math.round(charge._phases[i] * 100) / 100;
                phases.item(phase);
            }
            var color = charge.chargeColor.state.toString();
            var command = structure.Record.create(5)
                .attr("Ripple")
                .slot("id", id)
                .slot("x", x)
                .slot("y", y)
                .slot("phases", phases)
                .slot("color", color);
            this.nodeRef.command("ripple", command);
        };
        SwimMirrorViewController.prototype.mirrorDidPressHold = function (charge, view) {
            var id = charge.id;
            var x = Math.round(charge.centerX.state * 10000) / 10000;
            var y = Math.round(charge.centerY.state * 10000) / 10000;
            var r = charge.chargeRadius.state / 2;
            var color = charge.chargeColor.state.toString();
            var command = structure.Record.create(6)
                .attr("Remove")
                .slot("id", id)
                .slot("x", x)
                .slot("y", y)
                .slot("r", r)
                .slot("color", color);
            this.nodeRef.command("charge", command);
        };
        SwimMirrorViewController.prototype.mirrorDidPressMove = function (charge, view) {
            var id = charge.id;
            var x = Math.round(charge.centerX.state * 10000) / 10000;
            var y = Math.round(charge.centerY.state * 10000) / 10000;
            var r = charge.chargeRadius.state / 2;
            var color = charge.chargeColor.state.toString();
            var command = structure.Record.create(6)
                .attr("Move")
                .slot("id", id)
                .slot("x", x)
                .slot("y", y)
                .slot("r", r)
                .slot("color", color);
            this.nodeRef.command("charge", command);
        };
        SwimMirrorViewController.prototype.mirrorDidPressUp = function (charge, view) {
            var id = charge.id;
            var command = structure.Record.create(2)
                .attr("Remove")
                .slot("id", id);
            this.nodeRef.command("charge", command);
        };
        SwimMirrorViewController.prototype.onRemoteRipple = function (value) {
            var view = this.view;
            var id = value.get("id");
            if (document.hidden || id.getItem(0).equals(view.id)) {
                return;
            }
            var originX = value.get("x").numberValue(Math.random());
            var originY = value.get("y").numberValue(Math.random());
            var phases = value.get("phases").toAny();
            phases.unshift(0);
            var color = value.get("color").stringValue("#00a6ed");
            var mode = ChargeMode.from();
            mode.chargeRadius = 0;
            mode.chargeColor = color;
            if (view.mode.chargeOpacity) {
                mode.chargeOpacity = view.mode.chargeOpacity;
            }
            mode.rippleColor = color;
            if (view.mode.rippleDarken) {
                mode.rippleDarken = view.mode.rippleDarken;
            }
            if (view.mode.rippleOpacity) {
                mode.rippleOpacity = view.mode.rippleOpacity;
            }
            mode.pressDelay = -1;
            var charge = new ChargeView(id, Date.now(), originX, originY, phases, mode);
            view.appendChildView(charge);
        };
        SwimMirrorViewController.prototype.onRemoteUpdateCharge = function (key, value) {
            var view = this.view;
            if (key.getItem(0).equals(view.id)) {
                return;
            }
            var id = recon.Recon.toString(key);
            var t0 = value.get("t0").numberValue(Date.now());
            var centerX = value.get("x").numberValue(Math.random());
            var centerY = value.get("y").numberValue(Math.random());
            var chargeRadius = value.get("r").numberValue(0);
            if (!chargeRadius) {
                return;
            }
            var color = value.get("color").stringValue("#00a6ed");
            var tween = transition.Transition.duration(300);
            var charge = view.getChildView(id);
            if (charge) {
                charge.t0 = t0;
                charge.centerX.setState(centerX);
                charge.centerY.setState(centerY);
                charge.chargeColor(color, tween)
                    .chargeRadius(chargeRadius, tween)
                    .rippleColor(color, tween);
            } else {
                var mode = ChargeMode.from();
                mode.chargeRadius = 0;
                mode.chargeColor = color;
                if (view.mode.chargeDarken) {
                    mode.chargeDarken = view.mode.chargeDarken;
                }
                if (view.mode.chargeOpacity) {
                    mode.chargeOpacity = view.mode.chargeOpacity;
                }
                mode.rippleColor = color;
                if (view.mode.rippleDarken) {
                    mode.rippleDarken = view.mode.rippleDarken;
                }
                if (view.mode.rippleOpacity) {
                    mode.rippleOpacity = view.mode.rippleOpacity;
                }
                mode.pressDelay = 0;
                var charge_1 = new ChargeView(key, t0, centerX, centerY, [0], mode);
                charge_1.chargeRadius(chargeRadius, tween);
                view.setChildView(id, charge_1);
            }
        };
        SwimMirrorViewController.prototype.onRemoteRemoveCharge = function (key) {
            var view = this.view;
            if (key.getItem(0).equals(view.id)) {
                return;
            }
            var id = recon.Recon.toString(key);
            view.setChildView(id, null);
        };
        SwimMirrorViewController.prototype.removeAllCharges = function () {
            if (this.chargesDownlink) {
                this.chargesDownlink.forEach(function (key) {
                    this.onRemoteRemoveCharge(key);
                }, this);
            }
        };
        SwimMirrorViewController.prototype.onEvent = function (body, downlink) {
            if (downlink === this.ripplesDownlink) {
                this.onRemoteRipple(body);
            }
        };
        SwimMirrorViewController.prototype.didUpdate = function (key, newValue, oldValue, downlink) {
            if (downlink === this.chargesDownlink) {
                this.onRemoteUpdateCharge(key, newValue);
            }
        };
        SwimMirrorViewController.prototype.didRemove = function (key, oldValue, downlink) {
            if (downlink === this.chargesDownlink) {
                this.onRemoteRemoveCharge(key);
            }
        };
        SwimMirrorViewController.prototype.didConnect = function (downlink) {
            this.removeAllCharges();
        };
        SwimMirrorViewController.prototype.didDisconnect = function (downlink) {
            this.removeAllCharges();
        };
        return SwimMirrorViewController;
    }(MirrorViewController));

    exports.ChargeMode = ChargeMode;
    exports.ChargeView = ChargeView;
    exports.MirrorMode = MirrorMode;
    exports.MirrorView = MirrorView;
    exports.MirrorViewController = MirrorViewController;
    exports.SwimMirrorViewController = SwimMirrorViewController;

    Object.defineProperty(exports, '__esModule', {value: true});

}));
//# sourceMappingURL=swim-ripple.js.map
