use crate::agent::UplinkKind;
use crate::lane::WarpLaneKind;

// spatial lanes are omitted as they are unimplemented
const LANE_KINDS: [WarpLaneKind; 8] = [
    WarpLaneKind::Command,
    WarpLaneKind::Demand,
    WarpLaneKind::DemandMap,
    WarpLaneKind::Map,
    WarpLaneKind::JoinMap,
    WarpLaneKind::JoinValue,
    WarpLaneKind::Supply,
    WarpLaneKind::Value,
];

#[test]
fn uplink_kinds() {
    for kind in LANE_KINDS {
        let uplink_kind = kind.uplink_kind();
        if kind.map_like() {
            assert_eq!(uplink_kind, UplinkKind::Map);
        } else if matches!(kind, WarpLaneKind::Supply) {
            assert_eq!(uplink_kind, UplinkKind::Supply);
        } else {
            assert_eq!(uplink_kind, UplinkKind::Value)
        }
    }
}

// this is here for when spatial lanes are implemented as the test will no longer panic
#[test]
#[should_panic]
fn spatial_uplink_kind() {
    WarpLaneKind::Spatial.uplink_kind();
}
