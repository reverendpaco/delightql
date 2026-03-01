// segment_classifier.rs - Determine segment types (Join, SetOperation, Mixed)
//
// This module will classify segments based on their operators

use crate::pipeline::refiner::flattener::{FlatOperatorKind, FlatSegment};
use crate::pipeline::refiner::types::SegmentType;

/// Determine the type of segment based on its operators
pub(super) fn determine_segment_type(segment: &FlatSegment) -> SegmentType {
    if segment.operators.is_empty() {
        return SegmentType::Join; // Single table counts as join segment
    }

    let has_joins = segment
        .operators
        .iter()
        .any(|op| matches!(op.kind, FlatOperatorKind::Join { .. }));

    let has_setops = segment
        .operators
        .iter()
        .any(|op| matches!(op.kind, FlatOperatorKind::SetOp { .. }));

    match (has_joins, has_setops) {
        (true, false) => SegmentType::Join,
        (false, true) => SegmentType::SetOperation,
        (true, true) => SegmentType::Mixed,
        (false, false) => SegmentType::Join, // Shouldn't happen, but default to join
    }
}
