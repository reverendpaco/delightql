// Domain expression resolution helpers
// simple: column validation, ordinal resolution, literal conversion
// subqueries: scalar subquery resolution with fresh registry context
// projection: glob/pattern/range expansion for operator projection lists

pub(in crate::pipeline::resolver) mod projection;
pub(in crate::pipeline::resolver) mod simple;
pub(in crate::pipeline::resolver) mod subqueries;
