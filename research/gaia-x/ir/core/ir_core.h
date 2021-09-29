#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

enum class FfiAggOpt : int32_t {
  Sum = 0,
  Min = 1,
  Max = 2,
  Count = 3,
  CountDistinct = 4,
  ToList = 5,
  ToSet = 6,
  Avg = 7,
};

enum class FfiJoinKind : int32_t {
  /// Inner join
  Inner = 0,
  /// Left outer join
  LeftOuter = 1,
  /// Right outer join
  RightOuter = 2,
  /// Full outer join
  FullOuter = 3,
  /// Left semi-join, right alternative can be naturally adapted
  Semi = 4,
  /// Left anti-join, right alternative can be naturally adapted
  Anti = 5,
  /// aka. Cartesian product
  Times = 6,
};

enum class FfiNameIdOpt : int32_t {
  None = 0,
  Name = 1,
  Id = 2,
};

enum class FfiPropertyOpt : int32_t {
  None = 0,
  Id = 1,
  Label = 2,
  Key = 3,
};

enum class ResultCode : int32_t {
  Success = 0,
  /// Parse an expression error
  ParseExprError = 1,
  /// Query an object that does not exist
  NotExistError = 2,
  /// The error while transforming from C-like string, aka char*
  CStringError = 3,
  /// Negative index
  NegativeIndexError = 4,
};

struct FfiNameOrId {
  FfiNameIdOpt opt;
  const char *name;
  int32_t name_id;
};

struct FfiProperty {
  FfiPropertyOpt opt;
  FfiNameOrId key;
};

struct FfiVariable {
  FfiNameOrId tag;
  FfiProperty property;
};

struct FfiAggFn {
  const FfiVariable *vars;
  FfiAggOpt aggregate;
  FfiNameOrId alias;
};

extern "C" {

/// Transform a c-like string into `NameOrId`
///
/// .
FfiNameOrId as_tag_name(const char *name);

/// Transform an integer into `NameOrId`.
FfiNameOrId as_tag_id(int32_t name_id);

/// Build a variable
FfiVariable as_var(FfiNameOrId tag);

/// Build variable with property
FfiVariable as_var_ppt(FfiNameOrId tag, FfiProperty property);

/// Initialize a logical plan, which expose a pointer for c-like program to access the
/// entry of the logical plan. This pointer, however, is owned by Rust, and the caller
/// **must not** process any operation, which includes but not limited to deallocate it.
/// We have provided  the [`destroy_logical_plan`] api for deallocating the pointer of the logical plan.
const void *init_logical_plan();

/// To destroy a logical plan.
void destroy_logical_plan(const void *ptr_plan);

void debug_plan(const void *ptr_plan);

/// To initialize a project operator.
const void *init_project_operator();

/// To add a meta data for the project operator, which is a c-like string to represent an
/// expression, together with a `NameOrId` parameter that represents an alias.
ResultCode add_project_meta(const void *ptr_project, const char *expr, FfiNameOrId alias);

/// Append a project operator to the logical plan. To do so, one specifies the following arguments:
/// * `ptr_plan`: A rust-owned pointer created by `init_logical_plan()`.
/// * `ptr_project`: A rust-owned pointer created by `init_project_operator()`.
/// * `parent_id`: The unique parent operator's index in the logical plan.
/// * `id`: An index pointer that gonna hold the index for this operator.
///
/// After successfully appending to the logical plan, the `ptr_project` shall be released by
/// by the rust program. Therefore, the caller needs not to deallocate the pointer, and must
/// **not** use it thereafter.
///
/// # Return
/// * Returning [`ResultCode`] to capture any error.
///
/// **Note**: All following `append_xx_operator()` apis have the same usage as this one.
///
ResultCode append_project_operator(const void *ptr_plan,
                                   const void *ptr_project,
                                   int32_t parent_id,
                                   int32_t *id);

/// To initialize a select operator
const void *init_select_operator();

/// To add a select operator's metadata, which is a predicate represented as a c-string.
/// Note that, we use **add** here to make apis consistent. If multiple adds are conducted,
/// only the latest one is kept.
ResultCode add_select_meta(const void *ptr_select, const char *ptr_predicate);

/// Append a select operator to the logical plan
ResultCode append_select_operator(const void *ptr_plan,
                                  const void *ptr_select,
                                  int32_t parent_id,
                                  int32_t *id);

/// To initialize a join operator
const void *init_join_operator(FfiJoinKind join_kind);

/// To add a join operator's metadata, which is a pair of left and right keys.
/// In the join processing, a pair of data will be output if the corresponding fields
/// regarding left and right keys are **equivalent**.
ResultCode add_join_meta(const void *ptr_join, FfiVariable left_key, FfiVariable right_key);

/// Append a join operator to the logical plan
ResultCode append_join_operator(const void *ptr_plan,
                                const void *ptr_join,
                                int32_t parent_left,
                                int32_t parent_right,
                                int32_t *id);

/// To initialize a union operator
const void *init_union_operator();

/// Append a union operator to the logical plan
ResultCode append_union_operator(const void *ptr_plan,
                                 const void *ptr_union,
                                 int32_t parent_left,
                                 int32_t parent_right,
                                 int32_t *id);

/// To initialize a union operator
const void *init_groupby_operator();

/// The group function actually requires a collection of variables. Right now we
/// provide the support of just one variable cause it suits for most cases already.
/// TODO(longbin) Will provide the support for multiple grouping variables
FfiAggFn build_agg_fn(FfiVariable agg_var, FfiAggOpt aggregate, FfiNameOrId alias);

ResultCode add_agg_fn(const void *ptr_group, FfiAggFn agg_fn);

ResultCode add_grouping_key(const void *ptr_group, FfiVariable key);

/// Append a union operator to the logical plan
ResultCode append_groupby_operator(const void *ptr_plan,
                                   const void *ptr_group,
                                   int32_t parent,
                                   int32_t *id);

} // extern "C"
