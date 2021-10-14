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

enum class FfiDataType : int32_t {
  Unknown = 0,
  Boolean = 1,
  I32 = 2,
  I64 = 3,
  F64 = 4,
  Str = 5,
};

enum class FfiDirection : int32_t {
  Out = 0,
  In = 1,
  Both = 2,
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

enum class FfiOrderOpt : int32_t {
  Shuffle = 0,
  Asc = 1,
  Desc = 2,
};

enum class FfiPropertyOpt : int32_t {
  None = 0,
  Id = 1,
  Label = 2,
  Key = 3,
};

enum class FfiScanOpt : int32_t {
  Vertex = 0,
  Edge = 1,
  Table = 2,
};

enum class ResultCode : int32_t {
  Success = 0,
  /// Parse an expression error
  ParseExprError = 1,
  /// Query an object that does not exist
  NotExistError = 2,
  /// The error while transforming from C-like string, aka char*
  CStringError = 3,
  /// The provided data type is unknown
  UnknownTypeError = 4,
  /// The provided range is invalid
  InvalidRangeError = 5,
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

struct FfiConst {
  FfiDataType data_type;
  bool boolean;
  int32_t int32;
  int64_t int64;
  double float64;
  const char *cstr;
  const void *raw;
};

extern "C" {

/// Transform a c-like string into `NameOrId`
FfiNameOrId cstr_as_name_or_id(const char *cstr);

/// Transform an integer into `NameOrId`.
FfiNameOrId int_as_name_or_id(int32_t integer);

/// Build an id property
FfiProperty as_id_key();

/// Build a label property
FfiProperty as_label_key();

/// Build a keyed property from a given key
FfiProperty as_property_key(FfiNameOrId key);

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
const void *init_project_operator(bool is_append);

/// To add a mapping for the project operator, which maps a c-like string to represent an
/// expression, to a `NameOrId` parameter that represents an alias.
ResultCode add_project_mapping(const void *ptr_project,
                               const char *cstr_expr,
                               FfiNameOrId alias,
                               bool is_query_given);

/// Append a project operator to the logical plan. To do so, one specifies the following arguments:
/// * `ptr_plan`: A rust-owned pointer created by `init_logical_plan()`.
/// * `ptr_project`: A rust-owned pointer created by `init_project_operator()`.
/// * `parent_id`: The unique parent operator's index in the logical plan.
/// * `id`: An index pointer that gonna hold the index for this operator.
///
/// If it is successful to be appended to the logical plan, the `ptr_project` will be
/// automatically released by by the rust program. Therefore, the caller needs not to deallocate
/// the pointer, and must **not** use it thereafter.
///
/// Otherwise, user can manually call [`destroy_project_operator()`] to release the pointer.
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

void destroy_project_operator(const void *ptr);

/// To initialize a select operator
const void *init_select_operator();

/// To set a select operator's metadata, which is a predicate represented as a c-string.
ResultCode add_select_predicate(const void *ptr_select, const char *cstr_predicate);

/// Append a select operator to the logical plan
ResultCode append_select_operator(const void *ptr_plan,
                                  const void *ptr_select,
                                  int32_t parent_id,
                                  int32_t *id);

void destroy_select_operator(const void *ptr);

/// To initialize a join operator
const void *init_join_operator(FfiJoinKind join_kind);

/// To add a join operator's metadata, which is a pair of left and right keys.
/// In the join processing, a pair of data will be output if the corresponding fields
/// regarding left and right keys are **equivalent**.
ResultCode add_join_key_pair(const void *ptr_join, FfiVariable left_key, FfiVariable right_key);

/// Append a join operator to the logical plan
ResultCode append_join_operator(const void *ptr_plan,
                                const void *ptr_join,
                                int32_t parent_left,
                                int32_t parent_right,
                                int32_t *id);

void destroy_join_operator(const void *ptr);

/// To initialize a union operator
const void *init_union_operator();

/// Append a union operator to the logical plan
ResultCode append_union_operator(const void *ptr_plan,
                                 const void *ptr_union,
                                 int32_t parent_left,
                                 int32_t parent_right,
                                 int32_t *id);

/// To initialize a groupby operator
const void *init_groupby_operator();

/// The group function actually requires a collection of variables. Right now we
/// provide the support of just one variable cause it suits for most cases already.
/// TODO(longbin) Will provide the support for multiple grouping variables
FfiAggFn build_agg_fn(FfiVariable agg_var, FfiAggOpt aggregate, FfiNameOrId alias);

/// Add the key according to which the grouping is conducted
ResultCode add_groupby_key(const void *ptr_groupby, FfiVariable key);

/// Add the aggregate function for each group.
ResultCode add_groupby_agg_fn(const void *ptr_groupby, FfiAggFn agg_fn);

/// Append a groupby operator to the logical plan
ResultCode append_groupby_operator(const void *ptr_plan,
                                   const void *ptr_groupby,
                                   int32_t parent,
                                   int32_t *id);

void destroy_groupby_operator(const void *ptr);

/// To initialize an orderby operator
const void *init_orderby_operator();

/// Add the pair for conducting ordering.
ResultCode add_orderby_pair(const void *ptr_orderby, FfiVariable var, FfiOrderOpt order_opt);

/// Append an orderby operator to the logical plan
ResultCode append_orderby_operator(const void *ptr_plan,
                                   const void *ptr_orderby,
                                   int32_t parent,
                                   int32_t *id);

void destroy_orderby_operator(const void *ptr);

/// To initialize a dedup operator
const void *init_dedup_operator();

/// Add a key for de-duplicating.
ResultCode add_dedup_key(const void *ptr_dedup, FfiVariable var);

/// Append a dedup operator to the logical plan
ResultCode append_dedup_operator(const void *ptr_plan,
                                 const void *ptr_dedup,
                                 int32_t parent,
                                 int32_t *id);

void destroy_dedup_operator(const void *ptr);

/// To initialize an unfold operator
const void *init_unfold_operator();

/// Set the argument pair for unfold, which are:
/// * a tag points to a collection-type data field for unfolding,
/// * an alias for referencing to each element of the collection.
ResultCode set_unfold_pair(const void *ptr_unfold, FfiNameOrId tag, FfiNameOrId alias);

/// Append an unfold operator to the logical plan
ResultCode append_unfold_operator(const void *ptr_plan,
                                  const void *ptr_unfold,
                                  int32_t parent,
                                  int32_t *id);

void destroy_unfold_operator(const void *ptr);

/// To initialize a scan operator
const void *init_scan_operator(FfiScanOpt scan_opt);

ResultCode set_scan_limit(const void *ptr_scan, int32_t lower, int32_t upper);

ResultCode set_scan_schema_name(const void *ptr_scan, const char *cstr);

/// Add a mapping from the original data field name to an alias
ResultCode add_scan_data_field(const void *ptr_scan, FfiNameOrId field_name);

/// Append a scan operator to the logical plan
ResultCode append_scan_operator(const void *ptr_plan,
                                const void *ptr_scan,
                                int32_t parent,
                                int32_t *id);

void destroy_scan_operator(const void *ptr);

/// To initialize an indexed-scan operator from a scan operator
const void *init_idxscan_operator(const void *ptr_scan);

FfiConst boolean_as_const(bool boolean);

FfiConst int32_as_const(int32_t int32);

FfiConst int64_as_const(int64_t int64);

FfiConst f64_as_const(double float64);

FfiConst cstr_as_const(const char *cstr);

const void *init_kv_equiv_pairs();

ResultCode and_kv_equiv_pair(const void *ptr_pairs, FfiProperty key, FfiConst value);

ResultCode add_idxscan_kv_equiv_pairs(const void *ptr_idxscan, const void *ptr_pairs);

/// Append an indexed scan operator to the logical plan
ResultCode append_idxscan_operator(const void *ptr_plan,
                                   const void *ptr_idxscan,
                                   int32_t parent,
                                   int32_t *id);

void destroy_idxscan_operator(const void *ptr);

const void *init_limit_operator(bool is_topk);

ResultCode set_limit_range(const void *ptr_limit, int32_t lower, int32_t upper);

/// Append an indexed scan operator to the logical plan
ResultCode append_limit_operator(const void *ptr_plan,
                                 const void *ptr_limit,
                                 int32_t parent,
                                 int32_t *id);

void destroy_limit_operator(const void *ptr);

/// To initialize an expansion base
const void *init_expand_base(FfiDirection direction);

/// Set the start-vertex's tag to conduct this expansion
ResultCode set_expand_vtag(const void *ptr_expand, FfiNameOrId v_tag);

/// Add a label of the edge that this expansion must satisfy
ResultCode add_expand_label(const void *ptr_expand, FfiNameOrId label);

/// Add a property that this edge expansion must carry
ResultCode add_expand_property(const void *ptr_expand, FfiNameOrId property);

/// Set the size range limitation of this expansion
ResultCode set_expand_limit(const void *ptr_expand, int32_t lower, int32_t upper);

/// Set the edge predicate of this expansion
ResultCode set_expand_predicate(const void *ptr_expand, const char *cstr_predicate);

/// To initialize an edge expand operator from an expand base
const void *init_edgexpd_operator(const void *ptr_expand);

/// Set edge alias of this edge expansion
ResultCode set_edgexpd_alias(const void *ptr_edgexpd, FfiNameOrId alias);

/// Append an edge expand operator to the logical plan
ResultCode append_edgexpd_operator(const void *ptr_plan,
                                   const void *ptr_edgexpd,
                                   int32_t parent,
                                   int32_t *id);

void destroy_edgexpd_operator(const void *ptr);

/// To initialize an expansion base
const void *init_getv_operator();

/// Set the tag of edge/path to get its end vertex
ResultCode set_getv_tag(const void *ptr_getv, FfiNameOrId tag);

/// Set vertex alias of this getting vertex
ResultCode set_getv_alias(const void *ptr_getv, FfiNameOrId alias);

/// Add a label of the vertex that this getv must satisfy
ResultCode add_getv_label(const void *ptr_getv, FfiNameOrId label);

/// Add a property that this vertex must carry
ResultCode add_getv_property(const void *ptr_getv, FfiNameOrId property);

/// Set the size range limitation of getting vertices
ResultCode set_getv_limit(const void *ptr_getv, int32_t lower, int32_t upper);

/// Append an edge expand operator to the logical plan
ResultCode append_getv_operator(const void *ptr_plan,
                                const void *ptr_getv,
                                int32_t parent,
                                int32_t *id);

void destroy_getv_operator(const void *ptr);

/// To initialize an path expand operator from an expand base
const void *init_pathxpd_operator(const void *ptr_expand);

/// Set path alias of this path expansion
ResultCode set_pathxpd_alias(const void *ptr_edgexpd, FfiNameOrId alias);

/// Set the hop-range limitation of expanding path
ResultCode set_pathxpd_hops(const void *ptr_pathxpd, int32_t lower, int32_t upper);

/// Append an path-expand operator to the logical plan
ResultCode append_pathxpd_operator(const void *ptr_plan,
                                   const void *ptr_pathxpd,
                                   int32_t parent,
                                   int32_t *id);

void destroy_pathxpd_operator(const void *ptr);

} // extern "C"
