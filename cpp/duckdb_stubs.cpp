//===----------------------------------------------------------------------===//
//
//                         DuckArrow
//
// duckdb_stubs.cpp
//
//===----------------------------------------------------------------------===//
//
// Stub implementations for DuckDB class member functions.
// These generate vtables and typeinfo symbols that DuckDB's binary doesn't export.
//
// WARNING: These are minimal implementations. They may throw exceptions or
// return placeholder values. The extension should work as long as DuckDB
// doesn't call these functions on our objects (it typically won't since
// we're providing our own derived class implementations).
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// CreateInfo stubs (base class for our CreateSchemaInfo/CreateTableInfo)
//===----------------------------------------------------------------------===//

void CreateInfo::Serialize(Serializer & /* serializer */) const {
    throw NotImplementedException("CreateInfo::Serialize not available in duckarrow");
}

void CreateInfo::CopyProperties(CreateInfo &other) const {
    other.type = type;
    other.catalog = catalog;
    other.schema = schema;
    other.on_conflict = on_conflict;
    other.temporary = temporary;
    other.internal = internal;
    other.sql = sql;
}

unique_ptr<AlterInfo> CreateInfo::GetAlterInfo() const {
    throw NotImplementedException("CreateInfo::GetAlterInfo not available in duckarrow");
}

//===----------------------------------------------------------------------===//
// CatalogEntry stubs - destructor is key function
//===----------------------------------------------------------------------===//

CatalogEntry::~CatalogEntry() {
}

unique_ptr<CatalogEntry> CatalogEntry::Copy(ClientContext & /* context */) const {
    throw NotImplementedException("CatalogEntry::Copy not available in duckarrow");
}

unique_ptr<CreateInfo> CatalogEntry::GetInfo() const {
    throw NotImplementedException("CatalogEntry::GetInfo not available in duckarrow");
}

SchemaCatalogEntry &CatalogEntry::ParentSchema() {
    throw NotImplementedException("CatalogEntry::ParentSchema not available in duckarrow");
}

const SchemaCatalogEntry &CatalogEntry::ParentSchema() const {
    throw NotImplementedException("CatalogEntry::ParentSchema not available in duckarrow");
}

Catalog &CatalogEntry::ParentCatalog() {
    throw NotImplementedException("CatalogEntry::ParentCatalog not available in duckarrow");
}

const Catalog &CatalogEntry::ParentCatalog() const {
    throw NotImplementedException("CatalogEntry::ParentCatalog not available in duckarrow");
}

string CatalogEntry::ToSQL() const {
    return "";
}

//===----------------------------------------------------------------------===//
// SchemaCatalogEntry stubs
//===----------------------------------------------------------------------===//

unique_ptr<CreateInfo> SchemaCatalogEntry::GetInfo() const {
    throw NotImplementedException("SchemaCatalogEntry::GetInfo not available in duckarrow");
}

string SchemaCatalogEntry::ToSQL() const {
    return "";
}

//===----------------------------------------------------------------------===//
// Catalog stubs
//===----------------------------------------------------------------------===//

string Catalog::GetDefaultSchema() const {
    return DEFAULT_SCHEMA;
}

//===----------------------------------------------------------------------===//
// ColumnList stubs
//===----------------------------------------------------------------------===//

ColumnList ColumnList::Copy() const {
    ColumnList result;
    for (auto &col : columns) {
        result.AddColumn(col.Copy());
    }
    return result;
}

//===----------------------------------------------------------------------===//
// FunctionData stubs - destructor is key function
//===----------------------------------------------------------------------===//

FunctionData::~FunctionData() {
}

bool FunctionData::SupportStatementCache() const {
    return false;
}

//===----------------------------------------------------------------------===//
// EntryLookupInfo stubs
//===----------------------------------------------------------------------===//

CatalogType EntryLookupInfo::GetCatalogType() const {
    return CatalogType::INVALID;
}

const string &EntryLookupInfo::GetEntryName() const {
    static string empty;
    return empty;
}

//===----------------------------------------------------------------------===//
// Value stubs
//===----------------------------------------------------------------------===//

template <>
string Value::GetValue<string>() const {
    return ToString();
}

string Value::ToString() const {
    // Minimal implementation - just return the string representation
    if (IsNull()) {
        return "NULL";
    }
    // For string values, return the internal string
    if (type().id() == LogicalTypeId::VARCHAR) {
        return StringValue::Get(*this);
    }
    return "Value::ToString stub";
}

//===----------------------------------------------------------------------===//
// ColumnDefinition stubs
//===----------------------------------------------------------------------===//

ColumnDefinition ColumnDefinition::Copy() const {
    return ColumnDefinition(name, type);
}

//===----------------------------------------------------------------------===//
// Catalog stubs - virtual destructor generates typeinfo
//===----------------------------------------------------------------------===//

Catalog::~Catalog() {
}

//===----------------------------------------------------------------------===//
// Transaction stubs
//===----------------------------------------------------------------------===//

Transaction::~Transaction() {
}

//===----------------------------------------------------------------------===//
// TransactionManager stubs
//===----------------------------------------------------------------------===//

TransactionManager::~TransactionManager() {
}

//===----------------------------------------------------------------------===//
// InCatalogEntry stubs - destructor and Verify are key functions
//===----------------------------------------------------------------------===//

InCatalogEntry::~InCatalogEntry() {
}

void InCatalogEntry::Verify(Catalog & /* catalog */) {
    // No-op verification for duckarrow entries
}

//===----------------------------------------------------------------------===//
// Force typeinfo emission for classes with inline-only virtual functions
// By defining a non-inline virtual function, we become the "key function"
// and the compiler emits typeinfo in this translation unit.
//===----------------------------------------------------------------------===//

// For ParseInfo - define Serialize (declared but not inline)
void ParseInfo::Serialize(Serializer & /* serializer */) const {
    throw NotImplementedException("ParseInfo::Serialize not available in duckarrow");
}

// For FunctionData - define Equals (declared virtual, not inline)
bool FunctionData::Equals(const FunctionData & /* other */) const {
    return false;
}

// For Transaction - it has virtual destructor inline, need another virtual
// Looking at the class, StartTransaction might work
// Actually Transaction has pure virtual functions, so we can't instantiate it
// But we can provide a definition for a virtual function

// For LocalTableFunctionState and GlobalTableFunctionState
// These have virtual destructors that are inline. We need to add a cpp definition.
LocalTableFunctionState::~LocalTableFunctionState() {
}

GlobalTableFunctionState::~GlobalTableFunctionState() {
}

//===----------------------------------------------------------------------===//
// Force typeinfo by explicit instantiation via typeid
// This is a backup approach - the compiler must emit typeinfo if we use typeid
//===----------------------------------------------------------------------===//

namespace {
// Helper to force typeinfo emission - the compiler must generate typeinfo
// for any type used with typeid()
template<typename T>
const std::type_info& force_typeinfo() {
    return typeid(T);
}

// Force emission at static initialization time
struct TypeInfoForcer {
    TypeInfoForcer() {
        // These calls force the compiler to emit typeinfo for these types
        (void)force_typeinfo<duckdb::ParseInfo>();
        (void)force_typeinfo<duckdb::FunctionData>();
        (void)force_typeinfo<duckdb::Transaction>();
        (void)force_typeinfo<duckdb::TransactionManager>();
        (void)force_typeinfo<duckdb::Catalog>();
        (void)force_typeinfo<duckdb::InCatalogEntry>();
        (void)force_typeinfo<duckdb::LocalTableFunctionState>();
        (void)force_typeinfo<duckdb::GlobalTableFunctionState>();
        (void)force_typeinfo<duckdb::Function>();
        (void)force_typeinfo<duckdb::SimpleFunction>();
        (void)force_typeinfo<duckdb::SimpleNamedParameterFunction>();
    }
};

static TypeInfoForcer s_typeinfo_forcer;
}

//===----------------------------------------------------------------------===//
// Additional constructor and function stubs
//===----------------------------------------------------------------------===//

// Catalog constructor and methods
Catalog::Catalog(AttachedDatabase &db_p) : db(db_p) {
}

void Catalog::Initialize(optional_ptr<ClientContext> /* context */, bool /* load_builtin */) {
}

void Catalog::FinalizeLoad(optional_ptr<ClientContext> /* context */) {
}

void Catalog::OnDetach(ClientContext & /* context */) {
}

void Catalog::Verify() {
}

optional_ptr<DependencyManager> Catalog::GetDependencyManager() {
    return nullptr;
}

vector<MetadataBlockInfo> Catalog::GetMetadataInfo(ClientContext & /* context */) {
    throw NotImplementedException("Catalog::GetMetadataInfo not available");
}

bool Catalog::HasConflictingAttachOptions(const string & /* path */, const AttachOptions & /* options */) {
    return false;
}

bool Catalog::CheckAmbiguousCatalogOrSchema(ClientContext & /* context */, const string & /* name */) {
    return false;
}

PhysicalOperator &Catalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &, PhysicalOperator &child) {
    throw NotImplementedException("Catalog::PlanUpdate not available");
}

PhysicalOperator &Catalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &) {
    throw NotImplementedException("Catalog::PlanUpdate not available");
}

PhysicalOperator &Catalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &, PhysicalOperator &child) {
    throw NotImplementedException("Catalog::PlanDelete not available");
}

PhysicalOperator &Catalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &) {
    throw NotImplementedException("Catalog::PlanDelete not available");
}

PhysicalOperator &Catalog::PlanMergeInto(ClientContext &, PhysicalPlanGenerator &, LogicalMergeInto &, PhysicalOperator &) {
    throw NotImplementedException("Catalog::PlanMergeInto not available");
}

unique_ptr<LogicalOperator> Catalog::BindCreateIndex(Binder &, CreateStatement &, TableCatalogEntry &, unique_ptr<LogicalOperator>) {
    throw NotImplementedException("Catalog::BindCreateIndex not available");
}

unique_ptr<LogicalOperator> Catalog::BindAlterAddIndex(Binder &, TableCatalogEntry &, unique_ptr<LogicalOperator>, unique_ptr<CreateIndexInfo>, unique_ptr<AlterTableInfo>) {
    throw NotImplementedException("Catalog::BindAlterAddIndex not available");
}

// CatalogEntry constructor
CatalogEntry::CatalogEntry(CatalogType type_p, Catalog &catalog_p, string name_p)
    : type(type_p), set(nullptr), name(std::move(name_p)) {
    // Note: We don't initialize catalog reference here because it requires passing to parent
}

void CatalogEntry::Verify(Catalog & /* catalog */) {
    // No-op verification
}

// CatalogEntry methods
void CatalogEntry::SetAsRoot() {
}

void CatalogEntry::OnDrop() {
}

void CatalogEntry::Rollback(CatalogEntry & /* parent */) {
}

void CatalogEntry::UndoAlter(ClientContext & /* context */, AlterInfo & /* info */) {
}

unique_ptr<CatalogEntry> CatalogEntry::AlterEntry(CatalogTransaction /* transaction */, AlterInfo & /* info */) {
    throw NotImplementedException("CatalogEntry::AlterEntry not available");
}

unique_ptr<CatalogEntry> CatalogEntry::AlterEntry(ClientContext & /* context */, AlterInfo & /* info */) {
    throw NotImplementedException("CatalogEntry::AlterEntry not available");
}

// InCatalogEntry constructor
InCatalogEntry::InCatalogEntry(CatalogType type, Catalog &catalog_p, string name_p)
    : CatalogEntry(type, catalog_p, std::move(name_p)), catalog(catalog_p) {
}

// SchemaCatalogEntry constructor - CreateSchemaInfo is incomplete but we need the symbol
// This constructor should never be called in practice since our extension creates schemas differently
SchemaCatalogEntry::SchemaCatalogEntry(Catalog &catalog_p, CreateSchemaInfo & /* info */)
    : InCatalogEntry(CatalogType::SCHEMA_ENTRY, catalog_p, "stub_schema") {
    // Note: Cannot access info.schema since CreateSchemaInfo is incomplete
    // This stub just satisfies the linker
}

CatalogSet::EntryLookup SchemaCatalogEntry::LookupEntryDetailed(CatalogTransaction /* transaction */, const EntryLookupInfo & /* info */) {
    return CatalogSet::EntryLookup();
}

SimilarCatalogEntry SchemaCatalogEntry::GetSimilarEntry(CatalogTransaction /* transaction */, const EntryLookupInfo & /* info */) {
    return SimilarCatalogEntry();
}

// Transaction
Transaction::Transaction(TransactionManager &manager_p, ClientContext & /* context */) : manager(manager_p) {
}

void Transaction::SetReadWrite() {
}

// TransactionManager
TransactionManager::TransactionManager(AttachedDatabase &db_p) : db(db_p) {
}

// StringUtil
bool StringUtil::StartsWith(string str, string prefix) {
    return str.rfind(prefix, 0) == 0;
}

string StringUtil::Lower(const string &str) {
    string result = str;
    for (auto &c : result) {
        c = tolower(c);
    }
    return result;
}

string StringUtil::Upper(const string &str) {
    string result = str;
    for (auto &c : result) {
        c = toupper(c);
    }
    return result;
}

bool StringUtil::CIEquals(const string &a, const string &b) {
    return Lower(a) == Lower(b);
}

idx_t StringUtil::CIHash(const string &str) {
    return std::hash<string>{}(Lower(str));
}

// StringValue
const string &StringValue::Get(const Value &value) {
    // This is a bit hacky - returning a reference to a static for stub purposes
    static thread_local string result;
    result = value.ToString();
    return result;
}

// LogicalType
LogicalType::LogicalType() : id_(LogicalTypeId::INVALID) {
}

LogicalType::LogicalType(LogicalTypeId id) : id_(id) {
}

LogicalType::LogicalType(const LogicalType &other) : id_(other.id_) {
}

LogicalType::LogicalType(LogicalType &&other) noexcept : id_(other.id_) {
}

LogicalType::~LogicalType() {
}

LogicalType LogicalType::JSON() {
    return LogicalType(LogicalTypeId::VARCHAR);  // Fallback
}

LogicalType LogicalType::DECIMAL(uint8_t /* width */, uint8_t /* scale */) {
    return LogicalType(LogicalTypeId::DECIMAL);
}

// ColumnDefinition
ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p)
    : name(std::move(name_p)), type(std::move(type_p)) {
}

// ColumnList
ColumnList::ColumnList(bool /* allow_duplicate_names */) {
}

void ColumnList::AddColumn(ColumnDefinition col) {
    columns.push_back(std::move(col));
}

// Value
Value::Value(LogicalType /* type */) {
}

Value::Value(Value && /* other */) noexcept {
}

Value::~Value() {
}

// Base Exception constructor
Exception::Exception(ExceptionType type, const string &msg) : std::runtime_error(msg) {
}

// Exception constructors
NotImplementedException::NotImplementedException(const string &msg) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {
}

InternalException::InternalException(const string &msg) : Exception(ExceptionType::INTERNAL, msg) {
}

IOException::IOException(const string &msg) : Exception(ExceptionType::IO, msg) {
}

CatalogException::CatalogException(const string &msg) : Exception(ExceptionType::CATALOG, msg) {
}

// ErrorData
ErrorData::ErrorData() {
}

// hugeint_t
hugeint_t::hugeint_t(int64_t value) : lower(static_cast<uint64_t>(value)), upper(value >= 0 ? 0 : -1) {
}

// EnumUtil
template<>
const char *EnumUtil::ToChars<ParseInfoType>(ParseInfoType value) {
    return "UNKNOWN";
}

// ExceptionFormatValue
ExceptionFormatValue::ExceptionFormatValue(string value) {
    // Minimal implementation
}

template<>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue<string>(string value) {
    return ExceptionFormatValue(std::move(value));
}

string Exception::ConstructMessageRecursive(const string &msg, std::vector<ExceptionFormatValue> & /* values */) {
    return msg;
}

// Function constructor and destructor
Function::Function(string name_p) : name(std::move(name_p)) {
}

Function::~Function() {
}

// SimpleFunction constructor and destructor
SimpleFunction::SimpleFunction(string name_p, vector<LogicalType> arguments_p, LogicalType varargs_p)
    : Function(std::move(name_p)), arguments(std::move(arguments_p)), varargs(std::move(varargs_p)) {
}

SimpleFunction::~SimpleFunction() {
}

// SimpleFunction virtual methods
string SimpleFunction::ToString() const {
    return name;
}

// SimpleNamedParameterFunction constructor and destructor
SimpleNamedParameterFunction::SimpleNamedParameterFunction(string name_p, vector<LogicalType> arguments_p, LogicalType varargs_p)
    : SimpleFunction(std::move(name_p), std::move(arguments_p), std::move(varargs_p)) {
}

SimpleNamedParameterFunction::~SimpleNamedParameterFunction() {
}

// SimpleNamedParameterFunction virtual methods
string SimpleNamedParameterFunction::ToString() const {
    return name;
}

bool SimpleNamedParameterFunction::HasNamedParameters() const {
    return !named_parameters.empty();
}

// TableFunction constructor
TableFunction::TableFunction(string name, vector<LogicalType> arguments,
                            table_function_t function, table_function_bind_t bind,
                            table_function_init_global_t init_global,
                            table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), std::move(arguments)),
      bind(bind), init_global(init_global), init_local(init_local), function(function) {
}

// DBConfig - GetConfig is exported by DuckDB, we don't stub it

} // namespace duckdb
