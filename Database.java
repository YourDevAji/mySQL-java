import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

// DATABASE CONNECTION CLASS WITH TRANSACTION SUPPORT
class Database {
    private final ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();
    private final String url;
    private final String user;
    private final String password;

    public Database(String dbName, String user, String password) {
        this.url = "jdbc:mysql://localhost:3306/" + dbName + "?useSSL=false&serverTimezone=UTC";
        this.user = user;
        this.password = password;
    }

    private Connection getConnection() throws SQLException {
        Connection conn = threadLocalConnection.get();
        if (conn == null || conn.isClosed()) {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false); // Disable auto-commit by default
            threadLocalConnection.set(conn);
        }
        return conn;
    }

    public boolean execute(DatabaseQuery query) {
        try (PreparedStatement stmt = getConnection().prepareStatement(query.build())) {
            query.setParameters(stmt);
            stmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            rollback();
            return false;
        }
    }

    public ResultSet request(DatabaseQuery query) {
        try {
            PreparedStatement stmt = getConnection().prepareStatement(query.build());
            query.setParameters(stmt);
            return stmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean batchExecute(List<DatabaseQuery> queries) {
        try {
            for (DatabaseQuery query : queries) {
                try (PreparedStatement stmt = getConnection().prepareStatement(query.build())) {
                    query.setParameters(stmt);
                    stmt.addBatch();
                }
            }
            getConnection().commit();
            return true;
        } catch (SQLException e) {
            rollback();
            e.printStackTrace();
            return false;
        }
    }

    public void commit() {
        try {
            getConnection().commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void rollback() {
        try {
            getConnection().rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return getConnection().setSavepoint(name);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        getConnection().rollback(savepoint);
    }

    public void close() {
        try {
            Connection conn = threadLocalConnection.get();
            if (conn != null) {
                conn.close();
                threadLocalConnection.remove();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

// CONDITION ENUM & CLASS
enum ConditionType {
    EQ("="), NEQ("!="), GT(">"), LT("<"), LIKE("LIKE"), IN("IN"), BETWEEN("BETWEEN");
    private final String operator;
    ConditionType(String operator) { this.operator = operator; }
    public String getOperator() { return operator; }
}

// CONDITION Representation
class Condition {
    private Column column;
    private ConditionType condition;
    private List<Object> values;
    private DatabaseQuery subquery; // For subqueries

    public Condition(Column column, ConditionType condition, Object value) {
        this.column = column;
        this.condition = condition;
        this.values = new ArrayList<>();
        this.values.add(value);
    }

    public Condition(Column column, ConditionType condition, List<Object> values) {
        this.column = column;
        this.condition = condition;
        this.values = values;
    }

    public Condition(Column column, ConditionType condition, DatabaseQuery subquery) {
        this.column = column;
        this.condition = condition;
        this.subquery = subquery;
    }

    public String toSQL() {
        if (subquery != null) {
            return column.getFullName() + " " + condition.getOperator() + " (" + subquery.build() + ")";
        } else if (condition == ConditionType.IN) {
            String inValues = values.stream().map(v -> "?").collect(Collectors.joining(", "));
            return column.getFullName() + " " + condition.getOperator() + " (" + inValues + ")";
        } else if (condition == ConditionType.BETWEEN) {
            return column.getFullName() + " " + condition.getOperator() + " ? AND ?";
        } else {
            return column.getFullName() + " " + condition.getOperator() + " ?";
        }
    }

    public List<Object> getParameterValues() {
        return values;
    }
}

// JOIN REPRESENTATION CLASS
class Join {
    private Column from;
    private Column on;
    private JoinType type;

    public Join(Column from, Column on, JoinType type) {
        this.from = from;
        this.on = on;
        this.type = type;
    }

    public String toSQL() {
        return type.getSql() + " JOIN " + on.getTableName() + " ON " + from.getFullName() + " = " + on.getFullName();
    }
}

enum JoinType {
    INNER("INNER"), LEFT("LEFT"), RIGHT("RIGHT"), FULL("FULL");

    private final String sql;

    JoinType(String sql) { this.sql = sql; }

    public String getSql() { return sql; }
}

// ORDER BY ENUM & CLASS
enum OrderByType {
    ASC("ASC"), DESC("DESC");

    private final String sql;

    OrderByType(String sql) { this.sql = sql; }

    public String getSql() { return sql; }
}

class OrderBy {
    private Column column;
    private OrderByType type;

    public OrderBy(Column column, OrderByType type) {
        this.column = column;
        this.type = type;
    }

    public String toSQL() {
        return column.getFullName() + " " + type.getSql();
    }
}

// QUERY BASE CLASS WITH PARAMETER SUPPORT
abstract class DatabaseQuery {
    protected List<Object> parameters = new ArrayList<>();
    abstract String build();

    public void setParameters(PreparedStatement stmt) throws SQLException {
        for (int i = 0; i < parameters.size(); i++) {
            stmt.setObject(i + 1, parameters.get(i));
        }
    }
}

// INSERT Query Representation
class Insert extends DatabaseQuery {
    private Table table;
    private Map<Column, Object> columnValues = new HashMap<>();

    public Insert(Table table) {
        this.table = table;
    }

    public Insert column(Column column, Object value) {
        columnValues.put(column, value);
        return this;
    }

    @Override
    public String build() {
        String columns = columnValues.keySet().stream().map(Column::getFullName).collect(Collectors.joining(", "));
        String values = columnValues.values().stream().map(v -> "?").collect(Collectors.joining(", "));
        parameters.addAll(columnValues.values());
        return "INSERT INTO " + table.getName() + " (" + columns + ") VALUES (" + values + ")";
    }
}

// SELECT QUERY WITH PARAMETER HANDLING
class Select extends DatabaseQuery {
    private Table table;
    private String tableAlias;
    private boolean distinct = false;
    private List<Column> columns = new ArrayList<>();
    private List<Condition> conditions = new ArrayList<>();
    private List<Join> joins = new ArrayList<>();
    private List<OrderBy> orderByColumns = new ArrayList<>();
    private List<String> groupByColumns = new ArrayList<>();
    private List<Condition> havingConditions = new ArrayList<>();
    private int limit = -1; // Default: no limit
    private int offset = -1; // Default: no offset

    public Select(Table table) {
        this.table = table;
    }

    public Select alias(String alias) {
        this.tableAlias = alias;
        return this;
    }

    public Select distinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    public Select columns(Column... cols) {
        columns.addAll(Arrays.asList(cols));
        return this;
    }

    public Select where(Condition... conditions) {
        this.conditions.addAll(Arrays.asList(conditions));
        return this;
    }

    public Select join(Join join) {
        joins.add(join);
        return this;
    }

    public Select limit(int limit) {
        this.limit = limit;
        return this;
    }

    public Select offset(int offset) {
        this.offset = offset;
        return this;
    }

    public Select orderBy(OrderBy...  orderBys) {
        for (OrderBy orderBy : orderBys) {
            orderByColumns.add(orderBy);
        }
        return this;
    }

    public Select groupBy(Column... columns) {
        for (Column column : columns) {
            groupByColumns.add(column.getFullName());
        }
        return this;
    }

    public Select having(Condition... conditions) {
        this.havingConditions.addAll(Arrays.asList(conditions));
        return this;
    }

    public Select aggregate(String function, Column column, String alias) {
        columns.add(new Column(null, function + "(" + column.getFullName() + ")", alias));
        return this;
    }

    @Override
    public String build() {
        String distinctPart = distinct ? "DISTINCT " : "";
        String colPart = columns.isEmpty() ? "*" : columns.stream().map(Column::getFullName).collect(Collectors.joining(", "));
        String tablePart = table.getName() + (tableAlias != null ? " AS " + tableAlias : "");
        String joinPart = joins.isEmpty() ? "" : joins.stream().map(Join::toSQL).collect(Collectors.joining(" "));
        String wherePart = conditions.isEmpty() ? "" : " WHERE " + conditions.stream().map(Condition::toSQL).collect(Collectors.joining(" AND "));
        String groupByPart = groupByColumns.isEmpty() ? "" : " GROUP BY " + String.join(", ", groupByColumns);
        String havingPart = havingConditions.isEmpty() ? "" : " HAVING " + havingConditions.stream().map(Condition::toSQL).collect(Collectors.joining(" AND "));
        String orderByPart = orderByColumns.isEmpty() ? "" : " ORDER BY " + orderByColumns.stream().map(OrderBy::toSQL).collect(Collectors.joining(", "));
        String limitPart = limit > 0 ? " LIMIT " + limit : "";
        String offsetPart = offset > 0 ? " OFFSET " + offset : "";

        conditions.forEach(c -> parameters.addAll(c.getParameterValues()));
        havingConditions.forEach(c -> parameters.addAll(c.getParameterValues()));

        return "SELECT " + distinctPart + colPart + " FROM " + tablePart + " " + joinPart + wherePart + groupByPart + havingPart + orderByPart + limitPart + offsetPart;
    }
}

// UPDATE Query Representation
class Update extends DatabaseQuery {
    private Table table;
    private Map<Column, Object> columnValues = new HashMap<>();
    private List<Condition> conditions = new ArrayList<>();

    public Update(Table table) {
        this.table = table;
    }

    public Update set(Column column, Object value) {
        columnValues.put(column, value);
        return this;
    }

    public Update where(Condition... conditions) {
        this.conditions.addAll(Arrays.asList(conditions));
        return this;
    }

    @Override
    public String build() {
        String setPart = columnValues.entrySet().stream()
                .map(entry -> entry.getKey().getFullName() + " = ?")
                .collect(Collectors.joining(", "));

        String wherePart = conditions.isEmpty() ? "" : " WHERE " + conditions.stream().map(Condition::toSQL).collect(Collectors.joining(" AND "));

        parameters.addAll(columnValues.values());
        conditions.forEach(c -> parameters.addAll(c.getParameterValues()));

        return "UPDATE " + table.getName() + " SET " + setPart + wherePart;
    }
}

// DELETE Query Representation
class Delete extends DatabaseQuery {
    private Table table;
    private List<Condition> conditions = new ArrayList<>();

    public Delete(Table table) {
        this.table = table;
    }

    public Delete where(Condition... conditions) {
        this.conditions.addAll(Arrays.asList(conditions));
        return this;
    }

    @Override
    public String build() {
        String wherePart = conditions.isEmpty() ? "" : " WHERE " + conditions.stream().map(Condition::toSQL).collect(Collectors.joining(" AND "));
        conditions.forEach(c -> parameters.addAll(c.getParameterValues()));
        return "DELETE FROM " + table.getName() + wherePart;
    }
}

// TABLE REPRESENTATION
class Table {
    private String database;
    private String name;
    public Table(String name) { this.name = name; }
    public Table(String database, String name) { this.database = database; this.name = name; }
    public String getName() { return (database != null ? database + "." : "") + name; }
}

// COLUMN REPRESENTATION
class Column {
    private String name;
    private String parent;
    private String alias;
    public Column(String parent, String name, String alias) { this.parent = parent; this.name = name; this.alias = alias; }
    public String getFullName() { return (parent != null ? parent + "." : "") + name; }
    public String getTableName() { return parent; }
}