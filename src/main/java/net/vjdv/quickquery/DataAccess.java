package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Data access class
 */
public class DataAccess {
    private final Supplier<Connection> connectionSupplier;
    private final boolean closeConnection;

    /**
     * Creates a new instance of DataAccess
     *
     * @param connectionSupplier supplier of connection
     */
    protected DataAccess(Supplier<Connection> connectionSupplier, boolean closeConnection) {
        this.connectionSupplier = connectionSupplier;
        this.closeConnection = closeConnection;
    }

    /**
     * Connection shorter
     *
     * @return connection
     */
    private Connection conn() {
        return connectionSupplier.get();
    }

    /**
     * Creates a new PreparedStatementBuilder for a query
     *
     * @param sql query
     * @return PreparedStatementBuilder
     */
    public PreparedStatementBuilder query(String sql) {
        try {
            var stmt = conn().prepareStatement(sql);
            return new PreparedStatementBuilder(new PreparedStatementWrapper(stmt, closeConnection));
        } catch (SQLException ex) {
            throw new DataAccessException("Error creating prepared statement", ex);
        }
    }

    /**
     * Creates a new PreparedStatementBuilder for a query with generated keys, useful for autoincrement or serial columns
     *
     * @param sql query
     * @return PreparedStatementBuilder
     */
    public PreparedStatementBuilder queryWithGeneratedKey(String sql) {
        try {
            var stmt = conn().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            return new PreparedStatementBuilder(new PreparedStatementWrapper(stmt, closeConnection));
        } catch (SQLException ex) {
            throw new DataAccessException("Error creating prepared statement", ex);
        }
    }

    /**
     * Starts a query builder with all columns for a SELECT statement
     *
     * @param table table name
     * @return QueryBuilder started with SELECT * FROM table
     */
    public QueryBuilder select(String table) {
        return new QueryBuilder("SELECT * FROM " + table);
    }

    /**
     * Starts a query builder with all columns for record class
     *
     * @param recordClass record class
     * @return QueryBuilder started with SELECT * FROM "recordClass name in lowercase"
     */
    public <T extends Record> PreparedStatementExecutor<T> select(Class<T> recordClass) {
        return select(recordClass, builder -> {
        });
    }

    /**
     * Starts a query builder with all columns for record class and applies a consumer to modify the query
     *
     * @param recordClass     record class
     * @param builderConsumer consumer to modify the query
     * @return PreparedStatementExecutor for the record class
     */
    public <T extends Record> PreparedStatementExecutor<T> select(Class<T> recordClass, Consumer<QueryBuilder> builderConsumer) {
        String table = recordClass.getSimpleName().toLowerCase();
        var builder = select(table);
        builderConsumer.accept(builder);
        return query(builder.sql.toString()).setParameters(builder.indexParameters).resultMapper(recordClass);
    }

    /**
     * Starts a query builder with specific columns for a SELECT statement
     *
     * @param table   table name
     * @param columns columns to select
     * @return QueryBuilder started with SELECT columns FROM table
     */
    public QueryBuilder select(String table, String... columns) {
        String cols = String.join(", ", columns);
        return new QueryBuilder("SELECT " + cols + " FROM " + table);
    }

    /**
     * Query builder helps to build SQL queries in a fluent way
     */
    public class QueryBuilder {
        private final StringBuilder sql = new StringBuilder();
        private final Map<Integer, Object> indexParameters = new HashMap<>();
        private int index = 1;

        /**
         * Creates a new QueryBuilder starting with the given SQL statement
         *
         * @param sql initial SQL statement
         */
        public QueryBuilder(String sql) {
            this.sql.append(sql);
        }

        /**
         * Adds a WHERE clause to the query
         *
         * @param column column name
         * @param value  value to filter by
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder where(String column, Object value) {
            sql.append(" WHERE ").append(column).append(" = ?");
            indexParameters.put(index++, value);
            return QueryBuilder.this;
        }

        /**
         * Adds a condition to the query
         *
         * @param condition Condition to add
         * @return QueryBuilder instance for chaining
         */
        private QueryBuilder add(Condition condition) {
            sql.append(condition.column()).append(" ").append(condition.operator()).append(" ?");
            indexParameters.put(index++, condition.value());
            return QueryBuilder.this;
        }

        /**
         * Adds an AND clause to the query
         *
         * @param condition Condition to add
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder and(Condition condition) {
            return append(" AND ").add(condition);
        }

        /**
         * Adds an AND clause to the query
         *
         * @param column column name
         * @param value  value to filter by
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder and(String column, Object value) {
            return and(new Condition(column, value));
        }

        /**
         * Adds an OR clause to the query
         *
         * @param condition Condition to add
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder or(Condition condition) {
            return append(" OR ").add(condition);
        }

        /**
         * Adds an OR clause to the query
         *
         * @param column column name
         * @param value  value to filter by
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder or(String column, Object value) {
            return or(new Condition(column, value));
        }

        /**
         * Adds a condition group to the query
         *
         * @param condition ConditionGroup to add
         * @return QueryBuilder instance for chaining
         */
        private QueryBuilder add(ConditionGroup condition) {
            sql.append(condition.getSql());
            for (Map.Entry<Integer, Object> entry : condition.getIndexParameters().entrySet()) {
                var i = index + entry.getKey();
                indexParameters.put(i, entry.getValue());
            }
            index += condition.getIndexParameters().size();
            return QueryBuilder.this;
        }

        /**
         * Adds a condition group with AND to the query
         *
         * @param condition ConditionGroup to add
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder and(ConditionGroup condition) {
            return append(" AND ( ").add(condition).append(" )");
        }

        /**
         * Adds a condition group with OR to the query
         *
         * @param condition ConditionGroup to add
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder or(ConditionGroup condition) {
            return append(" OR ( ").add(condition).append(" )");
        }

        /**
         * Order the results by specified columns
         *
         * @param asc     true for ascending order, false for descending
         * @param columns columns to order by
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder orderBy(boolean asc, String... columns) {
            sql.append(" ORDER BY ").append(String.join(", ", columns)).append(asc ? " ASC" : " DESC");
            return QueryBuilder.this;
        }

        /**
         * Order the results by specified columns in ascending order
         *
         * @param columns columns to order by
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder orderBy(String... columns) {
            return orderBy(true, columns);
        }

        /**
         * Order the results by specified columns in descending order
         *
         * @param columns columns to order by
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder orderByDesc(String... columns) {
            return orderBy(false, columns);
        }

        /**
         * Appends additional SQL to the query
         *
         * @param sql any additional SQL to append
         * @return QueryBuilder instance for chaining
         */
        public QueryBuilder append(String sql) {
            this.sql.append(" ").append(sql);
            return this;
        }

        /**
         * returns the index parameters added to the query
         *
         * @return Map of index parameters
         */
        public Map<Integer, Object> getIndexParameters() {
            return indexParameters;
        }

        /**
         * Returns the resulting SQL query as a string
         *
         * @return SQL query string
         */
        public String getSql() {
            return sql.toString();
        }

        /**
         * Prepares the SQL statement with the parameters set in this QueryBuilder
         *
         * @return PreparedStatementBuilder ready to execute the query
         */
        public PreparedStatementBuilder prepare() {
            sql.append(";");
            return query(sql.toString()).setParameters(indexParameters);
        }

    }

}
