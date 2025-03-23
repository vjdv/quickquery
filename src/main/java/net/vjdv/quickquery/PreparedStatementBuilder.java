package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * A builder class to create a PreparedStatement with parameters
 */
public class PreparedStatementBuilder {
    private final PreparedStatement stmt;
    private int index = 1;

    /**
     * Create a PreparedStatementBuilder instance
     *
     * @param stmt the prepared statement to be used
     */
    public PreparedStatementBuilder(PreparedStatement stmt) {
        this.stmt = stmt;
    }

    /**
     * Set a string parameter to consecutive parameter index
     *
     * @param value the string value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setString(String value) {
        try {
            stmt.setString(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting string parameter", ex);
        }
    }

    /**
     * Set a byte array parameter to consecutive parameter index
     *
     * @param value the byte array value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setBytes(byte[] value) {
        try {
            stmt.setBytes(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting bytes parameter", ex);
        }
    }

    /**
     * Set an int parameter to consecutive parameter index
     *
     * @param value the int value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setInt(int value) {
        try {
            stmt.setInt(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting int parameter", ex);
        }
    }

    /**
     * Set a long parameter to consecutive parameter index
     *
     * @param value the long value
     * @return same PreparedStatementBuilder instance
     */
    public PreparedStatementBuilder setLong(long value) {
        try {
            stmt.setLong(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new DataAccessException("Error setting long parameter", ex);
        }
    }

    /**
     * Sets a result mapper function to be used in the query execution
     *
     * @param function the function to map the result
     * @param <T>      the type of the result
     * @return a PreparedStatementExecutor instance
     */
    public <T> PreparedStatementExecutor<T> resultMapper(Function<ResultSetWrapper, T> function) {
        return new PreparedStatementExecutor<>(stmt, function);
    }

    /**
     * Execute query without result using PreparedStatement.execute
     */
    public void execute() {
        try (stmt) {
            stmt.execute();
        } catch (SQLException ex) {
            throw new DataAccessException("Error executing query", ex);
        }
    }

    /**
     * Execute an update statement using PreparedStatement.executeUpdate
     *
     * @return the number of rows affected
     */
    public int executeUpdate() {
        try (stmt) {
            return stmt.executeUpdate();
        } catch (SQLException ex) {
            throw new DataAccessException("Error executing update", ex);
        }
    }

    /**
     * Execute an insert statement and return the autoincremented id
     *
     * @return the autoincremented id
     */
    public int insertAutoincrement() {
        try (stmt) {
            stmt.execute();
            try (var rs = stmt.getGeneratedKeys()) {
                return rs.getInt(1);
            }
        } catch (SQLException ex) {
            throw new DataAccessException("Error executing insert", ex);
        }
    }

}
