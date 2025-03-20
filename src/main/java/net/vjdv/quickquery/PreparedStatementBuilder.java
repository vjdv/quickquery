package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.QueryException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

public class PreparedStatementBuilder {
    private final PreparedStatement stmt;
    private int index = 1;

    public PreparedStatementBuilder(PreparedStatement stmt) {
        this.stmt = stmt;
    }

    public PreparedStatementBuilder setString(String value) {
        try {
            stmt.setString(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new QueryException("Error setting string parameter", ex);
        }
    }

    public PreparedStatementBuilder setBytes(byte[] value) {
        try {
            stmt.setBytes(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new QueryException("Error setting bytes parameter", ex);
        }
    }

    public PreparedStatementBuilder setInt(int value) {
        try {
            stmt.setInt(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new QueryException("Error setting int parameter", ex);
        }
    }

    public PreparedStatementBuilder setLong(long value) {
        try {
            stmt.setLong(index++, value);
            return this;
        } catch (SQLException ex) {
            throw new QueryException("Error setting long parameter", ex);
        }
    }

    public <T> PreparedStatementExecutor<T> resultMapper(Function<ResultSetWrapper, T> function) {
        return new PreparedStatementExecutor<>(stmt, function);
    }

    /**
     * Execute query without result
     */
    public void execute() {
        try (stmt) {
            stmt.execute();
        } catch (SQLException ex) {
            throw new QueryException("Error executing query", ex);
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
            throw new QueryException("Error executing insert", ex);
        }
    }

}
