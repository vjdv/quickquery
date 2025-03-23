package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Executes a prepared statement and processes the result set
 *
 * @param <T> type of the result
 */
public class PreparedStatementExecutor<T> {
    private final PreparedStatement stmt;
    private final Function<ResultSetWrapper, T> function;

    /**
     * Creates a new instance of PreparedStatementExecutor
     *
     * @param stmt     prepared statement
     * @param function function to process the result set
     */
    public PreparedStatementExecutor(PreparedStatement stmt, Function<ResultSetWrapper, T> function) {
        this.stmt = stmt;
        this.function = function;
    }

    /**
     * Returns the first item from the result set
     *
     * @return the first item
     */
    public Optional<T> findOne() {
        AtomicReference<T> result = new AtomicReference<>(null);
        try (stmt; var rs = stmt.executeQuery()) {
            if (rs.next()) {
                T item = function.apply(new ResultSetWrapper(rs));
                result.set(item);
            }
        } catch (SQLException ex) {
            throw new DataAccessException("Error quering item", ex);
        }
        return Optional.ofNullable(result.get());
    }

    /**
     * Returns a list of items from the result set
     *
     * @return list of items
     */
    public List<T> list() {
        List<T> list = new ArrayList<>();
        try (stmt; var rs = stmt.executeQuery()) {
            while (rs.next()) {
                T item = function.apply(new ResultSetWrapper(rs));
                list.add(item);
            }
        } catch (SQLException ex) {
            throw new DataAccessException("Error quering list", ex);
        }
        return list;
    }

    /**
     * Executes an action for each row in the result set
     *
     * @param consumer action to execute
     */
    public void forEach(Consumer<T> consumer) {
        try (stmt; var rs = stmt.executeQuery()) {
            while (rs.next()) {
                T item = function.apply(new ResultSetWrapper(rs));
                consumer.accept(item);
            }
        } catch (SQLException ex) {
            throw new DataAccessException("Error quering list", ex);
        }
    }

}
