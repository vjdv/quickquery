package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    private static void close(ResultSet rs, PreparedStatement stmt) {
        SQLException error = null;
        try {
            rs.close();
        } catch (SQLException ex) {
            error = ex;
        }
        try {
            stmt.close();
        } catch (SQLException ex) {
            if (error == null) {
                error = ex;
            } else {
                error.addSuppressed(ex);
            }
        }
        if (error != null) {
            throw new DataAccessException("Error closing stream", error);
        }
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

    /**
     * Returns a lazy stream of items from the result set.
     * Close the stream to release the statement and result set.
     *
     * @return stream of items
     */
    public Stream<T> stream() {
        ResultSetWrapper rs;
        try {
            var rsx = stmt.executeQuery();
            rs = new ResultSetWrapper(rsx);
        } catch (SQLException ex) {
            try {
                stmt.close();
            } catch (SQLException closeEx) {
                ex.addSuppressed(closeEx);
            }
            throw new DataAccessException("Error quering list", ex);
        }
        Spliterator<T> spliterator = new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED) {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                if (!rs.next()) {
                    return false;
                }
                action.accept(function.apply(rs));
                return true;
            }
        };
        return StreamSupport.stream(spliterator, false).onClose(() -> close(rs.getResultSet(), stmt));
    }

}
