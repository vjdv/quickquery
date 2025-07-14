package net.vjdv.quickquery;

/**
 * Represents a condition for filtering data in a query.
 *
 * @param column   the column name to filter by
 * @param operator the operator to use for comparison (e.g., '=', '>', '<', etc.)
 * @param value    the value to compare against
 */
public record Condition(String column, String operator, Object value) {
    /**
     * Creates a new Condition instance. Operator defaults to '='.
     *
     * @param column the column name to filter by
     * @param value  the value to compare against
     */
    public Condition(String column, Object value) {
        this(column, "=", value);
    }
}
