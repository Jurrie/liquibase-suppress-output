package liquibase.change.ext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.database.core.SybaseASADatabase;
import liquibase.database.core.SybaseDatabase;
import liquibase.exception.DatabaseException;
import liquibase.executor.AbstractExecutor;
import liquibase.executor.Executor;
import liquibase.executor.LoggingExecutor;
import liquibase.executor.jvm.JdbcExecutor;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

class SuppressOutputExecutor extends AbstractExecutor implements Executor
{
	private final Executor previousExecutor;

	public SuppressOutputExecutor(final Executor previousExecutor, final Database database)
	{
		this.previousExecutor = previousExecutor;
		setDatabase(database);
	}

	public Executor getPreviousExecutor()
	{
		return previousExecutor;
	}

	public boolean suppressesExecutionToDatabase()
	{
		return previousExecutor instanceof JdbcExecutor;
	}

	public boolean suppressesOutputToSQL()
	{
		return previousExecutor instanceof LoggingExecutor;
	}

	private void suppressSqlStatement(final SqlStatement sql) throws DatabaseException
	{
		suppressSqlStatement(sql, Collections.<SqlVisitor> emptyList());
	}

	private void suppressSqlStatement(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException
	{
		if (SqlGeneratorFactory.getInstance().generateStatementsVolatile(sql, database))
		{
			throw new DatabaseException(sql.getClass().getSimpleName() + " requires access to up to date database metadata which is not available when you suppress SQL execution");
		}

		for (String statement : applyVisitors(sql, sqlVisitors))
		{
			if (statement == null)
			{
				continue;
			}

			if (database instanceof MSSQLDatabase || database instanceof SybaseDatabase || database instanceof SybaseASADatabase)
			{
				comment("Suppressed: " + statement);
				comment("Suppressed: GO");
			}
			else
			{
				final String endDelimiter = sql instanceof RawSqlStatement ? ((RawSqlStatement) sql).getEndDelimiter() : ";";

				if (!statement.endsWith(endDelimiter))
				{
					statement += endDelimiter;
				}
				comment("Suppressed: " + statement);
			}
		}
	}

	@Override
	public <T> T queryForObject(SqlStatement sql, Class<T> requiredType) throws DatabaseException
	{
		return previousExecutor.queryForObject(sql, requiredType);
	}

	@Override
	public <T> T queryForObject(SqlStatement sql, Class<T> requiredType, List<SqlVisitor> sqlVisitors) throws DatabaseException
	{
		return previousExecutor.queryForObject(sql, requiredType, sqlVisitors);
	}

	@Override
	public long queryForLong(SqlStatement sql) throws DatabaseException
	{
		return previousExecutor.queryForLong(sql);
	}

	@Override
	public long queryForLong(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException
	{
		return previousExecutor.queryForLong(sql, sqlVisitors);
	}

	@Override
	public int queryForInt(SqlStatement sql) throws DatabaseException
	{
		return previousExecutor.queryForInt(sql);
	}

	@Override
	public int queryForInt(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException
	{
		return previousExecutor.queryForInt(sql, sqlVisitors);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List queryForList(SqlStatement sql, Class elementType) throws DatabaseException
	{
		return previousExecutor.queryForList(sql);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List queryForList(SqlStatement sql, Class elementType, List<SqlVisitor> sqlVisitors) throws DatabaseException
	{
		return previousExecutor.queryForList(sql, elementType, sqlVisitors);
	}

	@Override
	public List<Map<String, ?>> queryForList(SqlStatement sql) throws DatabaseException
	{
		return previousExecutor.queryForList(sql);
	}

	@Override
	public List<Map<String, ?>> queryForList(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException
	{
		return previousExecutor.queryForList(sql, sqlVisitors);
	}

	@Override
	public void execute(SqlStatement sql) throws DatabaseException
	{
		suppressSqlStatement(sql);
	}

	@Override
	public void execute(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException
	{
		suppressSqlStatement(sql, sqlVisitors);
	}

	@Override
	public int update(SqlStatement sql) throws DatabaseException
	{
		if (sql instanceof LockDatabaseChangeLogStatement)
		{
			return 1;
		}
		else if (sql instanceof UnlockDatabaseChangeLogStatement)
		{
			return 1;
		}

		suppressSqlStatement(sql);

		return 0;
	}

	@Override
	public int update(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException
	{
		suppressSqlStatement(sql, sqlVisitors);

		return 0;
	}

	@Override
	public void comment(String message) throws DatabaseException
	{
		previousExecutor.comment(message);
	}

	@Override
	public boolean updatesDatabase()
	{
		return false;
	}
}