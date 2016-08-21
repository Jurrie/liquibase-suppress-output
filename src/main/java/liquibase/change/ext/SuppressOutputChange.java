package liquibase.change.ext;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.RollbackImpossibleException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.executor.LoggingExecutor;
import liquibase.executor.jvm.JdbcExecutor;
import liquibase.statement.SqlStatement;

@DatabaseChange(name = "suppressOutput", description = "Suppresses output to either SQL output or JDBC calls", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class SuppressOutputChange extends AbstractChange
{
	public enum START_OR_STOP
	{
		START, STOP
	}

	public enum SUPPRESS
	{
		EXECUTE, SQLFILE
	}

	private START_OR_STOP startOrStop;
	private SUPPRESS suppress;

	@Override
	public String getConfirmationMessage()
	{
		StringBuilder sb = new StringBuilder("SuppressOutput: ");
		sb.append(suppress == SUPPRESS.EXECUTE ? "Actual execution to database" : "Outputting statements to SQL output file");
		sb.append(" is now ");
		sb.append(startOrStop == START_OR_STOP.START ? "suppressed" : "not suppressed anymore");
		return sb.toString();
	}

	@Override
	public SqlStatement[] generateStatements(final Database database)
	{
		validateParameters();

		final Executor currentExecutor = ExecutorService.getInstance().getExecutor(database);

		if (startOrStop == START_OR_STOP.START)
		{
			startSuppressing(database, currentExecutor);
		}
		else if (startOrStop == START_OR_STOP.STOP)
		{
			stopSuppressing(database, currentExecutor);
		}
		else
		{
			throw new IllegalArgumentException("Unknown startOrStop value " + startOrStop.name());
		}

		return new SqlStatement[] {};
	}

	@Override
	public SqlStatement[] generateRollbackStatements(final Database database) throws RollbackImpossibleException
	{
		validateParameters();

		final Executor currentExecutor = ExecutorService.getInstance().getExecutor(database);

		if (startOrStop == START_OR_STOP.START)
		{
			stopSuppressing(database, currentExecutor);
		}
		else if (startOrStop == START_OR_STOP.STOP)
		{
			startSuppressing(database, currentExecutor);
		}
		else
		{
			throw new IllegalArgumentException("Unknown startOrStop value " + startOrStop.name());
		}

		return new SqlStatement[] {};
	}

	private void startSuppressing(final Database database, final Executor currentExecutor)
	{
		if (suppress == SUPPRESS.EXECUTE)
		{
			if (currentExecutor instanceof JdbcExecutor)
			{
				final SuppressOutputExecutor newExecutor = new SuppressOutputExecutor(currentExecutor, database);
				ExecutorService.getInstance().setExecutor(database, newExecutor);
			}
			else if (currentExecutor instanceof SuppressOutputExecutor)
			{
				if (((SuppressOutputExecutor) currentExecutor).suppressesExecutionToDatabase())
				{
					// Execution already suppressed (Don't error out, because it seems Liquibase can call this function multiple times.)
				}
				else
				{
					throw new IllegalStateException("Output to SQL file already suppressed, also suppressing execution will never result in a change");
				}
			}
			else if (currentExecutor instanceof LoggingExecutor)
			{
				// Do nothing here, let SQL output pass.
			}
			else
			{
				throw new IllegalArgumentException("Unknown executor " + currentExecutor.getClass().getCanonicalName());
			}
		}
		else if (suppress == SUPPRESS.SQLFILE)
		{
			if (currentExecutor instanceof LoggingExecutor)
			{
				final SuppressOutputExecutor newExecutor = new SuppressOutputExecutor(currentExecutor, database);
				ExecutorService.getInstance().setExecutor(database, newExecutor);
			}
			else if (currentExecutor instanceof SuppressOutputExecutor)
			{
				if (((SuppressOutputExecutor) currentExecutor).suppressesOutputToSQL())
				{
					// SQL output already suppressed (Don't error out, because it seems Liquibase can call this function multiple times.)
				}
				else
				{
					throw new IllegalStateException("Execution already suppressed, also suppressing output to SQL file will never result in a change");
				}
			}
			else if (currentExecutor instanceof JdbcExecutor)
			{
				// Do nothing here, let JDBC execution pass.
			}
			else
			{
				throw new IllegalArgumentException("Unknown executor " + currentExecutor.getClass().getCanonicalName());
			}
		}
		else
		{
			throw new IllegalArgumentException("Unknown suppress target " + suppress.name());
		}
	}

	private void stopSuppressing(final Database database, final Executor currentExecutor)
	{
		if (suppress == SUPPRESS.EXECUTE)
		{
			if (currentExecutor instanceof JdbcExecutor)
			{
				// Execution not suppressed (Don't error out, because it seems Liquibase can call this function multiple times.)
			}
			else if (currentExecutor instanceof SuppressOutputExecutor)
			{
				if (((SuppressOutputExecutor) currentExecutor).suppressesExecutionToDatabase())
				{
					final Executor previousExecutor = ((SuppressOutputExecutor) currentExecutor).getPreviousExecutor();
					ExecutorService.getInstance().setExecutor(database, previousExecutor);
				}
				else
				{
					throw new IllegalStateException("Output to SQL file already suppressed, also suppressing execution will never result in a change");
				}
			}
			else if (currentExecutor instanceof LoggingExecutor)
			{
				// Do nothing here, let SQL output pass.
			}
			else
			{
				throw new IllegalArgumentException("Unknown executor " + currentExecutor.getClass().getCanonicalName());
			}
		}
		else if (suppress == SUPPRESS.SQLFILE)
		{
			if (currentExecutor instanceof LoggingExecutor)
			{
				// SQL output not suppressed (Don't error out, because it seems Liquibase can call this function multiple times.)
			}
			else if (currentExecutor instanceof SuppressOutputExecutor)
			{
				if (((SuppressOutputExecutor) currentExecutor).suppressesOutputToSQL())
				{
					final Executor previousExecutor = ((SuppressOutputExecutor) currentExecutor).getPreviousExecutor();
					ExecutorService.getInstance().setExecutor(database, previousExecutor);
				}
				else
				{
					throw new IllegalStateException("Execution already suppressed, also suppressing output to SQL file will never result in a change");
				}
			}
			else if (currentExecutor instanceof JdbcExecutor)
			{
				// Do nothing here, let JDBC execution pass.
			}
			else
			{
				throw new IllegalArgumentException("Unknown executor " + currentExecutor.getClass().getCanonicalName());
			}
		}
		else
		{
			throw new IllegalArgumentException("Unknown suppress target " + suppress.name());
		}
	}

	@Override
	public boolean generateStatementsVolatile(Database database)
	{
		return false;
	}

	@Override
	public boolean generateRollbackStatementsVolatile(Database database)
	{
		return false;
	}

	@Override
	public boolean supports(Database database)
	{
		return true;
	}

	private void validateParameters()
	{
		if (startOrStop == null)
		{
			throw new IllegalStateException("Please give 'startOrStop' parameter");
		}
		if (suppress == null)
		{
			throw new IllegalStateException("Please give 'suppress' parameter");
		}
	}

	@DatabaseChangeProperty(description = "Indicates whether you want to START or STOP suppression", requiredForDatabase = "all")
	public String getStartOrStop()
	{
		return startOrStop.name();
	}

	public void setStartOrStop(final String startOrStop)
	{
		this.startOrStop = START_OR_STOP.valueOf(startOrStop.toUpperCase());
	}

	@DatabaseChangeProperty(description = "Indicates what you want to suppress. Valid values are EXECUTE (suppress exection in database) or SQLFILE (suppress output to SQL file).", requiredForDatabase = "all")
	public String getSuppress()
	{
		return suppress.name();
	}

	public void setSuppress(final String suppress)
	{
		this.suppress = SUPPRESS.valueOf(suppress.toUpperCase());
	}
}