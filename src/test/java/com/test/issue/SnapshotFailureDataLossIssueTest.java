package com.test.issue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.embedded.async.AsyncEmbeddedEngine;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.Signal;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.source.EngineSourceTask;
import io.debezium.pipeline.signal.actions.snapshotting.ExecuteSnapshot;
import io.debezium.pipeline.signal.channels.process.InProcessSignalChannel;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static io.debezium.pipeline.signal.actions.AbstractSnapshotSignal.SnapshotType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

@TestInstance(Lifecycle.PER_CLASS)
public class SnapshotFailureDataLossIssueTest
{
	static
	{
		((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.INFO);

//		((Logger) LoggerFactory.getLogger("io.debezium")).setLevel(Level.OFF); // uncomment to turn off debezium logs

		((Logger) LoggerFactory.getLogger("org.apache.kafka")).setLevel(Level.OFF);
	}

	private static final boolean APPLY_FIX = false; // possible workaround. not actual fix. 

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static final int TABLE_1_INITIAL_ROWS_COUNT = 10;
	private static final int TABLE_1_BULK_INSERT_ROWS_COUNT = 200;
	private static final int TABLE_2_ROWS_COUNT = 200;

	private final PostgreSQLContainer<?> pgContainer;

	private final Map<String, Set<List<Object>>> tableRows = new ConcurrentHashMap<>();

	public SnapshotFailureDataLossIssueTest()
	{
		var container = new PostgreSQLContainer<>("postgres:14-alpine");

		container.setCommand("postgres", "-c", "fsync=off", "-c", "wal_level=logical");

		this.pgContainer = container;
	}

	@BeforeAll
	void setup() throws SQLException
	{
		this.pgContainer.start();

		// create tables table1 and table2 and insert 
		// TABLE_1_INITIAL_ROWS_COUNT rows in table1
		// and TABLE_2_ROWS_COUNT rows in table2, of
		// which the last two are "bad" rows.
		// (to simulate connector failure)

		this.setupTestDatabase();
	}

	@Test
	void testFailure() throws IOException, SQLException, IllegalAccessException
	{
		var closeables = new LinkedHashSet<Closeable>();

		try
		{
			var testId = UUID.randomUUID();

			new File(constructOffsetFileName(testId)).deleteOnExit(); // remove this line to preserve offsets file if required

			// start engine and process table1 (initial snapshot)
			var engine1 = this.startEngine(closeables, testId, "public.table1");

			await().untilAsserted(() ->
			{
				var table1Rows = this.tableRows.getOrDefault("table1", Set.of());

				assertThat(table1Rows).hasSize(TABLE_1_INITIAL_ROWS_COUNT);
			});

			engine1.close();

			System.out.println("INSERTING INITIAL DATA INTO TABLE 1 WHILE ENGINE IS STOPPED");

			this.bulkInsertDataIntoTable1(); // bulk insert data into table1 (upto TABLE_1_BULK_INSERT_ROWS_COUNT)

			// start engine again but this time with 2 tables (table1 and table2)
			var engine2 = this.startEngine(closeables, testId, "public.table1", "public.table2");

			// wait for engine to start fully
			await()
					.atMost(Duration.ofMinutes(5))
					.pollInterval(Duration.ofSeconds(1))
					.untilAsserted(() -> assertThat(getEngineState(engine2)).isEqualTo("POLLING_TASKS"));

			// send snapshot signal for table2
			this.sendBlockingSnapshotSignal(engine2, "public.table2");

			// wait for engine to fail and initiate shutdown process (due to the presence of "bad" rows in table2)
			await()
					.atMost(Duration.ofMinutes(1))
					.untilAsserted(() -> assertThat(getEngineState(engine2)).satisfiesAnyOf(
							state -> Assertions.assertThat(state).isEqualTo("STOPPING"), // see comment below
							state -> Assertions.assertThat(state).isEqualTo("STOPPED")
					));

			var shutdownWatch = StopWatch.createStarted();

			if (!getEngineState(engine2).equals("STOPPED"))
			{
				System.out.println();

				// if we get to this point, it means we've hit another (possibly unrelated) issue
				// where ChangeEventQueue is waiting for the 'isNotFull' signal at line "this.isNotFull.await"
				// but since the shutdown process would have closed the polling thread by this point, the 
				// 'isNotFull' signal never gets sent, causing the shutdown process to stall and eventually get forcefully
				// terminated after io.debezium.config.CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_SEC (hardcoded to 90 secs)

				System.out.println("WAITING FOR shutdown process to forcefully terminate itself and exit from stuck thread in ChangeEventQueue");

				System.out.println();
			}

			// wait for engine to fail and stop completely
			await()
					.atMost(Duration.ofMinutes(5))
					.pollInterval(Duration.ofSeconds(1))
					.untilAsserted(() -> assertThat(getEngineState(engine2)).satisfiesAnyOf(
							state -> Assertions.assertThat(state).isEqualTo("STOPPED")
					));

			System.out.println("engine2 completed shutdown in " + shutdownWatch.formatTime());

			assertThatIllegalStateException().isThrownBy(engine2::close)
					.withMessageContaining("Engine has been already shut down");

			System.out.println("engine2 failed and stopped. starting engine3");

			var engine3 = this.startEngine(closeables, testId, "public.table1", "public.table2");

			System.out.println("engine3 started");

			System.out.println("INSERTING FINAL DATA INTO TABLE 1");

			int finalId = this.insertFinalRowIntoTable1(); // insert one last row

			assertThat(finalId).isEqualTo(TABLE_1_BULK_INSERT_ROWS_COUNT + 1);

			// wait till row with "finalId" has been processed 
			await()
					.atMost(Duration.ofMinutes(1))
					.untilAsserted(() ->
					{
						var table1Rows = this.tableRows.getOrDefault("table1", Set.of());

						var ids = table1Rows.stream()
								.map(List::getFirst)
								.map(Integer.class::cast)
								.toList();

						assertThat(ids).contains(finalId);
					});

			// check once more if 'finalId' has been processed
			assertThat(this.tableRows.get("table1"))
					.map(List::getFirst)
					.asInstanceOf(InstanceOfAssertFactories.list(Integer.class))
					.contains(finalId);

			/*---- failing assertions ---- */

			// at this point, 'table1' should have all rows
			// starting from id=1 to id=finalId but a chunk of
			// rows seem to always be missing (presumably lost forever)
			// at this point.
			assertThat(this.tableRows.get("table1")).hasSize(finalId);

			assertThat(this.tableRows.get("table1"))
					.map(List::getFirst)
					.asInstanceOf(InstanceOfAssertFactories.list(Integer.class))
					.containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(1, TABLE_1_BULK_INSERT_ROWS_COUNT + 1).boxed().toList());

			engine3.close();
		}
		finally
		{
			for (var closeable : closeables)
			{
				try
				{
					closeable.close();
				}
				catch (IllegalStateException e)
				{
					// ignore. probably closed already
				}
				catch (Exception e)
				{
					// log and ignore
					System.out.println("Caught exception while closing " + e.getMessage());
				}
			}
		}
	}

	@AfterAll
	void tearDown()
	{
		this.pgContainer.stop();
	}

	private void sendBlockingSnapshotSignal(DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine, String... tables) throws JsonProcessingException
	{
		System.out.println("SENDING BLOCKING SNAPSHOT SIGNAL");

		var data = MAPPER.writeValueAsString(new SignalRecordData(Set.of(tables), SnapshotType.BLOCKING.name()));

		engine.getSignaler().signal(new Signal(UUID.randomUUID().toString(), ExecuteSnapshot.NAME, data, Map.of()));

		System.out.println("SENT BLOCKING SNAPSHOT SIGNAL");
	}

	private DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> startEngine(Set<Closeable> closeables, UUID testId, String... tables)
	{
		System.out.println("STARTING ENGINE");

		var engineRef = new AtomicReference<DebeziumEngine<?>>();

		var engine = asyncEngineBuilder()
				.using(this.getConfiguration(testId, tables))
				.notifying((records, committer) -> this.processBatch(engineRef, records, committer))
				.build();

		engineRef.set(engine);

		Thread.ofPlatform().start(engine); // use platform thread so that it shows up in thread-dumps

		closeables.add(engine);

		System.out.println("STARTED ENGINE");

		return engine;
	}

	@SneakyThrows
	private void processBatch(
			AtomicReference<DebeziumEngine<?>> engineRef,
			List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
	{
		System.out.println();

		var isSnapshotting = false;

		for (var record : records)
		{
			try
			{
				isSnapshotting |= this.processRecord(record);
			}
			catch (Throwable t)
			{
				if (APPLY_FIX)
				{
					clearPendingOffsets(engineRef.get());
				}
				throw t;
			}

			committer.markProcessed(record);
		}

		if (!APPLY_FIX || !isSnapshotting)
		{
			committer.markBatchFinished();
		}

		System.out.println();
	}

	private boolean processRecord(ChangeEvent<SourceRecord, SourceRecord> record) throws Exception
	{
		var value = (Struct) record.value().value();

		if (value.schema().fields().stream().map(Field::name).toList().contains(Envelope.FieldName.AFTER))
		{
			var source = value.getStruct(Envelope.FieldName.SOURCE);
			var tableRow = value.getStruct(Envelope.FieldName.AFTER);

			var lsn = source.getInt64(SourceInfo.LSN_KEY);
			var table = source.getString(SourceInfo.TABLE_NAME_KEY);

			var snapshotting = SnapshotRecord.fromSource(source) instanceof SnapshotRecord sr && sr.ordinal() <= SnapshotRecord.LAST.ordinal();

			System.out.println("processed --> " + lsn + " - " + table + " - " + (snapshotting ? "snapshotting" : "streaming") + " - " + tableRow);

			if (tableRow.getString("name").startsWith("badUser"))
			{
				throw new Exception("badUser");
			}

			var rowValues = tableRow.schema().fields().stream().map(tableRow::get).toList();

			Thread.sleep(100);

			this.tableRows.computeIfAbsent(table, s -> ConcurrentHashMap.newKeySet()).add(rowValues);

			return snapshotting;
		}

		return false;
	}

	private void setupTestDatabase() throws SQLException
	{
		try (var connection = this.getConnection(); var statement = connection.createStatement())
		{
			statement.execute("CREATE TABLE table1 (id integer primary key, name varchar NULL)");
			statement.execute("CREATE TABLE table2 (id integer primary key, name varchar NULL)");
		}

		try (var connection = this.getConnection())
		{
			@Cleanup var statement = connection.prepareStatement("INSERT INTO table1 VALUES (?, ?)");

			for (int i = 1; i <= TABLE_1_INITIAL_ROWS_COUNT; i++)
			{
				statement.setInt(1, i);
				statement.setString(2, "user" + i);

				statement.addBatch();
			}

			statement.executeBatch();
		}

		try (var connection = this.getConnection())
		{
			@Cleanup var statement = connection.prepareStatement("INSERT INTO table2 VALUES (?, ?)");

			for (int i = 1; i <= TABLE_2_ROWS_COUNT; i++)
			{
				statement.setInt(1, i);

				if (i == TABLE_2_ROWS_COUNT - 2)
				{
					statement.setString(2, "badUser" + i); // simulate bad record mid-snapshot
				}
				else
				{
					statement.setString(2, "user" + i);
				}

				statement.addBatch();
			}

			statement.executeBatch();
		}
	}

	private void bulkInsertDataIntoTable1() throws SQLException
	{
		try (var connection = this.getConnection())
		{
			@Cleanup var statement = connection.prepareStatement("INSERT INTO table1 VALUES (?, ?)");

			for (int i = TABLE_1_INITIAL_ROWS_COUNT + 1; i <= TABLE_1_BULK_INSERT_ROWS_COUNT; i++)
			{
				statement.setInt(1, i);
				statement.setString(2, "user" + i);

				statement.addBatch();
			}

			statement.executeBatch();
		}
	}

	private int insertFinalRowIntoTable1() throws SQLException
	{
		try (var connection = this.getConnection())
		{
			@Cleanup var statement = connection.prepareStatement("INSERT INTO table1 VALUES (?, ?)");

			var i = TABLE_1_BULK_INSERT_ROWS_COUNT + 1;

			statement.setInt(1, i);
			statement.setString(2, "user" + i);

			statement.addBatch();

			statement.executeBatch();

			return i;
		}
	}

	private Properties getConfiguration(UUID testId, String... tables)
	{
		var builder = Configuration.create();

		builder.with(EmbeddedEngineConfig.ENGINE_NAME, "issue-test-connector");
		builder.with(EmbeddedEngineConfig.CONNECTOR_CLASS, PostgresConnector.class.getName());

		builder.with(EmbeddedEngineConfig.OFFSET_STORAGE, FileOffsetBackingStore.class.getName());
		builder.with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, constructOffsetFileName(testId));

		builder.with(RelationalDatabaseConnectorConfig.HOSTNAME, this.pgContainer.getHost());
		builder.with(RelationalDatabaseConnectorConfig.PORT, String.valueOf(this.pgContainer.getMappedPort(POSTGRESQL_PORT)));
		builder.with(RelationalDatabaseConnectorConfig.USER, this.pgContainer.getUsername());
		builder.with(RelationalDatabaseConnectorConfig.PASSWORD, this.pgContainer.getPassword());
		builder.with(RelationalDatabaseConnectorConfig.DATABASE_NAME, this.pgContainer.getDatabaseName());
		builder.with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, String.join(",", tables));

		builder.with(PostgresConnectorConfig.SLOT_NAME, "slot1");
		builder.with(PostgresConnectorConfig.SKIPPED_OPERATIONS, "none");
		builder.with(PostgresConnectorConfig.PUBLICATION_NAME, "publication1");
		builder.with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL);
		builder.with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT);
		builder.with(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE, PostgresConnectorConfig.AutoCreateMode.FILTERED);

		builder.with(CommonConnectorConfig.TOPIC_PREFIX, "topic1");
		builder.with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, InProcessSignalChannel.CHANNEL_NAME);

		// test specific overrides
		builder.with(CommonConnectorConfig.MAX_BATCH_SIZE, 2);
		builder.with(CommonConnectorConfig.MAX_QUEUE_SIZE, 3);
		builder.with(EmbeddedEngineConfig.OFFSET_FLUSH_INTERVAL_MS, "1000");

		return builder.build().asProperties();
	}

	private Connection getConnection() throws SQLException
	{
		return DriverManager.getConnection(this.pgContainer.getJdbcUrl(), this.pgContainer.getUsername(), this.pgContainer.getPassword());
	}

	private static DebeziumEngine.Builder<ChangeEvent<SourceRecord, SourceRecord>> asyncEngineBuilder()
	{
		var factory = ConvertingAsyncEngineBuilderFactory.class.getName();

		return DebeziumEngine.create(KeyValueHeaderChangeEventFormat.of(Connect.class, Connect.class, Connect.class), factory);
	}

	/**
	 * is there a better way to get this ?
	 * expose getState() method on DebeziumEngine ??
	 */
	private static String getEngineState(DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine) throws IllegalAccessException
	{
		if (engine instanceof AsyncEmbeddedEngine<ChangeEvent<SourceRecord, SourceRecord>> asyncEngine)
		{
			if (FieldUtils.readField(asyncEngine, "state", true) instanceof AtomicReference<?> stateRef)
			{
				return String.valueOf(stateRef.get());
			}
		}
		throw new IllegalStateException("Unable to get engine state");
	}

	/**
	 * Only for demonstration of potential workaround. 
	 */
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static void clearPendingOffsets(DebeziumEngine<?> engine)
	{
		var tasks = (List<EngineSourceTask>) FieldUtils.readField(engine, "tasks", true);

		for (EngineSourceTask task : tasks)
		{
			var writer = task.context().offsetStorageWriter();

			if (((Map<?, ?>) FieldUtils.readField(writer, "data", true)) instanceof Map<?, ?> data)
			{
				data.clear();
			}

			if (((Map<?, ?>) FieldUtils.readField(writer, "toFlush", true)) instanceof Map<?, ?> toFlush)
			{
				toFlush.clear();
			}
		}
	}

	private static String constructOffsetFileName(UUID testId)
	{
		return "itc-" + testId + ".offsets";
	}

	private record SignalRecordData(@JsonProperty("data-collections") Set<String> dataCollections, String type)
	{
	}
}
