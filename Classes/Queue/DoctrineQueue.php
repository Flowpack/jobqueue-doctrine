<?php
declare(strict_types=1);

namespace Flowpack\JobQueue\Doctrine\Queue;

/*
 * This file is part of the Flowpack.JobQueue.Doctrine package.
 *
 * (c) Contributors to the package
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\InvalidArgumentException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Query\Expression\CompositeExpression;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\ORM\EntityManagerInterface;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Psr\Log\LoggerInterface;

/**
 * A queue implementation using doctrine as the queue backend
 *
 * Every reservation records when and by which process (hostname and PID) it was made. A worker
 * that is killed while it holds a reservation would otherwise leave the message "reserved"
 * forever. While waiting for messages, the queue therefore releases such orphaned reservations:
 * immediately if the reserving process ran on the same host and is gone, otherwise once the
 * reservation is older than the "reservationTimeout" option. A released message counts as a
 * failed attempt, just like a message released after an exception.
 */
class DoctrineQueue implements QueueInterface
{
    /**
     * Minimum interval between two searches for orphaned reservations, in seconds
     */
    private const STALE_RESERVATIONS_CHECK_INTERVAL = 60;

    protected string $name;

    protected array $options;

    protected Connection $connection;

    /**
     * Default timeout for message reserves, in seconds
     */
    protected int $defaultTimeout = 60;

    /**
     * Interval messages are looked up in waitAnd*(), in microseconds
     */
    protected int $pollInterval = 1000000;

    /**
     * Interval messages are looked up in waitAnd*(), if any messages were processed within the last $boostTime microseconds; in microseconds
     */
    protected int $boostPollInterval = 500000;

    /**
     * Number of microseconds of the "boost time": If any messages were processed within that time, the special $boostPollInterval is used instead of the default $pollInterval; in microseconds
     */
    protected int $boostTime = 10000000;

    /**
     * Time when the last message was processed
     */
    protected int $lastMessageTime = 0;

    /**
     * Age after which a reservation of a process on another host is considered orphaned, in seconds
     */
    protected int $reservationTimeout = 3600;

    /**
     * Time when orphaned reservations were last searched for
     */
    protected int $lastStaleReservationsCheckTime = 0;

    /**
     * Name of the table to store queue messages. Defaults to "<name>_messages"
     */
    protected string $tableName;

    protected ?LoggerInterface $logger = null;

    public function __construct(string $name, array $options)
    {
        $this->name = $name;
        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = (int)$options['defaultTimeout'];
        }
        if (isset($options['pollInterval'])) {
            $this->pollInterval = (int)($options['pollInterval'] * 1000000);
        }
        if (isset($options['boostPollInterval'])) {
            $this->boostPollInterval = (int)($options['boostPollInterval'] * 1000000);
        }
        if (isset($options['boostTime'])) {
            $this->boostTime = (int)($options['boostTime'] * 1000000);
        }
        if (isset($options['reservationTimeout'])) {
            $this->reservationTimeout = (int)$options['reservationTimeout'];
        }
        if (isset($options['tableName'])) {
            $this->tableName = $options['tableName'];
        } else {
            $this->tableName = 'flowpack_jobqueue_messages_' . $this->name;
        }
        $this->options = $options;
    }

    /**
     * @throws DBALException
     */
    public function injectDoctrineEntityManager(EntityManagerInterface $doctrineEntityManager): void
    {
        if (isset($this->options['backendOptions'])) {
            $this->connection = DriverManager::getConnection($this->options['backendOptions']);
        } else {
            $this->connection = $doctrineEntityManager->getConnection();
        }
    }

    public function injectLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }

    /**
     * Creates the queue table, or adds columns missing in a table created by an earlier version
     *
     * @inheritdoc
     * @throws DBALException
     */
    public function setUp(): void
    {
        $databasePlatform = $this->connection->getDatabasePlatform();
        if ($databasePlatform === null) {
            throw new \RuntimeException('No database platform for current connection', 1703863019);
        }
        $schemaManager = $this->connection->getSchemaManager();
        if ($schemaManager === null) {
            throw new \RuntimeException('No schema manager for current connection', 1703863021);
        }

        $schema = new Schema();
        $table = $schema->createTable($this->connection->quoteIdentifier($this->tableName));
        $table->addColumn('id', 'integer', ['autoincrement' => true]);
        $table->addColumn('payload', 'text');
        $table->addColumn('state', 'string', ['length' => 255]);
        $table->addColumn('failures', 'integer', ['default' => 0]);
        $table->addColumn('scheduled', 'datetime', ['notnull' => false]);
        $table->addColumn('reserved_at', 'datetime', ['notnull' => false]);
        $table->addColumn('reserved_by', 'string', ['length' => 255, 'notnull' => false]);
        $table->setPrimaryKey(['id']);
        $table->addIndex(['state', 'scheduled'], 'state_scheduled');

        if (!$schemaManager->tablesExist($this->tableName)) {
            foreach ($schema->toSql($databasePlatform) as $createDatabaseStatement) {
                $this->connection->executeStatement($createDatabaseStatement);
            }
            return;
        }

        $existingTable = $schemaManager->listTableDetails($this->tableName);
        $this->addMissingColumns($existingTable, $table);
    }

    /**
     * @inheritdoc
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @inheritdoc
     * @throws DBALException
     * @throws \JsonException
     * @throws \Doctrine\DBAL\Driver\Exception
     */
    public function submit($payload, array $options = []): string
    {
        if ($this->connection->getDatabasePlatform()->getName() === 'postgresql') {
            $insertStatement = $this->connection->prepare(sprintf(
                "INSERT INTO %s (payload, state, scheduled) VALUES (:payload, 'ready', %s) RETURNING id",
                $this->connection->quoteIdentifier($this->tableName),
                $this->resolveScheduledQueryPart($options)
            ));
            $result = $insertStatement->executeQuery(['payload' => json_encode($payload, JSON_THROW_ON_ERROR)]);
            return (string)$result->fetchOne();
        }

        $numberOfAffectedRows = (int)$this->connection->executeStatement(
            sprintf(
                "INSERT INTO %s (payload, state, scheduled) VALUES (:payload, 'ready', %s)",
                $this->connection->quoteIdentifier($this->tableName),
                $this->resolveScheduledQueryPart($options)
            ),
            ['payload' => json_encode($payload, JSON_THROW_ON_ERROR)]
        );
        if ($numberOfAffectedRows !== 1) {
            return '';
        }
        return (string)$this->connection->lastInsertId();
    }

    /**
     * @inheritdoc
     * @throws DBALException
     * @throws Exception
     * @throws \Doctrine\DBAL\Driver\Exception
     * @throws \JsonException
     */
    public function waitAndTake(?int $timeout = null): ?Message
    {
        $message = $this->reserveMessage($timeout);
        if ($message === null) {
            return null;
        }

        $numberOfDeletedRows = $this->connection->delete(
            $this->connection->quoteIdentifier($this->tableName),
            ['id' => (int)$message->getIdentifier()]
        );
        if ($numberOfDeletedRows !== 1) {
            // TODO error handling
            return null;
        }

        return $message;
    }

    /**
     * @inheritdoc
     * @throws DBALException
     * @throws \Doctrine\DBAL\Driver\Exception
     * @throws \JsonException
     */
    public function waitAndReserve(?int $timeout = null): ?Message
    {
        return $this->reserveMessage($timeout);
    }

    /**
     * @throws DBALException
     * @throws \JsonException
     * @throws \Doctrine\DBAL\Driver\Exception
     */
    protected function reserveMessage(?int $timeout = null): ?Message
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $this->reconnectDatabaseConnection();

        $selectMessageQuery = $this->createQueryBuilder();
        $selectMessageQuery
            ->select('*')
            ->where(
                $selectMessageQuery->expr()->and(
                    'state = :state',
                    $this->getScheduledQueryConstraint($selectMessageQuery)
                )
            )
            ->setParameter('state', 'ready')
            ->orderBy('id')
            ->setMaxResults(1);

        $reserveMessageQuery = $this->connection->createQueryBuilder();
        $reserveMessageQuerySql = $reserveMessageQuery
            ->update($this->connection->quoteIdentifier($this->tableName))
            ->set('state', ':newstate')
            ->set('reserved_at', $this->connection->getDatabasePlatform()->getCurrentTimestampSQL())
            ->set('reserved_by', ':reservedBy')
            ->where('id = :id')
            ->andWhere('state = :state')
            ->andWhere($this->getScheduledQueryConstraint($reserveMessageQuery))
            ->getSQL();

        $startTime = time();
        do {
            if (time() - $this->lastStaleReservationsCheckTime >= self::STALE_RESERVATIONS_CHECK_INTERVAL) {
                $this->releaseStaleReservations();
            }
            try {
                $row = $selectMessageQuery->execute()->fetchAssociative();
            } catch (TableNotFoundException $exception) {
                throw new \RuntimeException(sprintf('The queue table "%s" could not be found. Did you run ./flow queue:setup "%s"?', $this->tableName, $this->name), 1469117906, $exception);
            }
            if ($row !== false) {
                $numberOfUpdatedRows = (int)$this->connection->executeStatement(
                    $reserveMessageQuerySql,
                    [
                        'newstate' => 'reserved',
                        'reservedBy' => $this->getProcessIdentifier(),
                        'id' => (int)$row['id'],
                        'state' => 'ready'
                    ]
                );
                if ($numberOfUpdatedRows === 1) {
                    $this->lastMessageTime = time();
                    return $this->getMessageFromRow($row);
                }
            }
            if (time() - $startTime >= $timeout) {
                return null;
            }

            $currentPollInterval = ($this->lastMessageTime + (int)($this->boostTime / 1000000) > time()) ? $this->boostPollInterval : $this->pollInterval;
            usleep($currentPollInterval);
        } while (true);
    }

    /**
     * @inheritdoc
     * @throws DBALException
     */
    public function release(string $messageId, array $options = []): void
    {
        $this->connection->executeStatement(
            sprintf(
                "UPDATE %s SET state = 'ready', failures = failures + 1, scheduled = %s, reserved_at = NULL, reserved_by = NULL WHERE id = :id",
                $this->connection->quoteIdentifier($this->tableName),
                $this->resolveScheduledQueryPart($options)
            ),
            ['id' => (int)$messageId]);
    }

    /**
     * @inheritdoc
     */
    public function abort(string $messageId): void
    {
        $this->connection->update(
            $this->connection->quoteIdentifier($this->tableName),
            ['state' => 'failed'],
            ['id' => (int)$messageId]
        );
    }

    /**
     * @inheritdoc
     * @throws InvalidArgumentException
     */
    public function finish(string $messageId): bool
    {
        return $this->connection->delete($this->connection->quoteIdentifier($this->tableName), ['id' => (int)$messageId]) === 1;
    }

    /**
     * @inheritdoc
     */
    public function peek(int $limit = 1): array
    {
        $selectMessagesQuery = $this->createQueryBuilder();
        $selectMessagesQuery
            ->select('*')
            ->where('state = :state')
            ->andWhere($this->getScheduledQueryConstraint($selectMessagesQuery))
            ->setParameter('state', 'ready')
            ->orderBy('id')
            ->setMaxResults($limit);
        $rows = $selectMessagesQuery->execute()->fetchAllAssociative();
        $messages = [];

        foreach ($rows as $row) {
            $messages[] = $this->getMessageFromRow($row);
        }

        return $messages;
    }

    /**
     * @inheritdoc
     */
    public function countReady(): int
    {
        return (int)$this->createQueryBuilder()
            ->select('COUNT(*)')
            ->where('state = :state')
            ->setParameter('state', 'ready')
            ->execute()
            ->fetchOne();
    }

    /**
     * @inheritdoc
     */
    public function countReserved(): int
    {
        return (int)$this->createQueryBuilder()
            ->select('COUNT(*)')
            ->where('state = :state')
            ->setParameter('state', 'reserved')
            ->execute()
            ->fetchOne();
    }

    /**
     * @inheritdoc
     */
    public function countFailed(): int
    {
        return (int)$this->createQueryBuilder()
            ->select('COUNT(*)')
            ->where('state = :state')
            ->setParameter('state', 'failed')
            ->execute()
            ->fetchOne();
    }

    /**
     * @throws DBALException
     */
    public function flush(): void
    {
        $schemaManager = $this->connection->getSchemaManager();
        if ($schemaManager === null) {
            throw new \RuntimeException('No schema manager in current connection', 1703863433);
        }

        if ($schemaManager->tablesExist($this->tableName)) {
            $schemaManager->dropTable($this->connection->quoteIdentifier($this->tableName));
        }
        $this->setUp();
    }

    /**
     * Releases reservations whose process is gone or which are older than the reservation timeout
     *
     * A reservation without a timestamp was made by an earlier version of this queue. It gets the
     * current time as its timestamp, so that it is released after the regular timeout.
     *
     * @throws DBALException
     */
    protected function releaseStaleReservations(): void
    {
        $this->lastStaleReservationsCheckTime = time();
        $databasePlatform = $this->connection->getDatabasePlatform();
        $tableName = $this->connection->quoteIdentifier($this->tableName);

        $this->connection->executeStatement(
            sprintf("UPDATE %s SET reserved_at = %s WHERE state = 'reserved' AND reserved_at IS NULL", $tableName, $databasePlatform->getCurrentTimestampSQL())
        );

        $rows = $this->connection->fetchAllAssociative(
            sprintf(
                "SELECT id, reserved_by, CASE WHEN reserved_at <= %s THEN 1 ELSE 0 END AS timed_out FROM %s WHERE state = 'reserved'",
                $databasePlatform->getDateSubSecondsExpression($databasePlatform->getCurrentTimestampSQL(), $this->reservationTimeout),
                $tableName
            )
        );
        foreach ($rows as $row) {
            $reservedBy = $row['reserved_by'] !== null ? (string)$row['reserved_by'] : null;
            $reason = $this->getStaleReservationReason($reservedBy, (bool)$row['timed_out']);
            if ($reason === null) {
                continue;
            }
            $numberOfReleasedRows = (int)$this->connection->executeStatement(
                sprintf(
                    "UPDATE %s SET state = 'ready', failures = failures + 1, reserved_at = NULL, reserved_by = NULL WHERE id = :id AND state = 'reserved' AND %s",
                    $tableName,
                    $reservedBy === null ? 'reserved_by IS NULL' : 'reserved_by = :reservedBy'
                ),
                $reservedBy === null ? ['id' => (int)$row['id']] : ['id' => (int)$row['id'], 'reservedBy' => $reservedBy]
            );
            if ($numberOfReleasedRows === 1 && $this->logger !== null) {
                $this->logger->warning(sprintf('Released message %s in queue "%s" which was reserved by "%s": %s', $row['id'], $this->name, $reservedBy ?? 'unknown', $reason));
            }
        }
    }

    /**
     * Returns why the given reservation is orphaned, or null if it may still be in progress
     *
     * The PID check only works for processes on this host. A reservation of another host
     * (for example a container which was replaced) can only be judged by its age.
     */
    protected function getStaleReservationReason(?string $reservedBy, bool $timedOut): ?string
    {
        if ($reservedBy !== null && preg_match('/^(?<hostname>.+):(?<pid>\d+)$/', $reservedBy, $matches) === 1 && $matches['hostname'] === $this->getHostname()) {
            return $this->isProcessRunning((int)$matches['pid']) ? null : 'the reserving process has ended';
        }
        return $timedOut ? sprintf('the reservation is older than %d seconds', $this->reservationTimeout) : null;
    }

    /**
     * Identifies the current process across hosts, in the form "<hostname>:<pid>"
     */
    protected function getProcessIdentifier(): string
    {
        return $this->getHostname() . ':' . getmypid();
    }

    protected function getHostname(): string
    {
        return (string)gethostname();
    }

    /**
     * Without the POSIX extension, every process counts as running, so that only the timeout applies.
     */
    protected function isProcessRunning(int $pid): bool
    {
        if (!function_exists('posix_kill')) {
            return true;
        }
        if (posix_kill($pid, 0)) {
            return true;
        }
        // EPERM: the process exists, but belongs to another user
        return posix_get_last_error() === 1;
    }

    /**
     * Adds the columns of $desiredTable which $existingTable lacks. Existing columns are left
     * untouched, because altering them could rewrite large tables.
     *
     * @throws DBALException
     */
    protected function addMissingColumns(Table $existingTable, Table $desiredTable): void
    {
        $addedColumns = [];
        foreach ($desiredTable->getColumns() as $column) {
            if (!$existingTable->hasColumn($column->getName())) {
                $addedColumns[$column->getName()] = $column;
            }
        }
        if ($addedColumns === []) {
            return;
        }
        // The introspected table has an unquoted name, which breaks names like "flowpack_jobqueue_messages_test-queue"
        $quotedTableName = $this->connection->quoteIdentifier($this->tableName);
        $existingTable = new Table($quotedTableName, $existingTable->getColumns(), $existingTable->getIndexes());
        $tableDiff = new TableDiff($quotedTableName, $addedColumns, [], [], [], [], [], $existingTable);
        foreach ($this->connection->getDatabasePlatform()->getAlterTableSQL($tableDiff) as $alterTableStatement) {
            $this->connection->executeStatement($alterTableStatement);
        }
    }

    /**
     * @throws \JsonException
     */
    protected function getMessageFromRow(array $row): Message
    {
        return new Message((string)$row['id'], json_decode($row['payload'], true, 512, JSON_THROW_ON_ERROR), (int)$row['failures']);
    }

    /**
     * @throws Exception
     */
    protected function resolveScheduledQueryPart(array $options): string
    {
        if (!isset($options['delay'])) {
            return 'null';
        }

        return $this->connection->getDatabasePlatform()->getDateAddSecondsExpression($this->connection->getDatabasePlatform()->getCurrentTimestampSQL(), (int)$options['delay']);
    }

    protected function getScheduledQueryConstraint(QueryBuilder $qb): CompositeExpression
    {
        return $qb->expr()->or(
            $qb->expr()->isNull('scheduled'),
            $qb->expr()->lte('scheduled', $this->connection->getDatabasePlatform()->getCurrentTimestampSQL())
        );
    }

    private function createQueryBuilder(): QueryBuilder
    {
        return $this->connection->createQueryBuilder()
            ->from($this->connection->quoteIdentifier($this->tableName));
    }

    /**
     * Reconnects the database connection associated with this queue, if it doesn't respond to a ping
     *
     * @see \Neos\Flow\Persistence\Doctrine\PersistenceManager::persistAll()
     */
    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->connection->fetchOne('SELECT 1');
        } catch (\Exception $e) {
            $this->connection->close();
            $this->connection->connect();
        }
    }
}
