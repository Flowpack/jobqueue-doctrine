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
use Doctrine\ORM\EntityManagerInterface;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;

/**
 * A queue implementation using doctrine as the queue backend
 */
class DoctrineQueue implements QueueInterface
{
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
     * Name of the table to store queue messages. Defaults to "<name>_messages"
     */
    protected string $tableName;

    public function __construct(string $name, array $options)
    {
        $this->name = $name;
        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = (int)$options['defaultTimeout'];
        }
        if (isset($options['pollInterval'])) {
            $this->pollInterval = (int)$options['pollInterval'] * 1000000;
        }
        if (isset($options['boostPollInterval'])) {
            $this->boostPollInterval = (int)$options['boostPollInterval'] * 1000000;
        }
        if (isset($options['boostTime'])) {
            $this->boostTime = (int)$options['boostTime'] * 1000000;
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

    /**
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
        if (!$schemaManager->tablesExist($this->tableName)) {
            $schema = new Schema();
            $table = $schema->createTable($this->connection->quoteIdentifier($this->tableName));
            $table->addColumn('id', 'integer', ['autoincrement' => true]);
            $table->addColumn('payload', 'text');
            $table->addColumn('state', 'string', ['length' => 255]);
            $table->addColumn('failures', 'integer', ['default' => 0]);
            $table->addColumn('scheduled', 'datetime', ['notnull' => false]);
            $table->setPrimaryKey(['id']);
            $table->addIndex(['state', 'scheduled'], 'state_scheduled');

            $createDatabaseStatements = $schema->toSql($databasePlatform);
            foreach ($createDatabaseStatements as $createDatabaseStatement) {
                $this->connection->exec($createDatabaseStatement);
            }
        }
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
     */
    public function submit($payload, array $options = []): string
    {
        if ($this->connection->getDatabasePlatform()->getName() === 'postgresql') {
            $insertStatement = $this->connection->prepare("INSERT INTO {$this->connection->quoteIdentifier($this->tableName)} (payload, state, scheduled) VALUES (:payload, 'ready', {$this->resolveScheduledQueryPart($options)}) RETURNING id");
            $result = $insertStatement->executeQuery(['payload' => json_encode($payload)]);
            return (string)$result->fetchOne();
        }

        $numberOfAffectedRows = (int)$this->connection->executeStatement("INSERT INTO {$this->connection->quoteIdentifier($this->tableName)} (payload, state, scheduled) VALUES (:payload, 'ready', {$this->resolveScheduledQueryPart($options)})", ['payload' => json_encode($payload)]);
        if ($numberOfAffectedRows !== 1) {
            return '';
        }
        return (string)$this->connection->lastInsertId();
    }

    /**
     * @inheritdoc
     * @throws DBALException
     */
    public function waitAndTake(?int $timeout = null): ?Message
    {
        $message = $this->reserveMessage($timeout);
        if ($message === null) {
            return null;
        }

        $numberOfDeletedRows = $this->connection->delete($this->connection->quoteIdentifier($this->tableName), ['id' => (int)$message->getIdentifier()]);
        if ($numberOfDeletedRows !== 1) {
            // TODO error handling
            return null;
        }

        return $message;
    }

    /**
     * @inheritdoc
     * @throws DBALException
     */
    public function waitAndReserve(?int $timeout = null): ?Message
    {
        return $this->reserveMessage($timeout);
    }

    /**
     * @throws DBALException
     */
    protected function reserveMessage(?int $timeout = null): ?Message
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $this->reconnectDatabaseConnection();

        $qb = $this->connection->createQueryBuilder();
        $qb
            ->select('*')
            ->from($this->connection->quoteIdentifier($this->tableName))
            ->where(
                $qb->expr()->and(
                    $qb->expr()->eq('state', $qb->expr()->literal('ready')),
                    $this->getScheduledQueryConstraint($qb)
                )
            )
            ->orderBy('id')
            ->setMaxResults(1);
        $startTime = time();
        do {
            try {
                $row = $qb->execute()->fetchAssociative();
            } catch (TableNotFoundException $exception) {
                throw new \RuntimeException(sprintf('The queue table "%s" could not be found. Did you run ./flow queue:setup "%s"?', $this->tableName, $this->name), 1469117906, $exception);
            }
            if ($row !== false) {
                $innerQueryBuilder = $this->connection->createQueryBuilder();
                $innerQueryBuilder
                    ->update($this->connection->quoteIdentifier($this->tableName))
                    ->set('state', $innerQueryBuilder->expr()->literal('reserved'))
                    ->where(
                        $innerQueryBuilder->expr()->and(
                            $innerQueryBuilder->expr()->eq('id', (int)$row['id']),
                            $innerQueryBuilder->expr()->eq('state', $innerQueryBuilder->expr()->literal('ready')),
                            $this->getScheduledQueryConstraint($innerQueryBuilder)
                        )
                    );
                $numberOfUpdatedRows = (int)$this->connection->executeStatement($innerQueryBuilder->getSQL());
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
        $this->connection->executeStatement("UPDATE {$this->connection->quoteIdentifier($this->tableName)} SET state = 'ready', failures = failures + 1, scheduled = {$this->resolveScheduledQueryPart($options)} WHERE id = :id", ['id' => (int)$messageId]);
    }

    /**
     * @inheritdoc
     */
    public function abort(string $messageId): void
    {
        $this->connection->update($this->connection->quoteIdentifier($this->tableName), ['state' => 'failed'], ['id' => (int)$messageId]);
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
        $qb = $this->connection->createQueryBuilder();
        $qb
            ->select('*')
            ->from($this->connection->quoteIdentifier($this->tableName))
            ->where(
                $qb->expr()->and(
                    $qb->expr()->eq('state', $qb->expr()->literal('ready')),
                    $this->getScheduledQueryConstraint($qb)
                )
            )
            ->orderBy('id')
            ->setMaxResults($limit);
        $rows = $qb->execute()->fetchAllAssociative();
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
        $qb = $this->connection->createQueryBuilder();
        return (int)$qb
            ->select('COUNT(*)')
            ->from($this->connection->quoteIdentifier($this->tableName))
            ->where(
                $qb->expr()->eq('state', $qb->expr()->literal('ready')),
            )
            ->execute()
            ->fetchOne();
    }

    /**
     * @inheritdoc
     */
    public function countReserved(): int
    {
        $qb = $this->connection->createQueryBuilder();
        return (int)$qb
            ->select('COUNT(*)')
            ->from($this->connection->quoteIdentifier($this->tableName))
            ->where(
                $qb->expr()->eq('state', $qb->expr()->literal('reserved')),
            )
            ->execute()
            ->fetchOne();
    }

    /**
     * @inheritdoc
     */
    public function countFailed(): int
    {
        $qb = $this->connection->createQueryBuilder();
        return (int)$qb
            ->select('COUNT(*)')
            ->from($this->connection->quoteIdentifier($this->tableName))
            ->where(
                $qb->expr()->eq('state', $qb->expr()->literal('failed')),
            )
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

    protected function getMessageFromRow(array $row): Message
    {
        return new Message($row['id'], json_decode($row['payload'], true), (int)$row['failures']);
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
