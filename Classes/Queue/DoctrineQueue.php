<?php
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

use Doctrine\ORM\EntityManagerInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception\InvalidArgumentException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;

/**
 * A queue implementation using doctrine as the queue backend
 */
class DoctrineQueue implements QueueInterface
{
    /**
     * @var string
     */
    protected $name;

    /**
     * @var array
     */
    protected $options;

    /**
     * @var EntityManagerInterface
     */
    protected $doctrineEntityManager;

    /**
     * @var Connection
     */
    protected $connection;

    /**
     * Default timeout for message reserves, in seconds
     *
     * @var int
     */
    protected $defaultTimeout = 60;

    /**
     * Interval messages are looked up in waitAnd*(), in seconds
     *
     * @var int
     */
    protected $pollInterval = 1;

    /**
     * Name of the table to store queue messages. Defaults to "<name>_messages"
     *
     * @var string
     */
    protected $tableName;

    /**
     * @param string $name
     * @param array $options
     */
    public function __construct($name, array $options)
    {
        $this->name = $name;
        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = (integer)$options['defaultTimeout'];
        }
        if (isset($options['pollInterval'])) {
            $this->pollInterval = (integer)$options['pollInterval'];
        }
        if (isset($options['tableName'])) {
            $this->tableName = $options['tableName'];
        } else {
            $this->tableName = 'flowpack_jobqueue_messages_' . $this->name;
        }
        $this->options = $options;
    }

    /**
     * @param EntityManagerInterface $doctrineEntityManager
     * @return void
     * @throws DBALException
     */
    public function injectDoctrineEntityManager(EntityManagerInterface $doctrineEntityManager)
    {
        $this->doctrineEntityManager = $doctrineEntityManager;

        if (isset($this->options['backendOptions'])) {
            $this->connection = DriverManager::getConnection($this->options['backendOptions']);
        } else {
            $this->connection = $doctrineEntityManager->getConnection();
        }
    }

    /**
     * @throws DBALException
     */
    protected function ensureDatabaseConnection(): void
    {
        try {
            $this->connection->fetchAssoc("SELECT 1");
        } catch (DBALException $e) {
            if (isset($this->options['backendOptions'])) {
                $this->connection = DriverManager::getConnection($this->options['backendOptions']);
            } else {
                $this->connection = $this->doctrineEntityManager->getConnection();
            }
        }
    }

    /**
     * @inheritdoc
     * @throws DBALException
     */
    public function setUp(): void
    {
        switch ($this->connection->getDatabasePlatform()->getName()) {
            case 'sqlite':
                $createDatabaseStatement = "CREATE TABLE IF NOT EXISTS {$this->connection->quoteIdentifier($this->tableName)} (id INTEGER PRIMARY KEY AUTOINCREMENT, payload LONGTEXT NOT NULL, state VARCHAR(255) NOT NULL, failures INTEGER NOT NULL DEFAULT 0, scheduled TEXT DEFAULT NULL)";
                break;
            case 'postgresql':
                $createDatabaseStatement = "CREATE TABLE IF NOT EXISTS {$this->connection->quoteIdentifier($this->tableName)} (id SERIAL PRIMARY KEY, payload TEXT NOT NULL, state VARCHAR(255) NOT NULL, failures INTEGER NOT NULL DEFAULT 0, scheduled TIMESTAMP(0) WITHOUT TIME ZONE DEFAULT NULL)";
                break;
            default:
                $createDatabaseStatement = "CREATE TABLE IF NOT EXISTS {$this->connection->quoteIdentifier($this->tableName)} (id INTEGER PRIMARY KEY AUTO_INCREMENT, payload LONGTEXT NOT NULL, state VARCHAR(255) NOT NULL, failures INTEGER NOT NULL DEFAULT 0, scheduled DATETIME DEFAULT NULL) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB";
        }
        $this->connection->exec($createDatabaseStatement);
        try {
            $this->connection->exec("CREATE INDEX state_scheduled ON {$this->connection->quoteIdentifier($this->tableName)} (state, scheduled)");
        } catch (DBALException $e) {
            // See https://dba.stackexchange.com/questions/24531/mysql-create-index-if-not-exists
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
        $this->ensureDatabaseConnection();

        if ($this->connection->getDatabasePlatform()->getName() === 'postgresql') {
            $insertStatement = $this->connection->prepare("INSERT INTO {$this->connection->quoteIdentifier($this->tableName)} (payload, state, scheduled) VALUES (:payload, 'ready', {$this->resolveScheduledQueryPart($options)}) RETURNING id");
            $insertStatement->execute(['payload' => json_encode($payload)]);
            return (string)$insertStatement->fetchColumn(0);
        } else {
            $numberOfAffectedRows = $this->connection->executeUpdate("INSERT INTO {$this->connection->quoteIdentifier($this->tableName)} (payload, state, scheduled) VALUES (:payload, 'ready', {$this->resolveScheduledQueryPart($options)})", ['payload' => json_encode($payload)]);
            if ($numberOfAffectedRows !== 1) {
                return null;
            }
            return (string)$this->connection->lastInsertId();
        }
    }

    /**
     * @inheritdoc
     * @throws DBALException
     */
    public function waitAndTake(?int $timeout = null): ?Message
    {
        $this->ensureDatabaseConnection();

        $message = $this->reserveMessage($timeout);
        if ($message === null) {
            return null;
        }

        $numberOfDeletedRows = $this->connection->delete($this->connection->quoteIdentifier($this->tableName), ['id' => (integer)$message->getIdentifier()]);
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
     * @param int $timeout
     * @return Message
     * @throws DBALException
     */
    protected function reserveMessage(?int $timeout = null): ?Message
    {
        $this->ensureDatabaseConnection();

        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }

        $startTime = time();
        do {
            try {
                $row = $this->connection->fetchAssoc("SELECT * FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'ready' AND {$this->getScheduledQueryConstraint()} LIMIT 1");
            } catch (TableNotFoundException $exception) {
                throw new \RuntimeException(sprintf('The queue table "%s" could not be found. Did you run ./flow queue:setup "%s"?', $this->tableName, $this->name), 1469117906, $exception);
            }
            if ($row !== false) {
                $numberOfUpdatedRows = $this->connection->executeUpdate("UPDATE {$this->connection->quoteIdentifier($this->tableName)} SET state = 'reserved' WHERE id = :id AND state = 'ready' AND {$this->getScheduledQueryConstraint()}", ['id' => (integer)$row['id']]);
                if ($numberOfUpdatedRows === 1) {
                    return $this->getMessageFromRow($row);
                }
            }
            if (time() - $startTime >= $timeout) {
                return null;
            }
            sleep($this->pollInterval);
        } while (true);

        return null;
    }

    /**
     * @inheritdoc
     * @throws DBALException
     */
    public function release(string $messageId, array $options = []): void
    {
        $this->ensureDatabaseConnection();

        $this->connection->executeUpdate("UPDATE {$this->connection->quoteIdentifier($this->tableName)} SET state = 'ready', failures = failures + 1, scheduled = {$this->resolveScheduledQueryPart($options)} WHERE id = :id", ['id' => (integer)$messageId]);
    }

    /**
     * @inheritdoc
     */
    public function abort(string $messageId): void
    {
        $this->ensureDatabaseConnection();

        $this->connection->update($this->connection->quoteIdentifier($this->tableName), ['state' => 'failed'], ['id' => (integer)$messageId]);
    }

    /**
     * @inheritdoc
     * @throws InvalidArgumentException
     */
    public function finish(string $messageId): bool
    {
        $this->ensureDatabaseConnection();

        return $this->connection->delete($this->connection->quoteIdentifier($this->tableName), ['id' => (integer)$messageId]) === 1;
    }

    /**
     * @inheritdoc
     */
    public function peek(int $limit = 1): array
    {
        $this->ensureDatabaseConnection();

        $limit = (integer)$limit;
        $rows = $this->connection->fetchAll("SELECT * FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'ready' AND {$this->getScheduledQueryConstraint()} LIMIT $limit");
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
        $this->ensureDatabaseConnection();

        return (integer)$this->connection->fetchColumn("SELECT COUNT(*) FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'ready'");
    }

    /**
     * @inheritdoc
     */
    public function countReserved(): int
    {
        $this->ensureDatabaseConnection();

        return (integer)$this->connection->fetchColumn("SELECT COUNT(*) FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'reserved'");
    }

    /**
     * @inheritdoc
     */
    public function countFailed(): int
    {
        $this->ensureDatabaseConnection();

        return (integer)$this->connection->fetchColumn("SELECT COUNT(*) FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'failed'");
    }

    /**
     * @return void
     * @throws DBALException
     */
    public function flush(): void
    {
        $this->ensureDatabaseConnection();

        $this->connection->exec("DROP TABLE IF EXISTS {$this->connection->quoteIdentifier($this->tableName)}");
        $this->setUp();
    }

    /**
     * @param array $row
     * @return Message
     */
    protected function getMessageFromRow(array $row): Message
    {
        return new Message($row['id'], json_decode($row['payload'], true), (integer)$row['failures']);
    }

    /**
     * @param array $options
     * @return string
     */
    protected function resolveScheduledQueryPart(array $options): string
    {
        if (!isset($options['delay'])) {
            return 'null';
        }
        switch ($this->connection->getDatabasePlatform()->getName()) {
            case 'sqlite':
                return 'datetime(\'now\', \'+' . (integer)$options['delay'] . ' second\')';
            case 'postgresql':
                return 'NOW() + INTERVAL \'' . (integer)$options['delay'] . ' SECOND\'';
            default:
                return 'DATE_ADD(NOW(), INTERVAL ' . (integer)$options['delay'] . ' SECOND)';
        }
    }

    /**
     * @return string
     */
    protected function getScheduledQueryConstraint(): string
    {
        switch ($this->connection->getDatabasePlatform()->getName()) {
            case 'sqlite':
                return '(scheduled IS NULL OR scheduled <= datetime("now"))';
            default:
                return '(scheduled IS NULL OR scheduled <= NOW())';
        }
    }
}
