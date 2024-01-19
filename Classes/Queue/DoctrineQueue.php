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

        $startTime = time();
        do {
            try {
                $row = $this->connection->fetchAssociative("SELECT * FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'ready' AND {$this->getScheduledQueryConstraint()} ORDER BY id ASC LIMIT 1");
            } catch (TableNotFoundException $exception) {
                throw new \RuntimeException(sprintf('The queue table "%s" could not be found. Did you run ./flow queue:setup "%s"?', $this->tableName, $this->name), 1469117906, $exception);
            }
            if ($row !== false) {
                $numberOfUpdatedRows = (int)$this->connection->executeStatement("UPDATE {$this->connection->quoteIdentifier($this->tableName)} SET state = 'reserved' WHERE id = :id AND state = 'ready' AND {$this->getScheduledQueryConstraint()}", ['id' => (int)$row['id']]);
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
        $rows = $this->connection->fetchAllAssociative("SELECT * FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'ready' AND {$this->getScheduledQueryConstraint()} ORDER BY id ASC LIMIT $limit");
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
        return (int)$this->connection->fetchOne("SELECT COUNT(*) FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'ready'");
    }

    /**
     * @inheritdoc
     */
    public function countReserved(): int
    {
        return (int)$this->connection->fetchOne("SELECT COUNT(*) FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'reserved'");
    }

    /**
     * @inheritdoc
     */
    public function countFailed(): int
    {
        return (int)$this->connection->fetchColumn("SELECT COUNT(*) FROM {$this->connection->quoteIdentifier($this->tableName)} WHERE state = 'failed'");
    }

    /**
     * @throws DBALException
     */
    public function flush(): void
    {
        $this->connection->executeStatement("DROP TABLE IF EXISTS {$this->connection->quoteIdentifier($this->tableName)}");
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
        switch ($this->connection->getDatabasePlatform()->getName()) {
            case 'sqlite':
                return 'datetime(\'now\', \'+' . (int)$options['delay'] . ' second\')';
            case 'postgresql':
                return 'NOW() + INTERVAL \'' . (int)$options['delay'] . ' SECOND\'';
            default:
                return 'DATE_ADD(NOW(), INTERVAL ' . (int)$options['delay'] . ' SECOND)';
        }
    }

    protected function getScheduledQueryConstraint(QueryBuilder $qb): CompositeExpression
    {
        switch ($this->connection->getDatabasePlatform()->getName()) {
            case 'sqlite':
                return '(scheduled IS NULL OR scheduled <= datetime("now"))';
            default:
                return '(scheduled IS NULL OR scheduled <= NOW())';
        }
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
