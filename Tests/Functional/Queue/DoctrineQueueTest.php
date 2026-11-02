<?php
declare(strict_types=1);
namespace Flowpack\JobQueue\Doctrine\Tests\Functional\Queue;

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
use Flowpack\JobQueue\Doctrine\Queue\DoctrineQueue;
use Flowpack\JobQueue\Common\Tests\Functional\AbstractQueueTest;
use Neos\Utility\ObjectAccess;

/**
 * Functional test for DoctrineQueue
 */
class DoctrineQueueTest extends AbstractQueueTest
{

    /**
     * @inheritdoc
     */
    protected function getQueue()
    {
        return new DoctrineQueue('test-queue', $this->queueSettings);
    }

    public function testSetUpAddsReservationColumnsToExistingTable(): void
    {
        $connection = $this->getConnection();
        $connection->executeStatement(sprintf('ALTER TABLE %s DROP COLUMN reserved_at', $this->getQuotedTableName()));
        $connection->executeStatement(sprintf('ALTER TABLE %s DROP COLUMN reserved_by', $this->getQuotedTableName()));

        $this->queue->setUp();

        $columnNames = array_keys($connection->createSchemaManager()->listTableColumns(ObjectAccess::getProperty($this->queue, 'tableName', true)));
        self::assertContains('reserved_at', $columnNames);
        self::assertContains('reserved_by', $columnNames);
    }

    public function testReservingAMessageRecordsTheReservingProcess(): void
    {
        $messageId = $this->queue->submit('First message');
        $this->queue->waitAndReserve(1);

        $row = $this->fetchRow($messageId);
        self::assertSame(gethostname() . ':' . getmypid(), $row['reserved_by']);
        self::assertNotNull($row['reserved_at']);
    }

    public function testReservationsOfOtherHostsAreReleasedAfterTheReservationTimeout(): void
    {
        $expiredMessageId = $this->submitReservedMessage('other-host:123', 7200);
        $recentMessageId = $this->submitReservedMessage('other-host:124', 600);

        $message = $this->createQueue(['reservationTimeout' => 3600])->waitAndReserve(1);

        self::assertSame($expiredMessageId, $message->getIdentifier());
        self::assertSame(1, $message->getNumberOfReleases());
        self::assertSame('reserved', $this->fetchRow($recentMessageId)['state']);
        self::assertSame('other-host:124', $this->fetchRow($recentMessageId)['reserved_by']);
    }

    public function testReservationsOfEndedProcessesOnThisHostAreReleasedImmediately(): void
    {
        $endedProcessId = $this->getIdOfEndedProcess();
        $messageId = $this->submitReservedMessage(gethostname() . ':' . $endedProcessId, 0);

        $message = $this->createQueue(['reservationTimeout' => 3600])->waitAndReserve(1);

        self::assertSame($messageId, $message->getIdentifier());
        self::assertSame(1, $message->getNumberOfReleases());
    }

    public function testReservationsOfRunningProcessesOnThisHostAreKeptEvenAfterTheTimeout(): void
    {
        $messageId = $this->submitReservedMessage(gethostname() . ':' . getmypid(), 7200);

        self::assertNull($this->createQueue(['reservationTimeout' => 3600])->waitAndReserve(1));
        self::assertSame('reserved', $this->fetchRow($messageId)['state']);
    }

    public function testReservationsWithoutTimestampGetOneAndAreKept(): void
    {
        $messageId = $this->submitReservedMessage(null, null);

        self::assertNull($this->createQueue(['reservationTimeout' => 3600])->waitAndReserve(1));
        $row = $this->fetchRow($messageId);
        self::assertSame('reserved', $row['state']);
        self::assertNotNull($row['reserved_at']);
    }

    private function createQueue(array $options): DoctrineQueue
    {
        return new DoctrineQueue('test-queue', array_merge($this->queueSettings, $options));
    }

    private function getConnection(): Connection
    {
        return ObjectAccess::getProperty($this->queue, 'connection', true);
    }

    /**
     * @param int|null $reservationAge in seconds; null for a reservation without timestamp
     */
    private function submitReservedMessage(?string $reservedBy, ?int $reservationAge): string
    {
        $messageId = $this->queue->submit('Reserved message');
        $databasePlatform = $this->getConnection()->getDatabasePlatform();
        $reservedAtExpression = $reservationAge === null ? 'NULL' : $databasePlatform->getDateSubSecondsExpression($databasePlatform->getCurrentTimestampSQL(), $reservationAge);
        $this->getConnection()->executeStatement(
            sprintf("UPDATE %s SET state = 'reserved', reserved_at = %s, reserved_by = :reservedBy WHERE id = :id", $this->getQuotedTableName(), $reservedAtExpression),
            ['reservedBy' => $reservedBy, 'id' => (int)$messageId]
        );
        return $messageId;
    }

    private function fetchRow(string $messageId): array
    {
        return $this->getConnection()->fetchAssociative(sprintf('SELECT * FROM %s WHERE id = :id', $this->getQuotedTableName()), ['id' => (int)$messageId]);
    }

    private function getQuotedTableName(): string
    {
        return $this->getConnection()->quoteIdentifier(ObjectAccess::getProperty($this->queue, 'tableName', true));
    }

    private function getIdOfEndedProcess(): int
    {
        $process = proc_open(['true'], [], $pipes);
        $processId = proc_get_status($process)['pid'];
        proc_close($process);
        return $processId;
    }
}
