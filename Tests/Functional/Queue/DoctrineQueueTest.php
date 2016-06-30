<?php
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

use Flowpack\JobQueue\Doctrine\Queue\DoctrineQueue;
use Flowpack\JobQueue\Common\Tests\Functional\AbstractQueueTest;

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
        return new DoctrineQueue('Test-queue', $this->queueSettings);
    }
}