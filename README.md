# Flowpack.JobQueue.Doctrine

A job queue backend for the [Flowpack.JobQueue.Common](https://github.com/Flowpack/jobqueue-common) package based on [Doctrine](http://www.doctrine-project.org/).

## Usage

Install the package using Composer:

```
composer require flowpack/jobqueue-doctrine
```

If not already installed, that will fetch its requirements, namely the 
`jobqueue-common` package.

Now the queue can be configured like this:

```yaml
Flowpack:
  JobQueue:
    Common:
      queues:
        'some-queue':
          className: 'Flowpack\JobQueue\Doctrine\Queue\DoctrineQueue'
          executeIsolated: true
          options:
            defaultTimeout: 50
          releaseOptions:
            priority: 512
            delay: 120
```

The required tables can be created executing:

```
./flow queue:setup some-queue
```

## Boost Mode

The poll interval should be short enough to process messages in time, and long
enough to minimize resource consumption for the database. Boost mode is a 
solution which automatically handles spikes by processing messages in quick 
succession. When no new messages appear for a specified time, boost mode is
disabled again.

The frequency by which the queue loop will look for new messages is the 
configured `pollInterval`. In boost mode, the option `boostPollInterval` is 
used instead. `boostTime` defines the time since the last processed message 
after which boost mode is deactivated again.

## Specific options

The `DoctrineQueue` supports following options:

| Option            | Type    |                                 Default | Description                                                                                                                                                             |
|-------------------|---------|----------------------------------------:|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| defaultTimeout    | integer |                                      60 | Number of seconds new messages are waited for before a timeout occurs, this is overridden by a "timeout" argument in the `waitAndTake()` and `waitAndReserve()` methods |
| pollInterval      | float   |                                       1 | Number of seconds between SQL lookups for new messages                                                                                                                  |
| boostPollInterval | float   |                                     0.5 | Number of seconds between SQL lookups for new messages when in "boost mode"                                                                                             |
| boostTime         | float   |                                      10 | Maximum number of seconds since last processed message to activate "boost mode"                                                                                         |
| tableName         | string  | flowpack_jobqueue_messages_<queue-name> | Name of the database table for this queue. By default this is the queue name prefixed with "flowpack_jobqueue_messages_"                                                |
| backendOptions    | array   |                                       - | Doctrine-specific connection params (see [Doctrine reference](http://doctrine-orm.readthedocs.io/projects/doctrine-dbal/en/latest/reference/configuration.html))        |

*NOTE:* The `DoctrineQueue` should work with any database supported by
Doctrine DBAL. It has been tested on MySQL, PostgreSQL, SQL Server and 
SQLite. You can specify the backend via the `backendOptions`. If  you
omit this setting, the *current connection* will be re-used (i.e. the 
currently active Flow database).

### Submit options

Additional options supported by `JobManager::queue()`, `DoctrineQueue::submit()` and the `Job\Defer` annotation:

| Option | Type    | Default | Description                                                                                                                                                           |
|--------|---------|--------:|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delay  | integer |       0 | Number of seconds before a message is marked "ready" after submission. This can be useful to prevent premature execution of jobs (i.e. before entities are persisted) |

### Release options

Additional options to be specified via `releaseOptions` for the queue:

| Option | Type    | Default | Description                                                                      |
|--------|---------|--------:|----------------------------------------------------------------------------------|
| delay  | integer |       0 | Number of seconds before a message is marked "ready" after it has been released. |

## License

This package is licensed under the MIT license

## Contributions

Pull-Requests are more than welcome. Make sure to read the [Code Of Conduct](CodeOfConduct.rst).
