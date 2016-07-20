# Flowpack.JobQueue.Doctrine

A job queue backend for the [Flowpack.JobQueue.Common](https://github.com/Flowpack/jobqueue-common) package based on [Doctrine](http://www.doctrine-project.org/).

## Usage

Install the package using composer:

```
composer require flowpack/jobqueue-doctrine
```

If not already installed, that will fetch its requirements, namely the `jobqueue-common` package.

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
            client:
              host: 127.0.0.11
              port: 11301
            defaultTimeout: 50
          releaseOptions:
            priority: 512
            delay: 120
```

The required tables can be created executing:

```
./flow queue:setup some-queue
```

## Specific options

The `DoctrineQueue` supports following options:

| Option                  | Type    | Default                                 | Description                              |
| ----------------------- |---------| ---------------------------------------:| ---------------------------------------- |
| defaultTimeout          | integer | 60                                      | Number of seconds new messages are waited for before a timeout occurs (This is overridden by a "timeout" argument in the `waitAndTake()` and `waitAndReserve()` methods |
| pollInterval            | integer | 1                                       | Number of seconds between SQL lookups for new messages |
| tableName               | string  | flowpack_jobqueue_messages_<queue-name> | Name of the database table for this queue. By default this is the queue name prefixed with "flowpack_jobqueue_messages_" |
| backendOptions          | array   | -                                       | Doctrine-specific connection params (see [Doctrine reference](http://doctrine-orm.readthedocs.io/projects/doctrine-dbal/en/latest/reference/configuration.html))|

*NOTE:* The `DoctrineQueue` currently supports `MySQL`, `PostgreSQL` and `SQLite` backends. You can specify the backend via the `backendOptions`. If you omit this setting, the *current connection* will be re-used (i.e. the currently active Flow database).

### Submit options

Additional options supported by `JobManager::queue()`, `DoctrineQueue::submit()` and the `Job\Defer` annotation:

| Option                  | Type    | Default          | Description                              |
| ----------------------- |---------| ----------------:| ---------------------------------------- |
| delay                   | integer | 0                | Number of seconds before a message is marked "ready" after submission. This can be useful to prevent premature execution of jobs (i.e. before entities are persisted) |

### Release options

Additional options to be specified via `releaseOptions`: 

| Option                  | Type    | Default          | Description                              |
| ----------------------- |---------| ----------------:| ---------------------------------------- |
| delay                   | integer | 0                | Number of seconds before a message is marked "ready" after it has been released. |

## License

This package is licensed under the MIT license

## Contributions

Pull-Requests are more than welcome. Make sure to read the [Code Of Conduct](CodeOfConduct.rst).
