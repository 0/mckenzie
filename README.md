# McKenzie

McKenzie is a companion utility for the [Slurm](https://slurm.schedmd.com/) workload manager, acting as a bridge between Slurm worker jobs and a PostgreSQL database containing task details.

You will likely find that your needs are better served by lightweight task runners (such as [GNU Parallel](https://www.gnu.org/software/parallel/) or [GLOST](https://github.com/cea-hpc/glost)), or perhaps even the job array and dependency facilities in Slurm itself. The downsides of incorporating McKenzie into a project should be considered before attempting to use it at scale. For example, it is necessary for the user to ensure that an appropriate number of worker jobs of the right capacity are active at any given time, despite the volatile nature of job scheduling.

Note that while McKenzie is functional in its current form, it should be considered pre-alpha software. In particular, there are no tests or documentation, and it makes very many assumptions about its environment. There are almost certainly bugs, although I can't make any promises.


## Prerequisites

There is currently no way to formally declare a dependency on [Psycopg](https://www.psycopg.org/), because some end users will want [`psycopg2`](https://pypi.org/project/psycopg2/) and others [`psycopg2-binary`](https://pypi.org/project/psycopg2-binary/). One of these two packages must be installed manually.


## License

This software is available under the same license as Slurm itself (GNU GPLv2 or later). See `COPYING` and `DISCLAIMER` for more information.
