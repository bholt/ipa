<img align="right" width="140" src="http://bholt.org/img/ipa.jpg"/>

# IPA

[![CircleCI](https://circleci.com/gh/bholt/ipa.svg?style=svg)](https://circleci.com/gh/bholt/ipa)

This is the prototype implementation of the *Inconsistent Performance-bound Approximate (IPA)* programming model and storage system. This is part of the [*Disciplined Inconsistency*][di] project, which aims to make it safe to trade off consistency for performance in highly available applications built on top of replicated & distributed datastores.

The prototype implementation is built on top of [Cassandra][] in [Scala][], using Outworkers' [Phantom][] library to connect with Cassandra. The project is organized and built with [SBT][] (v0.13).

To learn more, read a [blog post][] explaining the high-level motivation for the project, or the latest draft of the paper under submission, available currently as a [tech report][].

[di]: http://sampa.cs.washington.edu/projects/disciplined-inconsistency.html
[blog post]: http://homes.cs.washington.edu/~bholt/posts/disciplined-inconsistency.html
[tech report]: http://bholt.github.io/gen/ipa-tr.pdf

## Overview
- `/src`: IPA source code (sbt project)
	- `ipa`: Core IPA code
	- `ipa.types`: IPA Type system, implemented as higher-kinded Scala types
	- `ipa.adts`: Data structures implemented with support for IPA annotations. A good example ADT to look at is `IPASet`, which uses `RushCommon` to implement `LatencyBound` and a `ReservationPool` to implement `ErrorBound`.
	- `ipa.apps`: Applications built using IPA types
		- **RawMix:** Microbenchmark which does random mix of IPASet operations
		- **RawMixCounter:** Microbenchmark like RawMix but with IPACounter ops
		- **Retwis:** Twitter clone
		- **TicketSleuth:** Ticket sales service
- `experiments.py`: Script to run parameter sweeps for each of the test apps
- `swarm.py`: Helpers to create a Docker Swarm instance and setup IPA jobs (Cassandra+ReservationServer containers + Client containers)
- `honeycomb.py`: Helpers that setup containers with simulated adverse network conditions using `tc netem`. Conditions for our experiments are defined in `honeycomb.yml`.

## Building & Running
This code has not been specifically streamlined to make it easy for others to run. However, it should be fairly straightforward given that most of it runs inside Docker, which is a standardized environment. The trickest part is usually getting the Docker Swarm configured correctly (this code was written and test in Docker v1.11, before Docker's new orchestration support was unveiled, so this process may be much simpler now).

Basic workflow to run the code should go as follows:

1. Install Java v8+, Scala v2.11, and SBT v0.13+.
2. Ensure it builds correctly with `sbt compile`
3. Launch a Swarm: 
	- Edit MASTER and AGENTS in `swarm.py` with hostnames of machines which must:
		- Be able to be ssh'd to
		- Not require a password to sudo
		- Have Docker v1.11 (or newer?) installed
	- Run `./swarm.py up`
	- Cross your fingers.
4. Run an IPA job with `./experiments.py ...`
	- See script to decipher the available options, or contact me.

[Cassandra]: http://cassandra.apache.org/
[Scala]: http://www.scala-lang.org/
[Phantom]: http://outworkers.github.io/phantom/
[SBT]: http://www.scala-sbt.org/
