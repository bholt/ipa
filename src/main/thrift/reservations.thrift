namespace java ipa.thrift

exception WriteException {}
exception ReadException {}

service LoggerService {
  string log(1: string message, 2: i32 logLevel) throws (1: WriteException e);
  i32 getLogSize() throws (1: ReadException e);
}

exception ReservationException {
  1: string why
}

typedef string uuid

union Primitive {
  1: i64 int
  2: double dbl
  3: string str
}

struct Inconsistent {
  1: Primitive value
}

struct Interval {
  1: Primitive min
  2: Primitive max
}

struct IntervalLong {
  1: i64 min
  2: i64 max
}

union Result {
  1: Inconsistent inconsistent
  2: Interval interval
}

struct Table {
  1: string space
  2: string name
}

service ReservationService {
  /**
   * Initialize new UuidSet
   * TODO: make generic version
   */
  void createUuidset(1: Table tbl, 2: double sizeTolerance) throws (1: ReservationException e)

  /** Initialize new Counter table. */
  void createCounter(1: Table tbl, 2: double tolerance) throws (1: ReservationException e)

  void incr(1: Table tbl, uuid key, i64 by) throws (1: ReservationException e)

  IntervalLong readInterval(1: Table tbl, uuid key) throws (1: ReservationException e)

  string metricsJson()
}
