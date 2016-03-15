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

exception ForwardTo {
  1: string who
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

struct Result {
   Interval interval
}

struct Table {
  1: string space
  2: string name
}

enum SetOpType {
  ADD,
  REMOVE,
  CONTAINS,
  SIZE
}

struct SetOp {
  1: SetOpType op
  2: uuid key
  3: optional Primitive value
}

enum CounterOpType { INIT, INCR, DECR, VALUE, TRUNCATE }

struct BoundedCounterOp {
  1: CounterOpType op
  2: optional uuid key
  3: optional i64 n
  4: bool forwarded = false
  5: optional i64 startTime
}

struct CounterResult {
  1: optional i64 value
  2: optional bool success
  3: optional i32 min
  4: optional i32 max
  5: optional string consistency
}

service ReservationService {
  /**
   * Initialize new UuidSet
   * TODO: make generic version
   */
  void createUuidset(1: Table tbl, 2: double sizeTolerance)
    throws (1: ReservationException e)

  /** Initialize new Counter table. */
  void createCounter(1: Table tbl, 2: double tolerance)
    throws (1: ReservationException e)

  void incr(1: Table tbl, 2: uuid key, 3: i64 by)
    throws (1: ReservationException e)

  IntervalLong readInterval(1: Table tbl, 2: uuid key)
    throws (1: ReservationException e)

  Result set_op(1: Table tbl, 2: SetOp op)
    throws (1: ReservationException e)

  CounterResult bounded_counter(1: Table t, 2: BoundedCounterOp op)
    throws (1: ReservationException e, 2: ForwardTo fwd)

  void metricsReset()
  string metricsJson()
}
