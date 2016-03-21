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
  4: uuid id
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
  SIZE,
  TRUNCATE
}

struct SetOp {
  1: SetOpType op
  2: optional uuid key
  3: optional Primitive value
}

struct SetResult {
  1: optional IntervalLong size
  2: optional bool contains
}

enum CounterOpType { INIT, INCR, DECR, VALUE, TRUNCATE }

struct BoundedCounterOp {
  1: CounterOpType op
  2: optional uuid key
  3: optional i64 n
  4: bool forwarded = false
  5: optional i64 estTravelTime
}

struct CounterResult {
  1: optional i64 value
  2: optional bool success
  3: optional i32 min
  4: optional i32 max
  5: optional string consistency
  6: optional i64 processingTime
}

service ReservationService {

  CounterResult bounded_counter(1: Table t, 2: BoundedCounterOp op)
    throws (1: ReservationException e, 2: ForwardTo fwd)

  CounterResult counter(1: Table t, 2: BoundedCounterOp op)
    throws (1: ReservationException e)

  SetResult ipaSet(1: Table t, 2: SetOp op)
    throws (1: ReservationException e)

  void metricsReset()
  string metricsJson()
}
