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

service ReservationService {
  /**
   * Initialize new UuidSet
   * TODO: make generic version
   */
  void createUuidset(1: string name, 2: double sizeTolerance) throws (1: ReservationException e)

  /** Initialize new Counter table. */
  void createCounter(1: string name, 2: double tolerance) throws (1: ReservationException e)

  void incr(1: string name, uuid key, i64 by) throws (1: ReservationException e)
  i64 read(1: string name, uuid key) throws (1: ReservationException e)
}
