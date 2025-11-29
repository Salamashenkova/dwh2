CREATE TABLE dwh_detailed.bookings (
  book_ref String,
  book_date DateTime,
  total_amount Decimal(10, 2),
  source_system_id Int32,
  created_at DateTime DEFAULT now(),
  version Int32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (book_ref);
CREATE TABLE dwh_detailed.airports (
  airport_code String,
  airport_name String,
  city String,
  coordinates_lon Float64,
  coordinates_lat Float64,
  timezone String,
  source_system_id Int32,
  created_at DateTime DEFAULT now(),
  version Int32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (airport_code);
CREATE TABLE dwh_detailed.aircrafts (
  aircraft_code String,
  model JSON,
  range Int32,
  source_system_id Int32,
  created_at DateTime DEFAULT now(),
  version Int32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (aircraft_code);
CREATE TABLE dwh_detailed.tickets (
  ticket_no String,
  book_ref String,
  passenger_id String,
  passenger_name String,
  contact_data JSON,
  source_system_id Int32,
  created_at DateTime DEFAULT now(),
  version Int32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (ticket_no);
CREATE TABLE dwh_detailed.flights (
  flight_id Int32,
  flight_no String,
  scheduled_departure DateTime,
  scheduled_arrival DateTime,
  departure_airport String,
  arrival_airport String,
  status String,
  aircraft_code String,
  actual_departure Nullable(DateTime),
  actual_arrival Nullable(DateTime),
  source_system_id Int32,
  created_at DateTime DEFAULT now(),
  version Int32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (flight_id);
CREATE TABLE dwh_detailed.ticket_flights (
  ticket_no String,
  flight_id Int32,
  fare_conditions String,
  amount Decimal(10, 2),
  source_system_id Int32,
  created_at DateTime DEFAULT now(),
  version Int32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (ticket_no, flight_id);
CREATE TABLE dwh_detailed.seats (
  aircraft_code String,
  seat_no String,
  fare_conditions String,
  source_system_id Int32,
  created_at DateTime DEFAULT now(),
  version Int32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (aircraft_code, seat_no);
CREATE TABLE dwh_detailed.boarding_passes (
  ticket_no String,
  flight_id Int32,
  boarding_no Int32,
  seat_no String,
  source_system_id Int32,
  created_at DateTime DEFAULT now(),
  version Int32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (ticket_no, flight_id);