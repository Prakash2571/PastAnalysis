#!/usr/bin/env bash
#
# Prints a snapshot of hybrid futures backfill progress.
#
set -euo pipefail

DB="${MONGO_DB:-past_data}"

docker compose exec -T mongo mongosh --quiet "$DB" --eval '
  var total = db.hourly_futures.countDocuments({});
  var done = db.backfill_progress.countDocuments({status: "done"});
  var symbols_done = db.backfill_progress.distinct("symbol", {status: "done"}).length;
  if (total > 0) {
    print("total candles : " + total);
    print("tasks done    : " + done);
    print("symbols done  : " + symbols_done);
    print("distinct syms : " + db.hourly_futures.distinct("symbol").length);
    print("--- by timeframe ---");
    db.hourly_futures.aggregate([
      {$group: {_id: "$timeframe", n: {$sum: 1},
                min: {$min: "$date"}, max: {$max: "$date"}}}
    ]).forEach(function(t) {
      print("  " + t._id + ": " + t.n + " candles (" +
            t.min.toISOString().slice(0,10) + " -> " +
            t.max.toISOString().slice(0,10) + ")");
    });
  } else {
    print("No documents yet. Is the backfill running?  (make logs)");
  }
'
