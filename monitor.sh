#!/usr/bin/env bash
#
# Prints a snapshot of hourly futures backfill progress.
#
set -euo pipefail

DB="${MONGO_DB:-past_data}"

docker compose exec -T mongo mongosh --quiet "$DB" --eval '
  var total = db.hourly_futures.countDocuments({});
  var done = db.backfill_progress.countDocuments({status: "done"});
  var symbols_done = db.backfill_progress.distinct("symbol", {status: "done"}).length;
  if (total > 0) {
    var a = db.hourly_futures.aggregate([
      {$group: {_id: null, min: {$min: "$timestamp"}, max: {$max: "$timestamp"}}}
    ]).toArray()[0];
    print("hourly candles : " + total);
    print("tasks done     : " + done + " (symbols×contracts)");
    print("symbols done   : " + symbols_done);
    print("date range     : " + a.min.toISOString().slice(0,10) + "  ->  " + a.max.toISOString().slice(0,10));
    print("distinct syms  : " + db.hourly_futures.distinct("symbol").length);
    var ct = db.hourly_futures.aggregate([{$group:{_id:"$contract_type",n:{$sum:1}}}]).toArray();
    ct.forEach(c => print("  " + c._id + ": " + c.n + " candles"));
  } else {
    print("No documents yet. Is the backfill running?  (make logs)");
  }
'
