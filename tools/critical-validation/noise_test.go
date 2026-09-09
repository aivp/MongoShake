package validation

import (
	"testing"

	"github.com/alibaba/MongoShake/v2/oplog"
	"go.mongodb.org/mongo-driver/bson"
)

// Deliberately narrow candidate. Unknown formats are kept, not discarded.
// Validation only: no production filter chain calls this function.
func pureCarrierNoise(log oplog.ParsedLog) bool {
	if log.Operation != "u" || log.Namespace != "ds_production.Carriers" ||
		log.TxnNumber != nil || len(log.LSID) != 0 || len(log.Query) != 1 || log.Query[0].Key != "_id" || log.Query[0].Value == nil {
		return false
	}
	seen := map[string]bool{}
	var set bson.D
	for _, field := range log.Object {
		if seen[field.Key] {
			return false
		}
		seen[field.Key] = true
		switch field.Key {
		case "$v":
			switch version := field.Value.(type) {
			case int32:
				if version != 1 {
					return false
				}
			case int64:
				if version != 1 {
					return false
				}
			default:
				return false
			}
		case "$set":
			var ok bool
			set, ok = field.Value.(bson.D)
			if !ok {
				return false
			}
		default:
			return false
		}
	}
	if len(set) == 0 {
		return false
	}
	seen = map[string]bool{}
	for _, field := range set {
		if seen[field.Key] {
			return false
		}
		seen[field.Key] = true
		switch field.Key {
		case "survivalTime", "canTimestamp", "updated_at", "created_at":
		default:
			return false
		}
	}
	return true
}

func TestCriticalNoiseFailOpen(t *testing.T) {
	noise := bson.D{{Key: "$set", Value: bson.D{{Key: "survivalTime", Value: int64(1)}}}}
	cases := []struct {
		name, ns, op string
		object       bson.D
		drop         bool
	}{
		{"pure_time", "Carriers", "u", noise, true},
		{"version_one", "Carriers", "u", append(bson.D{{Key: "$v", Value: int32(1)}}, noise...), true},
		{"organization_change", "Carriers", "u", bson.D{{Key: "$set", Value: bson.D{{Key: "enterpriseId", Value: "org-A"}, {Key: "survivalTime", Value: int64(1)}}}}, false},
		{"status_zero", "Carriers", "u", bson.D{{Key: "$set", Value: bson.D{{Key: "deviceStatus", Value: 0}}}}, false},
		{"insert", "Carriers", "i", noise, false},
		{"delete", "Carriers", "d", noise, false},
		{"replacement", "Carriers", "u", bson.D{{Key: "survivalTime", Value: int64(1)}}, false},
		{"unset", "Carriers", "u", bson.D{{Key: "$unset", Value: bson.D{{Key: "survivalTime", Value: true}}}}, false},
		{"upsert_shape", "Carriers", "u", append(append(bson.D{}, noise...), bson.E{Key: "$setOnInsert", Value: bson.D{}}), false},
		{"delta_v2", "Carriers", "u", bson.D{{Key: "$v", Value: int32(2)}, {Key: "diff", Value: bson.D{}}}, false},
		{"empty_set", "Carriers", "u", bson.D{{Key: "$set", Value: bson.D{}}}, false},
		{"dotted_field", "Carriers", "u", bson.D{{Key: "$set", Value: bson.D{{Key: "survivalTime.value", Value: int64(1)}}}}, false},
		{"unknown_modifier", "Carriers", "u", bson.D{{Key: "$future", Value: bson.D{}}}, false},
		{"binding", "CarrierDevice", "u", noise, false},
		{"driver_binding", "CarrierDriverMaps", "u", noise, false},
		{"drivers", "Drivers", "u", noise, false},
		{"organizations", "Organizations", "u", noise, false},
		{"status_collection", "CarrierStatusInfo", "u", noise, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := oplog.ParsedLog{Operation: tc.op, Namespace: "ds_production." + tc.ns, Object: tc.object, Query: bson.D{{Key: "_id", Value: "synthetic-vehicle"}}}
			if got := pureCarrierNoise(log); got != tc.drop {
				t.Fatalf("drop=%v want=%v", got, tc.drop)
			}
		})
	}
	t.Run("missing_identity", func(t *testing.T) {
		if pureCarrierNoise(oplog.ParsedLog{Operation: "u", Namespace: "ds_production.Carriers", Object: noise}) {
			t.Fatal("unidentified update dropped")
		}
	})
}
