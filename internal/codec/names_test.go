package codec

import "testing"

// Mangled names end up inside registered schema text, so they are part of the
// Schema Registry compatibility surface; these cases pin the exact output.
func TestNameMangling(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantTitle string
		wantProto string
		wantAvro  string
	}{
		{
			name:      "single lowercase word",
			in:        "orders",
			wantTitle: "Orders",
			wantProto: "Orders",
			wantAvro:  "Orders",
		},
		{
			name:      "underscores separate words",
			in:        "order_status",
			wantTitle: "Order_Status",
			wantProto: "OrderStatus",
			wantAvro:  "OrderStatus",
		},
		{
			name:      "dashes separate words and only protobuf drops them",
			in:        "user-events",
			wantTitle: "User-Events",
			wantProto: "UserEvents",
			wantAvro:  "User-Events",
		},
		{
			name:      "mixed separators",
			in:        "user-order_id",
			wantTitle: "User-Order_Id",
			wantProto: "UserOrderId",
			wantAvro:  "User-OrderId",
		},
		{
			name:      "digits are word boundaries too",
			in:        "order2item",
			wantTitle: "Order2Item",
			wantProto: "Order2Item",
			wantAvro:  "Order2Item",
		},
		{
			name:      "existing capitals are lowered inside a word",
			in:        "ORDERS",
			wantTitle: "Orders",
			wantProto: "Orders",
			wantAvro:  "Orders",
		},
		{
			name:      "camelCase collapses to one word",
			in:        "orderId",
			wantTitle: "Orderid",
			wantProto: "Orderid",
			wantAvro:  "Orderid",
		},
		{
			name:      "leading separator",
			in:        "_private",
			wantTitle: "_Private",
			wantProto: "Private",
			wantAvro:  "Private",
		},
		{
			name:      "empty",
			in:        "",
			wantTitle: "",
			wantProto: "",
			wantAvro:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compatTitle(tt.in); got != tt.wantTitle {
				t.Errorf("compatTitle(%q) = %q, want %q", tt.in, got, tt.wantTitle)
			}
			if got := mangleProto(tt.in); got != tt.wantProto {
				t.Errorf("mangleProto(%q) = %q, want %q", tt.in, got, tt.wantProto)
			}
			if got := mangleAvro(tt.in); got != tt.wantAvro {
				t.Errorf("mangleAvro(%q) = %q, want %q", tt.in, got, tt.wantAvro)
			}
		})
	}
}

func TestRecordNameAndSubject(t *testing.T) {
	tests := []struct {
		topic       string
		wantRecord  string
		wantSubject string
	}{
		{"orders", "OrdersRecord", "orders-value"},
		{"user-events", "UserEventsRecord", "user-events-value"},
		{"payment_events", "PaymentEventsRecord", "payment_events-value"},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			if got := recordName(tt.topic); got != tt.wantRecord {
				t.Errorf("recordName(%q) = %q, want %q", tt.topic, got, tt.wantRecord)
			}
			if got := subjectFor(tt.topic); got != tt.wantSubject {
				t.Errorf("subjectFor(%q) = %q, want %q", tt.topic, got, tt.wantSubject)
			}
		})
	}
}
