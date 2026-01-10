package dto

import "encoding/json"

type RecordUser struct {
    Name *json.RawMessage `json:"name,omitempty"`
    Age int64 `json:"age"`
}

type Record struct {
    Id string `json:"id"`
    User RecordUser `json:"user"`
    Price *float64 `json:"price,omitempty"`
    Active bool `json:"active"`
    Meta *json.RawMessage `json:"meta,omitempty"`
    UserName *json.RawMessage `json:"user-name,omitempty"`
    Class *json.RawMessage `json:"class,omitempty"`
    Status string `json:"status"`
    Source string `json:"source"`
}
