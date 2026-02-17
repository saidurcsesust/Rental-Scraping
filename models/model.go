package models

// Listing contains the minimum required Airbnb listing fields.
type Listing struct {
	Title    string            `json:"title"`
	Price    string            `json:"price"`
	Location string            `json:"location"`
	Rating   string            `json:"rating,omitempty"`
	URL      string            `json:"url"`
	Details  map[string]string `json:"details,omitempty"`
}
