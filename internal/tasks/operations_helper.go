package tasks

import (
	"errors"
	"strings"
)

func getAvroSchema(templateType string) (string, error) {
	switch strings.ToLower(templateType) {
	case "hotel":
		hotelAvro := `{
			"name": "Hotel",	
			"type": "record",
			"fields": [
				{"name": "id", "type": "string"},
				{"name": "country", "type": "string", "default": ""},
				{"name": "address", "type": "string", "default": ""},
				{"name": "free_parking", "type": "boolean", "default": false},
				{"name": "city", "type": "string", "default": ""},
				{"name": "template_type", "type": "string"},
				{"name": "url", "type": "string", "default": ""},
				{
					"name": "reviews",
					"type": {
						"type": "array",
						"items": {
							"name": "Review",
							"type": "record",
							"fields": [
								{"name": "date", "type": "string", "default": ""},
								{"name": "author", "type": "string", "default": ""},
								{
									"name": "rating",
									"type": "record",
									"fields": [
										{"name": "rating_value", "type": "double", "default": 0.0},
										{"name": "cleanliness", "type": "double", "default": 0.0},
										{"name": "overall", "type": "double", "default": 0.0},
										{"name": "checkin", "type": "double", "default": 0.0},
										{"name": "rooms", "type": "double", "default": 0.0}
									]
								}
							]
						},
						"default": []
					}
				},
				{"name": "phone", "type": "string", "default": ""},
				{"name": "price", "type": "double", "default": 0.0},
				{"name": "avg_rating", "type": "double", "default": 0.0},
				{"name": "free_breakfast", "type": "boolean", "default": false},
				{"name": "name", "type": "string", "default": ""},
				{"name": "public_likes", "type": {"type": "array", "items": "string"}, "default": []},
				{"name": "email", "type": "string", "default": ""},
				{"name": "mutated", "type": "double", "default": 0.0},
				{"name": "padding", "type": "string", "default": ""}
			]
		}`
		return hotelAvro, nil
	default:
		return "", errors.New("invalid template type")
	}
}
