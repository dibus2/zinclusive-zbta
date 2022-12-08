__API_SCHEMA__ = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["request"],
    "properties": {
        "request": {
            "type": "object",
            "required": ["meta", "bt_data"],
            "properties": {
                "meta": {
                    "type": "object",
                    "required": ["application_info", "application_details", "actions", "data_provider"],
                    "properties": {
                        "application_info": {
                            "type": "object",
                            "required": [],
                            "properties": {
                                "name": {
                                    "description": "the name of the applicant",
                                    "type": "string"
                                },
                                "zip": {
                                    "description": "the zip (5) or +4 of the applicants",
                                    "type": "string"
                                },
                                "phone": {
                                    "description": "the phone number of the applicant with or without the indicator",
                                    "type": "string",
                                    "minLength": 10,
                                    "maxLength": 12,
                                },
                                "email": {
                                    "description": "the email address of the applicant",
                                    "type": "string"
                                },
                                "street": {
                                    "description": "the street and number of the applicant's address",
                                    "type": "string"
                                },
                                "city": {
                                    "description": "the city of the applicant",
                                    "type": "string"
                                },
                                "state": {
                                    "description": "this is the 2 digit state code",
                                    "minLength": 2,
                                    "maxLength": 2
                                }
                            }
                        },
                        "application_details": {
                            "type": "object",
                            "required": ["transaction_id"],
                            "properties": {
                                "transaction_id": {
                                    "description": "the transaction id, used for log tracking and so on.",
                                    "type": "string"
                                },
                                "score_tags": {
                                    "description": "the list of score tags to score.",
                                    "type": "array",
                                    "items": {
                                        "type": "string"
                                    },
                                    "minItems": 0,
                                    "uniqueItems": True
                                }
                            }
                        },
                        "actions": {
                            "type": "array",
                            "uniqueItems": True,
                            "items": {
                                "type": "string",
                                "enum": ["triggers", "attributes", "scores", "all"]
                            },
                        },
                        "data_provider": {
                            "type": "string",
                            "pattern": "fiserv_alldata"
                        }
                    }
                },
                "bt_data": {
                    "type": "object",
                    "required": ["data"],
                    "properties": {
                        "data": {
                            "type": "array"
                        }
                    }
                }
            }
        }
    }
}
