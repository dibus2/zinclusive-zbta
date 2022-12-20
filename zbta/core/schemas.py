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

__ACCOUNT_SCHEMA__ = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["accountinfo", "banktrans"],
    "properties": {
        "account_info": {
            "type": "object",
            "required": ["AcctBal", "FIAcctInfo"],
            "properties": {
                "AcctBal": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["CurrAmt"],
                        "properties": {
                            "CurrAmt": {
                                "type": "object",
                                "required": ["Amt", "CurCode"],
                                "properties": {
                                    "Amt": {
                                        "type": "number",
                                        "description": "the current balance amt",
                                    },
                                    "CurCode": {
                                        "type": "string",
                                        "enum": ["USD"]
                                    }
                                }
                            }
                        }
                    }
                },
                "FIAcctInfo": {
                    "type": "object",
                    "required": ["FIAcctId"],
                    "properties": {
                        "FIAcctId": {
                            "type": "object",
                            "required": ["AcctType", "AcctId"],
                            "properties": {
                                "AcctType": {
                                    "type": "string",
                                    "enum": ["DDA", "CCA"],
                                    "description": "the account type."
                                },
                                "AcctId": {
                                    "type": "string",
                                    "description": "this is the account number."
                                }
                            }
                        }
                    }
                }
            
            }
        },
        "banktrans": {
            "type": "object",
            "required": ["result"],
            "properties": {
                "result": {
                    "type": "object",
                    "required": ["DepAcctTrnInqRs"],
                    "properties": {
                        "DepAcctTrnInqRs": {
                            "required": ["DepAcctTrns"],
                            "type": "object",
                            "properties": {
                                "DepAcctTrns": {
                                    "type": "object",
                                    "required": ["BankAcctTrnRec", "SelectionCriterion"],
                                    "properties": {
                                        "BankAcctTrnRec": {
                                            "type": "array",
                                            "minItems": 1,
                                            "items": {
                                                "type": "object",
                                                "required": ["TrnID", "TrnType", "PostedDt", "Memo", "Category", "CurAmt"],
                                                "properties": {
                                                    "TrnID": {
                                                        "type": "number",
                                                        "description": "the transaction id."
                                                    },
                                                    "TrnType": {
                                                        "type": "string",
                                                        "description": "the transaction type",
                                                        "enum": ["Credit", "Debit"]
                                                    },
                                                    "PostedDt": {
                                                        "description": "the date the transaction was posted.",
                                                        "type": "string"
                                                    },
                                                    "Memo": {
                                                        "description": "the transaction description.",
                                                        "type": "string"
                                                    },
                                                    "Category": {
                                                        "type": "string",
                                                        "description": "the category as provided by fiserv."
                                                    },
                                                    "CurAmt": {
                                                        "type": "object",
                                                        "required": ["Amt", "CurCode"],
                                                        "properties": {
                                                            "Amt": {
                                                                "type": "number",
                                                                "description": "the transaction amount"
                                                            },
                                                            "CurCode": {
                                                                "type": "string",
                                                                "enum": ["USD"],
                                                                "description": "the currency of the transaction."
                                                            }
                                                        }
                                                    }
                                                    
                                                }
                                            }
                                        },
                                        "SelectionCriterion": {
                                            "type": "object",
                                            "required": ["SelRangeDt"],
                                            "properties": {
                                                "SelRangeDt": {
                                                    "type": "object",
                                                    "required": ["StartDt", "EndDt"],
                                                    "properties": {
                                                        "StartDt": {
                                                            "description": "the oldest balance",
                                                            "type": "string"
                                                        },
                                                        "EndDt": {
                                                            "description": "the most recent balance",
                                                            "type": "string"
                                                        }
                                                        
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
