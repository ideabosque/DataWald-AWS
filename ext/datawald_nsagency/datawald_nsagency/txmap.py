TXMAP = {
    "customer": {
        "balance": {
            "funct": "str(src['balance'])",
            "src": [
                {
                    "key": "balance",
                    "label": "balance"
                }
            ],
            "type": "attribute"
        },
        "createdDate": {
            "funct": "src['createdDate'].strftime('%Y-%m-%d %H:%M:%S')",
            "src": [
                {
                    "key": "dateCreated",
                    "label": "createdDate"
                }
            ],
            "type": "attribute"
        },
        "creditLimit": {
            "funct": "str(src['creditLimit']) if src['creditLimit'] is not None else '0' ",
            "src": [
                {
                    "key": "creditLimit",
                    "label": "creditLimit"
                }
            ],
            "type": "attribute"
        },
        "email": {
            "funct": "str(src['email'])",
            "src": [
                {
                    "key": "email",
                    "label": "email"
                }
            ],
            "type": "attribute"
        },
        "externalId": {
            "funct": "str(src['externalId'])",
            "src": [
                {
                    "key": "externalId",
                    "label": "externalId"
                }
            ],
            "type": "attribute"
        },
        "lastModifiedDate": {
            "funct": "src['lastModifiedDate'].strftime('%Y-%m-%d %H:%M:%S')",
            "src": [
                {
                    "key": "lastModifiedDate",
                    "label": "lastModifiedDate"
                }
            ],
            "type": "attribute"
        },
        "salesRep": {
            "funct": "str(src['salesRep'])",
            "src": [
                    {
                        "key": "salesRep|email",
                        "label": "salesRep"
                    }
            ],
            "type": "attribute"
        },
        "terms": {
            "funct": "str(src['terms']) if src['terms'] is not None else '####' ",
            "src": [
                {
                    "key": "terms",
                    "label": "terms"
                }
            ],
            "type": "attribute"
        }
    },
    "inventory": {
        "createdDate": {
            "funct": "src['createdDate'].strftime('%Y-%m-%d %H:%M:%S')",
            "src": [
                {
                    "key": "createdDate",
                    "label": "createdDate"
                }
            ],
            "type": "attribute"
        },
        "lastModifiedDate": {
            "funct": "src['lastModifiedDate'].strftime('%Y-%m-%d %H:%M:%S')",
            "src": [
                {
                    "key": "lastModifiedDate",
                    "label": "lastModifiedDate"
                }
            ],
            "type": "attribute"
        },
        "locations": {
            "funct": {
                "full": {
                    "funct": "src['full']",
                    "src": [
                        {
                            "default": True,
                            "label": "full"
                        }
                    ],
                    "type": "attribute"
                },
                "in_stock": {
                    "funct": "True if src['qty'] > 0 else False",
                    "src": [
                            {
                                "key": "quantityAvailable",
                                "label": "qty"
                            }
                    ],
                    "type": "attribute"
                },
                "on_hand": {
                    "funct": "Decimal(src['on_hand'])",
                    "src": [
                        {
                            "key": "quantityAvailable",
                            "label": "on_hand"
                        }
                    ],
                    "type": "attribute"
                },
                "past_on_hand": {
                    "funct": "Decimal(src['past_on_hand'])",
                    "src": [
                        {
                            "default": 0,
                            "label": "past_on_hand"
                        }
                    ],
                    "type": "attribute"
                },
                "qty": {
                    "funct": "Decimal(src['qty'])",
                    "src": [
                        {
                            "key": "quantityAvailable",
                            "label": "qty"
                        }
                    ],
                    "type": "attribute"
                },
                "store_id": {
                    "funct": "src['store_id']",
                    "src": [
                            {
                                "default": 0,
                                "label": "store_id"
                            }
                    ],
                    "type": "attribute"
                },
                "warehouse": {
                    "funct": "src['warehouse']",
                    "src": [
                        {
                            "key": "location",
                            "label": "warehouse"
                        }
                    ],
                    "type": "attribute"
                }
            },
            "src": [
                {
                    "key": "locationsList|locations"
                }
            ],
            "type": "list"
        },
        "sku": {
            "funct": "src['sku']",
            "src": [
                {
                    "key": "itemId",
                    "label": "sku"
                }
            ],
            "type": "attribute"
        }
    },
    "invoice": {
        "addresses": {
            "funct": {
                "billto": {
                    "funct": {
                        "address": {
                            "funct": "src['address']",
                            "src": [
                                {
                                    "key": "billingAddress|addr1",
                                    "label": "address"
                                }
                            ],
                            "type": "attribute"
                        },
                        "city": {
                            "funct": "src['city']",
                            "src": [
                                {
                                    "key": "billingAddress|city",
                                    "label": "city"
                                }
                            ],
                            "type": "attribute"
                        },
                        "company": {
                            "funct": "src['company']",
                            "src": [
                                {
                                    "key": "billingAddress|addressee",
                                    "label": "company"
                                }
                            ],
                            "type": "attribute"
                        },
                        "contact": {
                            "funct": "src['contact']",
                            "src": [
                                {
                                    "key": "billingAddress|attention",
                                    "label": "contact"
                                }
                            ],
                            "type": "attribute"
                        },
                        "country": {
                            "funct": "src['country']",
                            "src": [
                                {
                                    "key": "billingAddress|country",
                                    "label": "country"
                                }
                            ],
                            "type": "attribute"
                        },
                        "postcode": {
                            "funct": "src['postcode']",
                            "src": [
                                    {
                                        "key": "billingAddress|zip",
                                        "label": "postcode"
                                    }
                            ],
                            "type": "attribute"
                        },
                        "region": {
                            "funct": "src['region']",
                            "src": [
                                {
                                    "key": "billingAddress|state",
                                    "label": "region"
                                }
                            ],
                            "type": "attribute"
                        },
                        "telephone": {
                            "funct": "src['telephone']",
                            "src": [
                                {
                                    "key": "billingAddress|addrPhone",
                                    "label": "telephone"
                                }
                            ],
                            "type": "attribute"
                        }
                    },
                    "src": [
                        {
                            "key": "####"
                        }
                    ],
                    "type": "dict"
                },
                "shipto": {
                    "funct": {
                        "address": {
                            "funct": "src['address']",
                            "src": [
                                {
                                    "key": "shippingAddress|addr1",
                                    "label": "address"
                                }
                            ],
                            "type": "attribute"
                        },
                        "city": {
                            "funct": "src['city']",
                            "src": [
                                {
                                    "key": "shippingAddress|city",
                                    "label": "city"
                                }
                            ],
                            "type": "attribute"
                        },
                        "company": {
                            "funct": "src['company']",
                            "src": [
                                {
                                    "key": "shippingAddress|addressee",
                                    "label": "company"
                                }
                            ],
                            "type": "attribute"
                        },
                        "contact": {
                            "funct": "src['contact']",
                            "src": [
                                {
                                    "key": "shippingAddress|attention",
                                    "label": "contact"
                                }
                            ],
                            "type": "attribute"
                        },
                        "country": {
                            "funct": "src['country']",
                            "src": [
                                {
                                    "key": "shippingAddress|country",
                                    "label": "country"
                                }
                            ],
                            "type": "attribute"
                        },
                        "postcode": {
                            "funct": "src['postcode']",
                            "src": [
                                    {
                                        "key": "shippingAddress|zip",
                                        "label": "postcode"
                                    }
                            ],
                            "type": "attribute"
                        },
                        "region": {
                            "funct": "src['region']",
                            "src": [
                                {
                                    "key": "shippingAddress|state",
                                    "label": "region"
                                }
                            ],
                            "type": "attribute"
                        },
                        "telephone": {
                            "funct": "src['telephone']",
                            "src": [
                                {
                                    "key": "shippingAddress|addrPhone",
                                    "label": "telephone"
                                }
                            ],
                            "type": "attribute"
                        }
                    },
                    "src": [
                        {
                            "key": "####"
                        }
                    ],
                    "type": "dict"
                }
            },
            "src": [
                {
                    "key": "####"
                }
            ],
            "type": "dict"
        },
        "createdDate": {
            "funct": "src['createdDate'].strftime('%Y-%m-%d %H:%M:%S')",
            "src": [
                {
                    "key": "createdDate",
                    "label": "createdDate"
                }
            ],
            "type": "attribute"
        },
        "items": {
            "funct": {
                "amount": {
                    "funct": "str(src['amount'])",
                    "src": [
                        {
                            "key": "amount",
                            "label": "amount"
                        }
                    ],
                    "type": "attribute"
                },
                "price": {
                    "funct": "str(src['price'])",
                    "src": [
                        {
                            "key": "rate",
                            "label": "price"
                        }
                    ],
                    "type": "attribute"
                },
                "qty": {
                    "funct": "str(src['qty'])",
                    "src": [
                        {
                            "key": "quantity",
                            "label": "qty"
                        }
                    ],
                    "type": "attribute"
                },
                "qtyOrdered": {
                    "funct": "str(src['qtyOrdered'])",
                    "src": [
                        {
                            "key": "quantityOrdered",
                            "label": "qtyOrdered"
                        }
                    ],
                    "type": "attribute"
                },
                "sku": {
                    "funct": "src['sku']",
                    "src": [
                        {
                            "key": "item|name",
                            "label": "sku"
                        }
                    ],
                    "type": "attribute"
                },
                "upc": {
                    "funct": "str(src['upc'])",
                    "src": [
                        {
                            "key": "@custcol8",
                            "label": "upc"
                        }
                    ],
                    "type": "attribute"
                }
            },
            "src": [
                {
                    "key": "itemList|item"
                }
            ],
            "type": "list"
        },
        "lastModifiedDate": {
            "funct": "src['lastModifiedDate'].strftime('%Y-%m-%d %H:%M:%S')",
            "src": [
                {
                    "key": "lastModifiedDate",
                    "label": "lastModifiedDate"
                }
            ],
            "type": "attribute"
        },
        "shippingCost": {
            "funct": "str(src['shippingCost'])",
            "src": [
                {
                    "key": "shippingCost",
                    "label": "shippingCost"
                }
            ],
            "type": "attribute"
        },
        "subTotal": {
            "funct": "str(src['subTotal'])",
            "src": [
                    {
                        "key": "subTotal",
                        "label": "subTotal"
                    }
            ],
            "type": "attribute"
        },
        "total": {
            "funct": "str(src['total'])",
            "src": [
                {
                    "key": "total",
                    "label": "total"
                }
            ],
            "type": "attribute"
        },
        "trackingNumbers": {
            "funct": "src['linkedTrackingNumbers'] if src['linkedTrackingNumbers'] is not None else src['trackingNumbers']",
            "src": [
                {
                    "key": "trackingNumbers",
                    "label": "trackingNumbers"
                },
                {
                    "key": "linkedTrackingNumbers",
                    "label": "linkedTrackingNumbers"
                }
            ],
            "type": "attribute"
        }
    },
    "itemReceipt": {
        "id": {
            "funct": "src['id']",
            "src": [
                {
                    "key": "id",
                    "label": "id"
                }
            ],
            "type": "attribute"
        },
        "internalId": {
            "funct": "src['InternalId']",
            "src": [
                {
                    "key": "data|InternalId",
                    "label": "InternalId"
                }
            ],
            "type": "attribute"
        },
        "items": {
            "funct": {
                "qty": {
                    "funct": "src['qty']",
                    "src": [
                        {
                            "key": "Qty",
                            "label": "qty"
                        }
                    ],
                    "type": "attribute"
                },
                "sku": {
                    "funct": "src['item_no']",
                    "src": [
                        {
                            "key": "Item_No",
                            "label": "item_no"
                        }
                    ],
                    "type": "attribute"
                }
            },
            "src": [
                {
                    "key": "data|Items"
                }
            ],
            "type": "list"
        },
        "ref": {
            "funct": "src['ref']",
            "src": [
                {
                    "key": "data|RefNo",
                    "label": "ref"
                }
            ],
            "type": "attribute"
        }
    },
    "salesOrder": {
        "billingAddress": {
            "funct": {
                "addr1": {
                    "funct": "src['addr1']",
                    "src": [
                        {
                            "key": "address",
                            "label": "addr1"
                        }
                    ],
                    "type": "attribute"
                },
                "addrPhone": {
                    "funct": "src['addrPhone'] if src['addrPhone'] not in ['.', 'n/a'] and src['addrPhone'] is not None else '' ",
                    "src": [
                        {
                            "key": "telephone",
                            "label": "addrPhone"
                        }
                    ],
                    "type": "attribute"
                },
                "attention": {
                    "funct": "src['attention']",
                    "src": [
                        {
                            "key": "company",
                            "label": "attention"
                        }
                    ],
                    "type": "attribute"
                },
                "city": {
                    "funct": "src['city']",
                    "src": [
                        {
                            "key": "city",
                            "label": "city"
                        }
                    ],
                    "type": "attribute"
                },
                "country": {
                    "funct": "src['country']",
                    "src": [
                        {
                            "key": "country",
                            "label": "country"
                        }
                    ],
                    "type": "attribute"
                },
                "firstName": {
                    "funct": "src['firstName']",
                    "src": [
                        {
                            "key": "firstname",
                            "label": "firstName"
                        }
                    ],
                    "type": "attribute"
                },
                "lastName": {
                    "funct": "src['lastName']",
                    "src": [
                        {
                            "key": "lastname",
                            "label": "lastName"
                        }
                    ],
                    "type": "attribute"
                },
                "state": {
                    "funct": "src['state']",
                    "src": [
                        {
                            "key": "region",
                            "label": "state"
                        }
                    ],
                    "type": "attribute"
                },
                "zip": {
                    "funct": "src['zip']",
                    "src": [
                        {
                            "key": "postcode",
                            "label": "zip"
                        }
                    ],
                    "type": "attribute"
                }
            },
            "src": [
                {
                    "key": "addresses|billto"
                }
            ],
            "type": "dict"
        },
        "bo_customer_id": {
            "funct": "src['bo_customer_id']",
            "src": [
                {
                    "key": "bo_customer_id",
                    "label": "bo_customer_id"
                }
            ],
            "type": "attribute"
        },
        "customFields": {
            "funct": {
                "custbody2": {
                    "funct": "src['custbody2']",
                    "src": [
                        {
                            "default": "No",
                            "key": "custbody2",
                            "label": "custbody2"
                        }
                    ],
                    "type": "attribute"
                },
                "custbody_customergroup": {
                    "funct": "src['custbody_customergroup']",
                    "src": [
                        {
                            "default": "Wholesale T4",
                            "key": "customer_group",
                            "label": "custbody_customergroup"
                        }
                    ],
                    "type": "attribute"
                }
            },
            "src": [
                {
                    "key": "####"
                }
            ],
            "type": "dict"
        },
        "email": {
            "funct": "src['email']",
            "src": [
                {
                    "key": "addresses|billto|email",
                    "label": "email"
                }
            ],
            "type": "attribute"
        },
        "fe_customer_id": {
            "funct": "src['fe_customer_id']",
            "src": [
                {
                    "key": "fe_customer_id",
                    "label": "fe_customer_id"
                }
            ],
            "type": "attribute"
        },
        "fe_order_id": {
            "funct": "src['fe_order_id']",
            "src": [
                {
                    "key": "fe_order_id",
                    "label": "fe_order_id"
                }
            ],
            "type": "attribute"
        },
        "firstName": {
            "funct": "src['firstName']",
            "src": [
                {
                    "key": "addresses|billto|firstname",
                    "label": "firstName"
                }
            ],
            "type": "attribute"
        },
        "id": {
            "funct": "src['id']",
            "src": [
                {
                    "key": "id",
                    "label": "id"
                }
            ],
            "type": "attribute"
        },
        "items": {
            "funct": {
                "price": {
                    "funct": "str(src['price'])",
                    "src": [
                        {
                            "key": "price",
                            "label": "price"
                        }
                    ],
                    "type": "attribute"
                },
                "qty": {
                    "funct": "str(src['qty'])",
                    "src": [
                        {
                            "key": "qty",
                            "label": "qty"
                        }
                    ],
                    "type": "attribute"
                },
                "sku": {
                    "funct": "src['sku']",
                    "src": [
                        {
                            "key": "sku",
                            "label": "sku"
                        }
                    ],
                    "type": "attribute"
                }
            },
            "src": [
                {
                    "key": "items"
                }
            ],
            "type": "list"
        },
        "lastName": {
            "funct": "src['lastName']",
            "src": [
                {
                    "key": "addresses|billto|lastname",
                    "label": "lastName"
                }
            ],
            "type": "attribute"
        },
        "memo": {
            "funct": "src['memo']",
            "src": [
                {
                    "default": "####",
                    "key": "purchase_order",
                    "label": "memo"
                }
            ],
            "type": "attribute"
        },
        "otherRefNum": {
            "funct": "src['otherRefNum']",
            "src": [
                {
                    "key": "fe_order_id",
                    "label": "otherRefNum"
                }
            ],
            "type": "attribute"
        },
        "paymentMethod": {
            "funct": "src['paymentMethod']",
            "src": [
                {
                    "default": "####",
                    "key": "payment_method",
                    "label": "paymentMethod"
                }
            ],
            "type": "attribute"
        },
        "priceLevel": {
            "funct": "src['price_level']",
            "src": [
                {
                    "default": "Wholesale T4",
                    "key": "customer_group",
                    "label": "price_level"
                }
            ],
            "type": "attribute"
        },
        "salesRep": {
            "funct": "src['salesRep']",
            "src": [
                {
                    "key": "representative",
                    "label": "salesRep"
                }
            ],
            "type": "attribute"
        },
        "shipMethod": {
            "funct": "src['shipMethod']",
            "src": [
                {
                    "default": "####",
                    "key": "ship_method",
                    "label": "shipMethod"
                }
            ],
            "type": "attribute"
        },
        "shippingAddress": {
            "funct": {
                "addr1": {
                    "funct": "src['addr1']",
                    "src": [
                        {
                            "key": "address",
                            "label": "addr1"
                        }
                    ],
                    "type": "attribute"
                },
                "addr2": {
                    "funct": "src['addr2']",
                    "src": [
                        {
                            "key": "telephone",
                            "label": "addr2"
                        }
                    ],
                    "type": "attribute"
                },
                "addrPhone": {
                    "funct": "src['addrPhone'] if src['addrPhone'] not in ['.', 'n/a'] and src['addrPhone'] is not None else '' ",
                    "src": [
                        {
                            "key": "telephone",
                            "label": "addrPhone"
                        }
                    ],
                    "type": "attribute"
                },
                "attention": {
                    "funct": "src['attention']",
                    "src": [
                        {
                            "key": "company",
                            "label": "attention"
                        }
                    ],
                    "type": "attribute"
                },
                "city": {
                    "funct": "src['city']",
                    "src": [
                        {
                            "key": "city",
                            "label": "city"
                        }
                    ],
                    "type": "attribute"
                },
                "country": {
                    "funct": "src['country']",
                    "src": [
                        {
                            "key": "country",
                            "label": "country"
                        }
                    ],
                    "type": "attribute"
                },
                "firstName": {
                    "funct": "src['firstName']",
                    "src": [
                        {
                            "key": "firstname",
                            "label": "firstName"
                        }
                    ],
                    "type": "attribute"
                },
                "lastName": {
                    "funct": "src['lastName']",
                    "src": [
                        {
                            "key": "lastname",
                            "label": "lastName"
                        }
                    ],
                    "type": "attribute"
                },
                "state": {
                    "funct": "src['state']",
                    "src": [
                        {
                            "key": "region",
                            "label": "state"
                        }
                    ],
                    "type": "attribute"
                },
                "zip": {
                    "funct": "src['zip']",
                    "src": [
                        {
                            "key": "postcode",
                            "label": "zip"
                        }
                    ],
                    "type": "attribute"
                }
            },
            "src": [
                {
                    "key": "addresses|shipto"
                }
            ],
            "type": "dict"
        },
        "shippingCost": {
            "funct": "src['shippingCost']",
            "src": [
                {
                    "key": "shipping_total",
                    "label": "shippingCost"
                }
            ],
            "type": "attribute"
        },
        "source": {
            "funct": "src['source']",
            "src": [
                {
                    "default": "Web Services",
                    "key": "source",
                    "label": "source"
                }
            ],
            "type": "attribute"
        }
    }
}
