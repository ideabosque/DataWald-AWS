TXMAP = {
    "order": {
        "addresses": {
            "funct": {
                "billto": {
                    "funct": {
                        "address": {
                            "funct": "src['address']",
                            "src": [
                                {
                                    "key": "billto_address",
                                    "label": "address"
                                }
                            ],
                            "type": "attribute"
                        },
                        "city": {
                            "funct": "src['city']",
                            "src": [
                                {
                                    "key": "billto_city",
                                    "label": "city"
                                }
                            ],
                            "type": "attribute"
                        },
                        "companyname": {
                            "funct": "src['companyname']",
                            "src": [
                                {
                                    "key": "billto_companyname",
                                    "label": "companyname"
                                }
                            ],
                            "type": "attribute"
                        },
                        "firstname": {
                            "funct": "src['firstname']",
                            "src": [
                                {
                                    "key": "billto_firstname",
                                    "label": "firstname"
                                }
                            ],
                            "type": "attribute"
                        },
                        "lastname": {
                            "funct": "src['lastname']",
                            "src": [
                                {
                                    "key": "billto_lastname",
                                    "label": "lastname"
                                }
                            ],
                            "type": "attribute"
                        },
                        "email": {
                            "funct": "src['email']",
                            "src": [
                                {
                                    "key": "billto_email",
                                    "label": "email"
                                }
                            ],
                            "type": "attribute"
                        },
                        "country": {
                            "funct": "src['country']",
                            "src": [
                                {
                                    "key": "billto_country",
                                    "label": "country"
                                }
                            ],
                            "type": "attribute"
                        },
                        "postcode": {
                            "funct": "src['postcode']",
                            "src": [
                                {
                                    "key": "billto_postcode",
                                    "label": "postcode"
                                }
                            ],
                            "type": "attribute"
                        },
                        "region": {
                            "funct": "src['region']",
                            "src": [
                                {
                                    "key": "billto_region",
                                    "label": "region"
                                }
                            ],
                            "type": "attribute"
                        },
                        "telephone": {
                            "funct": "src['telephone']",
                            "src": [
                                {
                                    "key": "billto_telephone",
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
                                    "key": "shipto_address",
                                    "label": "address"
                                }
                            ],
                            "type": "attribute"
                        },
                        "city": {
                            "funct": "src['city']",
                            "src": [
                                {
                                    "key": "shipto_city",
                                    "label": "city"
                                }
                            ],
                            "type": "attribute"
                        },
                        "companyname": {
                            "funct": "src['companyname']",
                            "src": [
                                {
                                    "key": "shipto_companyname",
                                    "label": "companyname"
                                }
                            ],
                            "type": "attribute"
                        },
                        "firstname": {
                            "funct": "src['firstname']",
                            "src": [
                                {
                                    "key": "shipto_firstname",
                                    "label": "firstname"
                                }
                            ],
                            "type": "attribute"
                        },
                        "lastname": {
                            "funct": "src['lastname']",
                            "src": [
                                {
                                    "key": "shipto_lastname",
                                    "label": "lastname"
                                }
                            ],
                            "type": "attribute"
                        },
                        "country": {
                            "funct": "src['country']",
                            "src": [
                                {
                                    "key": "shipto_country",
                                    "label": "country"
                                }
                            ],
                            "type": "attribute"
                        },
                        "postcode": {
                            "funct": "src['postcode']",
                            "src": [
                                {
                                    "key": "shipto_postcode",
                                    "label": "postcode"
                                }
                            ],
                            "type": "attribute"
                        },
                        "region": {
                            "funct": "src['region']",
                            "src": [
                                {
                                    "key": "shipto_region",
                                    "label": "region"
                                }
                            ],
                            "type": "attribute"
                        },
                        "telephone": {
                            "funct": "src['telephone']",
                            "src": [
                                {
                                    "key": "shipto_telephone",
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
        "fe_order_id": {
            "funct": "src['fe_order_id']",
            "src": [
                {
                    "key": "m_order_inc_id",
                    "label": "fe_order_id"
                }
            ],
            "type": "attribute"
        },
        "fe_order_date": {
            "funct": "src['fe_order_date'].strftime('%Y-%m-%d %H:%M:%S')",
            "src": [
                {
                    "key": "m_order_date",
                    "label": "fe_order_date"
                }
            ],
            "type": "attribute"
        },
        "fe_order_update_date": {
            "funct": "src['fe_order_update_date'].strftime('%Y-%m-%d %H:%M:%S')",
            "src": [
                {
                    "key": "m_order_update_date",
                    "label": "fe_order_update_date"
                }
            ],
            "type": "attribute"
        },
        "fe_order_status": {
            "funct": "src['fe_order_status']",
            "src": [
                {
                    "key": "m_order_status",
                    "label": "fe_order_status"
                }
            ],
            "type": "attribute"
        },
        "fe_store_id": {
            "funct": "src['fe_store_id']",
            "src": [
                {
                    "key": "m_store_id",
                    "label": "fe_store_id"
                }
            ],
            "type": "attribute"
        },
        "fe_customer_id": {
            "funct": "src['fe_customer_id']",
            "src": [
                {
                    "key": "m_customer_id",
                    "label": "fe_customer_id"
                }
            ],
            "type": "attribute"
        },
        "fe_customer_group": {
            "funct": "src['fe_customer_group']",
            "src": [
                {
                    "key": "m_customer_group",
                    "label": "fe_customer_group"
                }
            ],
            "type": "attribute"
        },
        "shipment_carrier": {
            "funct": "src['shipment_carrier']",
            "src": [
                {
                    "key": "shipment_carrier",
                    "label": "shipment_carrier"
                }
            ],
            "type": "attribute"
        },
        "shipment_method": {
            "funct": "src['shipment_method']",
            "src": [
                {
                    "key": "shipment_method",
                    "label": "shipment_method"
                }
            ],
            "type": "attribute"
        },
        "total_qty": {
            "funct": "src['total_qty']",
            "src": [
                {
                    "key": "total_qty",
                    "label": "total_qty"
                }
            ],
            "type": "attribute"
        },
        "sub_total": {
            "funct": "src['sub_total']",
            "src": [
                {
                    "key": "sub_total",
                    "label": "sub_total"
                }
            ],
            "type": "attribute"
        },
        "discount_amt": {
            "funct": "src['discount_amt']",
            "src": [
                {
                    "key": "discount_amt",
                    "label": "discount_amt"
                }
            ],
            "type": "attribute"
        },
        "shipping_amt": {
            "funct": "src['shipping_amt']",
            "src": [
                {
                    "key": "shipping_amt",
                    "label": "shipping_amt"
                }
            ],
            "type": "attribute"
        },
        "tax_amt": {
            "funct": "src['tax_amt']",
            "src": [
                {
                    "key": "tax_amt",
                    "label": "tax_amt"
                }
            ],
            "type": "attribute"
        },
        "giftcard_amt": {
            "funct": "src['giftcard_amt']",
            "src": [
                {
                    "key": "giftcard_amt",
                    "label": "giftcard_amt"
                }
            ],
            "type": "attribute"
        },
        "storecredit_amt": {
            "funct": "src['storecredit_amt']",
            "src": [
                {
                    "key": "storecredit_amt",
                    "label": "storecredit_amt"
                }
            ],
            "type": "attribute"
        },
        "grand_total": {
            "funct": "src['grand_total']",
            "src": [
                {
                    "key": "grand_total",
                    "label": "grand_total"
                }
            ],
            "type": "attribute"
        },
        "coupon_code": {
            "funct": "src['coupon_code']",
            "src": [
                {
                    "key": "coupon_code",
                    "label": "coupon_code"
                }
            ],
            "type": "attribute"
        },
        "shipping_tax_amt": {
            "funct": "src['shipping_tax_amt']",
            "src": [
                {
                    "key": "shipping_tax_amt",
                    "label": "shipping_tax_amt"
                }
            ],
            "type": "attribute"
        },
        "payment_method": {
            "funct": "src['payment_method']",
            "src": [
                {
                    "key": "payment_method",
                    "label": "payment_method"
                }
            ],
            "type": "attribute"
        },
        "items": {
            "funct": {
                "sub_total": {
                    "funct": "str(src['sub_total'])",
                    "src": [
                        {
                            "key": "sub_total",
                            "label": "sub_total"
                        }
                    ],
                    "type": "attribute"
                },
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
                "original_price": {
                    "funct": "str(src['original_price'])",
                    "src": [
                        {
                            "key": "original_price",
                            "label": "original_price"
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
                "name": {
                    "funct": "src['name']",
                    "src": [
                        {
                            "key": "name",
                            "label": "name"
                        }
                    ],
                    "type": "attribute"
                },
                "discount_amt": {
                    "funct": "str(src['discount_amt'])",
                    "src": [
                        {
                            "key": "discount_amt",
                            "label": "discount_amt"
                        }
                    ],
                    "type": "attribute"
                },
                "tax_amt": {
                    "funct": "str(src['tax_amt'])",
                    "src": [
                        {
                            "key": "tax_amt",
                            "label": "tax_amt"
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
        }
    }
}
