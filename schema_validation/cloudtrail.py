from pyspark.sql.types import *
from pydantic import BaseModel, validator
from typing import Optional, List, Dict

cloudtrail_schema = StructType([
    StructField("Records", ArrayType(
        StructType([
            StructField("additionalEventData", MapType(StringType(), StringType())),
            StructField("apiVersion", StringType()),
            StructField("awsRegion", StringType()),
            StructField("errorCode", StringType()),
            StructField("errorMessage", StringType()),
            StructField("eventID", StringType()),
            StructField("eventName", StringType(), nullable=False),
            StructField("eventSource", StringType(), nullable=False),
            StructField("eventTime", TimestampType(), nullable=False),
            StructField("eventType", StringType()),
            StructField("eventVersion", StringType(), nullable=False),
            StructField("readOnly", BooleanType()),
            StructField("recipientAccountId", StringType()),
            StructField("requestID", StringType()),
            StructField("requestParameters", MapType(StringType(), StringType())),
            StructField("resources", ArrayType(
                StructType([
                    StructField("ARN", StringType()),
                    StructField("accountId", StringType()),
                    StructField("type", StringType())
                ])
            )),
            StructField("responseElements", MapType(StringType(), StringType())),
            StructField("sharedEventID", StringType()),
            StructField("sourceIPAddress", StringType()),
            StructField("serviceEventDetails", MapType(StringType(), StringType())),
            StructField("userAgent", StringType()),
            StructField("userIdentity", StructType([
                StructField("accessKeyId", StringType()),
                StructField("accountId", StringType()),
                StructField("arn", StringType()),
                StructField("invokedBy", StringType()),
                StructField("principalId", StringType()),
                StructField("sessionContext", StructType([
                    StructField("attributes", StructType([
                        StructField("creationDate", StringType()),
                        StructField("mfaAuthenticated", StringType())
                    ])),
                    StructField("sessionIssuer", StructType([
                        StructField("accountId", StringType()),
                        StructField("arn", StringType()),
                        StructField("principalId", StringType()),
                        StructField("type", StringType()),
                        StructField("userName", StringType())
                    ]))
                ])),
                StructField("type", StringType()),
                StructField("userName", StringType()),
                StructField("webIdFederationData", StructType([
                    StructField("federatedProvider", StringType()),
                    StructField("attributes", MapType(StringType(), StringType()))
                ]))
            ])),
            StructField("vpcEndpointId", StringType())
        ])
    ), nullable=False)
])

# Pydantic Model for Post-Validation
class CloudTrailRecord(BaseModel):
    eventName: str
    eventSource: str
    eventTime: str
    eventVersion: str
    awsRegion: Optional[str]
    userIdentity: Optional[Dict]
    requestParameters: Optional[Dict]
    responseElements: Optional[Dict]
    resources: Optional[List[Dict]]
    
    @validator("eventTime")
    def validate_event_time(cls, v):
        if not v.endswith('Z'):
            raise ValueError("Invalid timestamp format")
        return v

class CloudTrailValidation:
    @staticmethod
    def validate_df(df):
        """Post-read validation checks"""
        if df.filter("eventName IS NULL OR eventSource IS NULL").count() > 0:
            raise ValueError("Missing required fields in some records")
            
        # Check timestamp format validity
        # invalid_timestamps = df.filter(
        #     "eventTime IS NOT NULL AND eventTime NOT LIKE '%Z'"
        # )
        # if invalid_timestamps.count() > 0:
        #     raise ValueError("Invalid timestamp format detected")
