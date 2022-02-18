# rtdl - The Real-Time Data Lake ⚡️
<img src="./public/logos/rtdl-logo.png" height="250px" width="250px"></img>  
[rtdl](https://rtdl.io) makes it easy to build and maintain a real-time data lake. You 
configure a data stream with a source - often from a tool like Kafka or Segment - and a 
cloud storage destination, and rtdl builds you a real-time data lake in Parquet format that 
automatically works with [Dremio](https://www.dremio.com/) to give you access your real-time 
data in common BI and ML tools – just like a data warehouse.  
  
You provide the streams, rtdl builds your data lake.  
  
See the rtdl [docs](https://rtdl.io/docs/), [blog](https://rtdl.io/blog/), and 
[website](https://rtdl.io) to keep up to date and for more details about how you can use 
rtdl.


## V0.0.2 - Current status -- what works and what doesn't

### What works? 🚀
rtdl is not full-featured yet, but it is currently functional. You can use the API on port 80 to 
configure streams that ingest json from an rtdl endpoint on port 8080, process them into Parquet, 
and save the files to a destination configured in your stream. rtdl can write files locally, to 
AWS S3, and to GCP Cloud Storage, and you can query your data with Dremio on port 9047 (login with 
Username: `rtdl` and Password `rtdl1234`).

### What's new? 💥
  * Switched from Apache Hive Metastore + Presto to Dremio. **Dremio works for all storage types.**
  * Added support for using a flattened JSON object as value for `gcp_json_credentials` field in the 
    `createStream` API call. Previously, you had to double-quote everything and flatten.
  * Added CONTRIBUTING.md and decided to use a DCO over a CLA - tl;dr use -s when you commit, like 
    `git commit -s -m "..."`


### What doesn't work/what's next on the roadmap? 🚴🏼
  * Add support for Azure Blob Storage
  * Add support for Segment Webhooks as a source
  * Add support for more compressions - currently default Snappy compression is supported


## Quickstart (for AWS) 🌱
### Initialize and start rtdl
For more detailed instructions see our [Initialize rtdl docs](https://rtdl.io/docs/setting-up/initializertdl).
1.  Run `docker compose -f docker-compose.init.yml up -d`.
    * **Note:** This configuration should be fault-tolerant, but if any containers or 
      processes fail when running this, run `docker compose -f docker-compose.init.yml down` 
      and retry.
2.  After the containers `rtdl_rtdl-db-init` and `rtdl_dremio-init` exit and complete with `EXITED (0)`, kill and 
    delete the rtdl container set by running `docker compose -f docker-compose.init.yml down`.
3.  Run `docker compose up -d` every time after.  
    **Note:** Your memory setting in Docker must be at least 8GB. rtdl may become unstable if it is 
    set lower.
    * `docker compose down` to stop.
**Note #1:** To start from scratch, run `rm -rf storage/` from the rtdl root folder.  
**Note #2:** If you experience file write issues preventing Dremio services from starting, 
please add 'user: root" to the `docker-compose.init.yml` and `docker-compose.yml` files 
in the Dremio service definition section. This issue has been encountered on Linux.

### Setup your storage buckets (in AWS) and stream in rtdl
For more detailed setup instructions for your cloud provider see our setup docs:
  * [Setup with AWS](https://rtdl.io/docs/setting-up/setupwithaws).
  * [Setup with GCP](https://rtdl.io/docs/setting-up/setupwithgcp).
  * [Setup with Azure](https://rtdl.io/docs/setting-up/setupwithazure).

1.  Create a new S3 bucket.
    * For more information, see [Amazon’s documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-bucket.html).
2.  Create a new IAM user.
    * For more information, see [Amazon’s documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html#id_users_create_console).
3.  Create a IAM new policy.
    * Use the below permissions, and attach the policy to the IAM 
      user created in step 2. Replace `<YOUR_BUCKET_NAME>` with the 
      name of the S3 bucket you created in step 1.
      ```
      {
          "Version": "2012-10-17",
          "Statement": [
              {
                  "Sid": "ListAllBuckets",
                  "Effect": "Allow",
                  "Action": [
                      "s3:GetBucketLocation",
                      "s3:ListAllMyBuckets"
                  ],
                  "Resource": [
                      "arn:aws:s3:::*"
                  ]
              },
              {
                  "Sid": "ListBucket",
                  "Effect": "Allow",
                  "Action": [
                      "s3:ListBucket"
                  ],
                  "Resource": [
                      "arn:aws:s3:::<YOUR_BUCKET_NAME>"
                  ]
              },
              {
                  "Sid": "ManageBucket",
                  "Effect": "Allow",
                  "Action": [
                      "s3:GetObject",
                      "s3:PutObject",
                      "s3:PutObjectAcl",
                      "s3:DeleteObject"
                  ],
                  "Resource": [
                      "arn:aws:s3:::<YOUR_BUCKET_NAME>/*"
                  ]
              }
          ]
      }
      ```
  4.  Create access keys for your IAM user. For more information, see 
      [Amazon's documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey).
        * Save the `Access Key ID` and `Secret Access Key` for use in 
          configuring your stream in rtdl.
  5.  Configure your stream in rtdl.  
      Send a call to the API at http://localhost:8080/ingest.
      * Example `createStream` call body for creating a data lake on AWS S3.  
        ```
        {
        "active": true,
        "message_type": "test-msg-aws",
        "file_store_type_id": 2,
        "region": "us-west-1",
        "bucket_name": "testBucketAWS",
        "folder_name": "testFolderAWS",
        "partition_time_id": 1,
        "compression_type_id": 1,
        "aws_access_key_id": "[aws_access_key_id]",
        "aws_secret_access_key": "[aws_secret_access_key]"
        }
        ```
      * Example `createStream` curl call for creating a data lake on AWS S3.  
        ```
        curl --location --request POST 'http://localhost:80/createStream' \
        --header 'Content-Type: application/json' \
        --data-raw '{
        "active": true,
        "message_type": "test-msg-aws",
        "file_store_type_id": 2,
        "region": "us-west-1",
        "bucket_name": "testBucketAWS",
        "folder_name": "testFolderAWS",
        "partition_time_id": 1,
        "compression_type_id": 1,
        "aws_access_key_id": "[aws_access_key_id]",
        "aws_secret_access_key": "[aws_secret_access_key]"
        }'
        ```
      **Note:** A Postman collection with examples of all rtdl API calls can be found on GitHub at [realtimedatalake/postman-rtdl-public](https://github.com/realtimedatalake/postman-rtdl-public).  

### Send data to rtdl
For more detailed instructions see our [Send data to rtdl docs](https://rtdl.io/docs/setting-up/senddata).  
All data should be sent to the `ingest` endpoint of the ingest service on port 8080 -- e.g. http://localhost:8080/ingest.
* You can send ***any*** json with just ```stream_id``` in the payload and rtdl will add it to your lake.
  ```
  {
      "stream_id":"837a8d07-cd06-4e17-bcd8-aef0b5e48d31",
      "name":"user1",
      "array":[1,2,3],
      "properties":{"age":20}
  }
  ```  
	You can optionally add ```message_type``` should you choose to override the ```message_type``` specified while creating the stream.
	rtdl will default to a message type ```rtdl_default``` if message type is absent in both stream definition and actual message.



## Architecture 🏛
rtdl has a multi-service architecture composed of tested and trusted open source tools – including Apache Flink Stateful Functions, 
Apache Kafka, and Dremio – to process and catalog your data and custom-built services to interact with them more easily.  

For more details about rtdl's architecture, see our [Architecture docs](https://rtdl.io/docs/architecture).


## License 🤝
[MIT](./LICENSE)


## Contributing 😎
Contributions are always welcome!  
See our [CONTRIBUTING](./CONTRIBUTING.md) for ways to get started. 
This project adheres to the rtdl [code of conduct](./CODE_OF_CONDUCT.md) - a 
direct adaptation of the [Contributor Covenant](https://www.contributor-covenant.org/), 
version [2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct.html).


## Appreciation 🙏
  * [Apache Flink](https://flink.apache.org/)
  * [Flink Stateful Functions](https://flink.apache.org/stateful-functions.html)
  * [Dremio](https://www.dremio.com/)
  * [Apache Kafka](https://kafka.apache.org/)
  * [sqlx](https://github.com/jmoiron/sqlx) by [jmoiron](https://github.com/jmoiron)
  * [parquet-go](https://github.com/xitongsys/parquet-go) by [xitongsys](https://github.com/xitongsys)
  * [kafka-go](https://github.com/segmentio/kafka-go) by [Segment](https://github.com/segmentio)


## Authors ✍🏽
  * [Dipanjan Biswas](https://www.github.com/dipanjanb)
  * [Gavin Johnson](https://www.github.com/thtmnisamnstr)
