# Restream

Replay Firehose Streams in Kinesis Streams!

## Installation

You can use `pip` to install Restream.

```bash
pip install git+https://github.com/bufferapp/restream
```

If you prefer, you can clone it and run the setup.py file. Use the following commands to install Restream from Github:

```bash
git clone https://github.com/bufferapp/restream
cd restream
python setup.py install
```

## Requirements

The script assumes you have data in S3 that follow the Firehose pattern (`bucket/key/<year>/<month>/<day>/<hour>`). Before running the script make sure you have a Kinesis Stream ready to consume a lot of data. Change the number of shards to fit the script throughput before restreaming large datasets!

## Usage

The script requires the name of the bucket and the path (`key`) of the root Firehose folder as well as a delimited timeframe.

```bash
restream [bucket] [key] [start-date] [end-date]
```

For example, if the Firehose stream saves the records into `s3://my-bucket/my-data/` and we want to restream the data from `2017-06-12 20:00:00` to `2017-06-12 22:00:00` we can run the following:

```bash
restream my-bucket my-data my-stream-name -sd '2017-06-12 20' -ed '2017 Jun 12 22:00' -y
```

Date input is flexible and has a resolution up to the hour.

## Contributing

We appreciate all kind of contributions! Please open an issue to start a discussion around a feature idea or a bug. :smile:
