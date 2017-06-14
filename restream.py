import click
import boto3
import sys
import zlib
from dateutil import parser
from dateutil import rrule
from kinesis_producer import KinesisProducer


def parse_date(raw_date):
    try:
        date = parser.parse(raw_date)
        return date
    except ValueError as e:
        click.echo('Error parsing {}! {}'.format(raw_date, e))
        sys.exit(-1)


def decode(input_data):
    try:
        decompressor = zlib.decompressobj(32 + zlib.MAX_WBITS)
        encoded_data = decompressor.decompress(input_data)
        data = encoded_data.decode('utf-8')
        return data
    except Exception:
        click.echo('Error decoding {}..'.format(input_data[:10]))


@click.command()
@click.argument('bucket', type=str)
@click.argument('key', type=str)
@click.argument('stream', type=str)
@click.option('-sd', '--start', help='Start date. Up to hour precision')
@click.option('-ed', '--end', help='End date. Up to hour precision')
@click.option('-y', '--yes', default=True, is_flag=True,
              help='Skip confirmation')
@click.option('-d', '--delimiter', help='Delimiter of Firehose records in S3')
def restream(bucket, key, stream, start, end, yes, delimiter):
    """Replay saved Firehose Streams into Kinesis streams.

    The data in the KEY path inside the BUCKET will be sent to STREAM.
    """

    # Instanciate Kinesis Producer
    kinesis_config = dict(
        aws_region='us-east-1',
        buffer_size_limit=1,
        buffer_time_limit=1,
        kinesis_concurrency=1,
        kinesis_max_retries=10,
        record_delimiter=b'\n',
        stream_name=stream,
    )

    producer = KinesisProducer(config=kinesis_config)

    # Instanciate S3 client
    s3 = boto3.client('s3')

    # Parse dates
    start_date = parse_date(start)
    end_date = parse_date(end)

    # Allow to skip confirmation prompt
    if yes:
        click.confirm('Restream from {} to {}'.format(
            start_date,
            end_date
        ), abort=True)

    # Iterate each hourly by bucket and restream the records individually
    for dt in rrule.rrule(rrule.HOURLY, dtstart=start_date, until=end_date):
        # Generate folder prefix
        prefix = key + '/' + dt.strftime('%Y/%m/%d/%H')

        # Grab list of objects in the prefix folder
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = objects.get('Contents')

        # Skip folders that don't exists
        if not contents:
            click.echo('Warning! {} not found'.format(prefix))
            continue

        for s3_object in contents:
            object_response = s3.get_object(
                Bucket='buffer-data',
                Key=s3_object.get('Key')
            )

            object_body = object_response['Body']
            object_data = object_body.read()

            data = decode(object_data)

            if delimiter:
                records = data.split(delimiter)
            else:
                records = data.splitlines()

            # Send the individual records to Kinesis
            object_name = s3_object.get('Key').split('/')[-1]
            label = 'Sending {} records:'.format(object_name)
            with click.progressbar(records, label=label) as bar:
                for record in bar:
                    if not record:
                        click.echo('Empty record!')
                        continue
                    producer.send(record.encode())

        producer.close()
        producer.join()
