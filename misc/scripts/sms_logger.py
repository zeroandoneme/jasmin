#!/usr/bin/env python
"""This script will log all sent sms through Jasmin with user information.

Requirement:
- Activate publish_submit_sm_resp in jasmin.cfg
- Install psycopg2:             # Used for PostgreSQL connection
    +   pip install psycopg2
- Install mysql.connector:      # Used for MySQL connection
    +   pip install mysql-connector-python

Optional:
- SET ENVIRONMENT ENV:
    + DB_TYPE_MYSQL     # Default: 1            # 1 for MySQL, 0 for PostgreSQL
    + DB_HOST           # Default: 127.0.0.1    # IP or Docker container name
    + DB_DATABASE       # Default: jasmin       # should Exist
    + DB_TABLE          # Default: submit_log   # the script will create it if it doesn't Exist
    + DB_USER           # Default: jasmin       # for the Database connection.
    + DB_PASS           # Default: jadmin       # for the Database connection
    + AMQP_BROKER_HOST  # Default: 127.0.0.1    # RabbitMQ host used by Jasmin SMS Gateway. IP or Docker container name
    + AMQP_BROKER_PORT  # Default: 5672         # RabbitMQ port used by Jasmin SMS Gateway. IP or Docker container name

Database Scheme:
- MySQL table:
    CREATE TABLE ${DB_TABLE}  (
        `msgid`            VARCHAR(45) PRIMARY KEY,
        `v_msgid`            VARCHAR(45),
        `source_connector` VARCHAR(15),
        `routed_cid`       VARCHAR(30),
        `source_addr`      VARCHAR(40),
        `destination_addr` VARCHAR(40) NOT NULL CHECK (`destination_addr` <> ''),
        `rate`             DECIMAL(12, 7),
        `charge`             DECIMAL(12, 7),
        `pdu_count`        TINYINT(3) DEFAULT 1,
        `short_message`    BLOB,
        `binary_message`   BLOB,
        `status`           VARCHAR(15) NOT NULL CHECK (`status` <> ''),
        `uid`              VARCHAR(15) NOT NULL CHECK (`uid` <> ''),
        `trials`           TINYINT(4) DEFAULT 1,
        `created_at`       DATETIME NOT NULL,
        `status_at`        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX (`v_msgid`),
        INDEX (`source_connector`),
        INDEX (`routed_cid`),
        INDEX (`source_addr`),
        INDEX (`destination_addr`),
        INDEX (`status`),
        INDEX (`uid`),
        INDEX (`created_at`),
        INDEX (`created_at`, `uid`),
        INDEX (`created_at`, `uid`, `status`),
        INDEX (`created_at`, `routed_cid`),
        INDEX (`created_at`, `routed_cid`, `status`),
        INDEX (`created_at`, `source_connector`),
        INDEX (`created_at`, `source_connector`, `status`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
- PostgreSQL table:
    CREATE TABLE IF NOT EXISTS ${DB_TABLE}  (
        msgid VARCHAR(45) NOT NULL PRIMARY KEY,
        v_msgid VARCHAR(45) NOT NULL,
        source_connector VARCHAR(15) NULL DEFAULT NULL,
        routed_cid VARCHAR(30) NULL DEFAULT NULL,
        source_addr VARCHAR(40) NULL DEFAULT NULL,
        destination_addr VARCHAR(40) NOT NULL CHECK (destination_addr <> ''),
        rate DECIMAL(12,7) NULL DEFAULT NULL,
        charge DECIMAL(12,7) NULL DEFAULT NULL,
        pdu_count SMALLINT NULL DEFAULT '1',
        short_message BYTEA NULL DEFAULT NULL,
        binary_message BYTEA NULL DEFAULT NULL,
        status VARCHAR(15) NOT NULL CHECK (status <> ''),
        uid VARCHAR(15) NOT NULL CHECK (uid <> ''),
        trials SMALLINT NULL DEFAULT '1',
        created_at TIMESTAMP(0) NOT NULL,
        status_at TIMESTAMP(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX ON ${DB_TABLE} (v_msgid);
    CREATE INDEX ON ${DB_TABLE} (source_connector);
    CREATE INDEX ON ${DB_TABLE} (routed_cid);
    CREATE INDEX ON ${DB_TABLE} (source_addr);
    CREATE INDEX ON ${DB_TABLE} (destination_addr);
    CREATE INDEX ON ${DB_TABLE} (status);
    CREATE INDEX ON ${DB_TABLE} (uid);
    CREATE INDEX ON ${DB_TABLE} (created_at);
    CREATE INDEX ON ${DB_TABLE} (created_at, uid);
    CREATE INDEX ON ${DB_TABLE} (created_at, uid, status);
    CREATE INDEX ON ${DB_TABLE} (created_at, routed_cid);
    CREATE INDEX ON ${DB_TABLE} (created_at, routed_cid, status);
    CREATE INDEX ON ${DB_TABLE} (created_at, source_connector);
    CREATE INDEX ON ${DB_TABLE} (created_at, source_connector, status);
"""

import os
from time import sleep
import pickle as pickle
import binascii
from datetime import datetime
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
import txamqp.spec
import pprint
from smpp.pdu.pdu_types import DataCoding

from mysql.connector import connect as _mysql_connect
from psycopg2 import pool as _postgres_pool
from psycopg2 import Error as _postgres_error
import logging
from logging.handlers import RotatingFileHandler


def get_log_directory():
    now = datetime.now()
    log_dir = os.path.join(EDR_LOG_PATH, now.strftime("%Y/%m/%d"))
    os.makedirs(log_dir, exist_ok=True)
    return log_dir

# Generate log file path with timestamp
def get_log_file():
    log_dir = get_log_directory()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(log_dir, f"{timestamp}.log")

# Logging
EDR_LOG_PATH = '/var/log/jasmin/edrs'

log_rotate = 'W6'
log_format = '%(asctime)s %(levelname)-8s %(process)d %(message)s'
log_date_format = '%Y-%m-%d %H:%M:%S'

# Set up a edr logger
edr_log = logging.getLogger('edr')
if len(edr_log.handlers) != 1:
    edr_log_file = get_log_file()
    print('*** log file: %s' % edr_log_file, flush=True)
    edr_log.setLevel(logging.INFO)
    handler = RotatingFileHandler(edr_log_file, maxBytes=1 * 1024 * 1024, backupCount=5)  # 1MB max per file
    # handler = TimedRotatingFileHandler(edr_log_file, when=config.log_rotate)
    formatter = logging.Formatter(log_format, log_date_format)
    handler.setFormatter(formatter)
    edr_log.addHandler(handler)
    edr_log.propagate = False

q = {}

# Database connection parameters
db_type_mysql = int(os.getenv('DB_TYPE_MYSQL', '1')) == 1
db_host = os.getenv('DB_HOST', '127.0.0.1')
db_database = os.getenv('DB_DATABASE', 'jasmin')
db_table = os.getenv('DB_TABLE', 'submit_log')
db_user = os.getenv('DB_USER', 'jasmin')
db_pass = os.getenv('DB_PASS', 'jadmin')
# AMQB broker connection parameters
amqp_broker_host = os.getenv('AMQP_BROKER_HOST', '127.0.0.1')
amqp_broker_port = int(os.getenv('AMQP_BROKER_PORT', '5672'))
amqp_broker_user = os.getenv('AMQP_BROKER_USER', 'guest')
amqp_broker_password = os.getenv('AMQP_BROKER_PASSWORD', 'guest')

def get_psql_conn():
    psql_pool = _postgres_pool.SimpleConnectionPool(
        1,
        20,
        user=db_user,
        password=db_pass,
        host=db_host,
        database=db_database)
    return psql_pool.getconn()

def get_mysql_conn():
    return _mysql_connect(
        user=db_user,
        password=db_pass,
        host=db_host,
        database=db_database,
        pool_name = "mypool",
        pool_size = 20)

@inlineCallbacks
def gotConnection(conn, username, password):
    print("*** Connected to broker, authenticating: %s" % username, flush=True)
    yield conn.start({"LOGIN": username, "PASSWORD": password})

    print("*** Authenticated. Ready to receive messages", flush=True)
    chan = yield conn.channel(1)
    yield chan.channel_open()

    yield chan.queue_declare(queue="sms_logger_queue")

    # Bind to submit.sm.* and submit.sm.resp.* routes to track sent messages
    yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='submit.sm.*')
    yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='submit.sm.resp.*')
    # Bind to dlr_thrower.* to track DLRs
    yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='dlr_thrower.*')
    # Bind to submit.error.* to track errors
    yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='submit.error.*')

    yield chan.basic_consume(queue='sms_logger_queue', no_ack=False, consumer_tag="sms_logger")
    queue = yield conn.queue("sms_logger")

    if db_type_mysql:
        db_conn = get_mysql_conn()
        if db_conn:
            print("*** Pooling 20 connections", flush=True)
            print("*** Connected to MySQL", flush=True)
    else:
        db_conn = get_psql_conn()
        if db_conn:
            print ("*** Pooling 20 connections", flush=True)
            print ("*** Connected to psql", flush=True)


    cursor = db_conn.cursor()

    if db_type_mysql:
        create_table = ("""CREATE TABLE IF NOT EXISTS {}  (
                `msgid`            VARCHAR(45) PRIMARY KEY,
                `v_msgid`            VARCHAR(45),
                `source_connector` VARCHAR(15),
                `routed_cid`       VARCHAR(30),
                `source_addr`      VARCHAR(40),
                `destination_addr` VARCHAR(40) NOT NULL CHECK (`destination_addr` <> ''),
                `rate`             DECIMAL(12, 7),
                `charge`             DECIMAL(12, 7),
                `pdu_count`        TINYINT(3) DEFAULT 1,
                `short_message`    BLOB,
                `binary_message`   BLOB,
                `status`           VARCHAR(15) NOT NULL CHECK (`status` <> ''),
                `uid`              VARCHAR(15) NOT NULL CHECK (`uid` <> ''),
                `trials`           TINYINT(4) DEFAULT 1,
                `created_at`       DATETIME NOT NULL,
                `status_at`        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX (`v_msgid`),
                INDEX (`source_connector`),
                INDEX (`routed_cid`),
                INDEX (`source_addr`),
                INDEX (`destination_addr`),
                INDEX (`status`),
                INDEX (`uid`),
                INDEX (`created_at`),
                INDEX (`created_at`, `uid`),
                INDEX (`created_at`, `uid`, `status`),
                INDEX (`created_at`, `routed_cid`),
                INDEX (`created_at`, `routed_cid`, `status`),
                INDEX (`created_at`, `source_connector`),
                INDEX (`created_at`, `source_connector`, `status`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;""".format(db_table))
    else:
        create_table = ("""CREATE TABLE IF NOT EXISTS {}  (
                msgid VARCHAR(45) NOT NULL PRIMARY KEY,
                v_msgid VARCHAR(45) NOT NULL,
                source_connector VARCHAR(15) NULL DEFAULT NULL,
                routed_cid VARCHAR(30) NULL DEFAULT NULL,
                source_addr VARCHAR(40) NULL DEFAULT NULL,
                destination_addr VARCHAR(40) NOT NULL CHECK (destination_addr <> ''),
                rate DECIMAL(12,7) NULL DEFAULT NULL,
                charge DECIMAL(12,7) NULL DEFAULT NULL,
                pdu_count SMALLINT NULL DEFAULT '1',
                short_message BYTEA NULL DEFAULT NULL,
                binary_message BYTEA NULL DEFAULT NULL,
                status VARCHAR(15) NOT NULL CHECK (status <> ''),
                uid VARCHAR(15) NOT NULL CHECK (uid <> ''),
                trials SMALLINT NULL DEFAULT '1',
                created_at TIMESTAMP(0) NOT NULL,
                status_at TIMESTAMP(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX ON {} (v_msgid);
            CREATE INDEX ON {} (source_connector);
            CREATE INDEX ON {} (routed_cid);
            CREATE INDEX ON {} (source_addr);
            CREATE INDEX ON {} (destination_addr);
            CREATE INDEX ON {} (status);
            CREATE INDEX ON {} (uid);
            CREATE INDEX ON {} (created_at);
            CREATE INDEX ON {} (created_at, uid);
            CREATE INDEX ON {} (created_at, uid, status);
            CREATE INDEX ON {} (created_at, routed_cid);
            CREATE INDEX ON {} (created_at, routed_cid, status);
            CREATE INDEX ON {} (created_at, source_connector);
            CREATE INDEX ON {} (created_at, source_connector, status);
            """.format(db_table,db_table,db_table,
                       db_table,db_table,db_table,
                       db_table,db_table,db_table,
                       db_table,db_table,db_table,
                       db_table,db_table,db_table,))

    cursor.execute(create_table)
    if cursor.rowcount > 0:
        print ('*** {} table was created successfully'.format(db_table), flush=True)
    else:
        print ('*** {} table already exist'.format(db_table), flush=True)

    db_conn.commit()


    # Wait for messages
    # This can be done through a callback ...
    while True:
        msg = yield queue.get()
        props = msg.content.properties

        #Pretty print `msg` (including its properties)
        print("\n=== MESSAGE CONTENT ===")
        pprint.pprint(vars(msg), indent=4, width=120, depth=4)

        print("\n=== ROUTING KEY ===")
        pprint.pprint(msg.routing_key, indent=4, width=120, depth=4)

        print("\n=== MESSAGE PROPERTIES ===")
        pprint.pprint(props, indent=4, width=120, depth=4)

        # Deserialize the PDU
        # pdu0 = pickle.loads(msg.content.body)

        # print("\n=== PDU CONTENT ===")
        # pprint.pprint(vars(pdu0), indent=4, width=120, depth=4)

        # print("\n=== PDU PARAMETERS ===")
        # pprint.pprint(pdu0.params, indent=4, width=120, depth=4)

        if db_type_mysql:
            db_conn.ping(reconnect=True, attempts=10, delay=1)
        else:
            check_connection = True
            while check_connection:
                try:
                    cursor = db_conn.cursor()
                    cursor.execute('SELECT 1')
                    check_connection = False
                except _postgres_error:
                    print ('*** PostgreSQL connection exception. Trying to reconnect', flush=True)
                    db_conn = get_psql_conn()
                    if db_conn:
                        print ("*** Pooling 20 connections", flush=True)
                        print ("*** Re-connected to psql", flush=True)
                    cursor = db_conn.cursor()
                    pass

        if msg.routing_key[:10] == 'submit.sm.' and msg.routing_key[:15] != 'submit.sm.resp.':
            try:
                print('*** Got submit.sm message: %s' % props['message-id'], flush=True)
                pdu = pickle.loads(msg.content.body)
                pdu_count = 1
                short_message = pdu.params['short_message']
                billing = props['headers']
                billing_pickle = billing.get('submit_sm_resp_bill')
                if not billing_pickle:
                    billing_pickle = billing.get('submit_sm_bill')
                if billing_pickle is not None:
                    submit_sm_bill = pickle.loads(billing_pickle)
                else:
                    submit_sm_bill = None
                source_connector = props['headers']['source_connector']
                routed_cid = msg.routing_key[10:]
                print(billing_pickle)
                print(source_connector)
                print(routed_cid)
                # Is it a multipart message ?
                while hasattr(pdu, 'nextPdu'):
                    # Remove UDH from first part
                    if pdu_count == 1:
                        short_message = short_message[6:]

                    pdu = pdu.nextPdu

                    # Update values:
                    pdu_count += 1
                    short_message += pdu.params['short_message'][6:]

                # Save short_message bytes
                binary_message = binascii.hexlify(short_message)

                # If it's a binary message, assume it's utf_16_be encoded
                if pdu.params['data_coding'] is not None:
                    dc = pdu.params['data_coding']
                    if (isinstance(dc, int) and dc == 8) or (isinstance(dc, DataCoding) and str(dc.schemeData) == 'UCS2'):
                        short_message = short_message.decode('utf_16_be', 'ignore').encode('utf_8')

                q[props['message-id']] = {
                    'source_connector': source_connector,
                    'routed_cid': routed_cid,
                    'rate': 0,
                    'charge': 0,
                    'uid': 0,
                    'destination_addr': pdu.params['destination_addr'],
                    'source_addr': pdu.params['source_addr'],
                    'pdu_count': pdu_count,
                    'short_message': short_message,
                    'binary_message': binary_message,
                }
                if submit_sm_bill is not None:
                    q[props['message-id']]['rate'] = submit_sm_bill.getTotalAmounts()
                    q[props['message-id']]['charge'] = submit_sm_bill.getTotalAmounts() * pdu_count
                    q[props['message-id']]['uid'] = submit_sm_bill.user.uid
            except Exception as e:
                print(f"Error in scenario: {e}")
        elif msg.routing_key[:15] == 'submit.sm.resp.':
            print('*** Got submit.sm.resp message: %s' % props['message-id'], flush=True)
            # It's a submit_sm_resp

            pdu = pickle.loads(msg.content.body)
            if props['message-id'] not in q:
                print('*** Got resp of an unknown submit_sm: %s' % props['message-id'], flush=True)
                chan.basic_ack(delivery_tag=msg.delivery_tag)
                continue

            qmsg = q[props['message-id']]


            if qmsg['source_addr'] is None:
                qmsg['source_addr'] = ''

            vendor_message_id = pdu.params['message_id'].decode('utf-8') if isinstance(pdu.params['message_id'], bytes) else pdu.params['message_id']
            source_addr = qmsg['source_addr'].decode('utf-8') if isinstance(qmsg['source_addr'], bytes) else qmsg['source_addr']
            destination_addr = qmsg['destination_addr'].decode('utf-8') if isinstance(qmsg['destination_addr'], bytes) else qmsg['destination_addr']

            # pgsql
            # insert_log = ("""INSERT INTO {} (msgid, v_msgid, source_addr, rate, pdu_count, charge,
            #                                           destination_addr, short_message,
            #                                           status, uid, created_at, binary_message,
            #                                           routed_cid, source_connector, status_at)
            #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            #         ON CONFLICT (msgid) DO UPDATE SET trials = {}.trials + 1;""".format(db_table, db_table))

            # mysql
            insert_log = ("""INSERT INTO {} (msgid, v_msgid, source_addr, rate, pdu_count, charge,
                                                      destination_addr, short_message,
                                                      status, uid, created_at, binary_message,
                                                      routed_cid, source_connector, status_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE trials = {}.trials + 1;""".format(db_table, db_table))

            print ('*** query = {}'.format(insert_log), flush=True)
            print ('message-id = {}'.format(props['message-id']), flush=True)
            print ('vendor-message-id = {}'.format(vendor_message_id), flush=True)
            print ('source_addr = {}'.format(source_addr), flush=True)
            print ('rate = {}'.format(qmsg['rate']), flush=True)
            print ('pdu_count = {}'.format(qmsg['pdu_count']), flush=True)
            print ('charge = {}'.format(qmsg['charge']), flush=True)
            print ('destination_addr = {}'.format(destination_addr), flush=True)
            print ('short_message = {}'.format(qmsg['short_message']), flush=True)
            print ('PDU Status = {}'.format(pdu.status.name), flush=True)
            print ('uid = {}'.format(qmsg['uid']), flush=True)
            print ('created_at = {}'.format(props['headers']['created_at']), flush=True)
            print ('binary_message = {}'.format(qmsg['binary_message']), flush=True)
            print ('routed_cid = {}'.format(qmsg['routed_cid']), flush=True)
            print ('source_connector = {}'.format(qmsg['source_connector']), flush=True)
            print ('status_at = {}'.format(props['headers']['created_at']), flush=True)

            cursor.execute(insert_log, (
                props['message-id'],
                vendor_message_id,
                source_addr,
                qmsg['rate'],
                qmsg['pdu_count'],
                qmsg['charge'],
                destination_addr,
                qmsg['short_message'],
                pdu.status.name,
                qmsg['uid'],
                props['headers']['created_at'],
                qmsg['binary_message'],
                qmsg['routed_cid'],
                qmsg['source_connector'],
                props['headers']['created_at'],))
            db_conn.commit()
        elif msg.routing_key[:12] == 'dlr_thrower.':
            print('*** Got dlr_thrower message: %s' % props['message-id'], flush=True)
            if props['headers']['message_status'][:5] == 'ESME_':
                # Ignore dlr from submit_sm_resp
                chan.basic_ack(delivery_tag=msg.delivery_tag)
                continue

            # It's a dlr
            if props['message-id'] not in q:
                print('*** Got dlr of an unknown submit_sm: %s' % props['message-id'], flush=True)
                chan.basic_ack(delivery_tag=msg.delivery_tag)
                continue

            # Update message status
            qmsg = q[props['message-id']]
            update_log = ("UPDATE submit_log SET status = %s, status_at = %s WHERE msgid = %s;".format(db_table))
            cursor.execute(update_log, (
                props['headers']['message_status'],
                datetime.now(),
                props['message-id'],))
            db_conn.commit()

        elif msg.routing_key[:13] == 'submit.error.':
            print('*** Got submit.error message: %s' % props['headers']['type'], flush=True)
            edr_log.info(msg.content.body)
        else:
            print('*** unknown route: %s' % msg.routing_key, flush=True)

        chan.basic_ack(delivery_tag=msg.delivery_tag)

    # A clean way to tear down and stop
    yield chan.basic_cancel("sms_logger")
    yield chan.channel_close()
    chan0 = yield conn.channel(0)
    yield chan0.connection_close()

    reactor.stop()

if __name__ == "__main__":
    sleep(2)
    print(' ', flush=True)
    print(' ', flush=True)
    print('***************** sms_logger *****************', flush=True)
    if db_type_mysql == 1:
        print('*** Staring sms_logger, DB drive: MySQL', flush=True)
    else:
        print('*** Staring sms_logger, DB drive: PostgreSQL', flush=True)
    print('**********************************************', flush=True)

    host = amqp_broker_host
    port = amqp_broker_port
    vhost = '/'
    username = amqp_broker_user
    password = amqp_broker_password
    spec_file = '/etc/jasmin/resource/amqp0-9-1.xml'

    spec = txamqp.spec.load(spec_file)

    # Connect and authenticate
    d = ClientCreator(reactor,
                      AMQClient,
                      delegate=TwistedDelegate(),
                      vhost=vhost,
                      spec=spec).connectTCP(host, port)
    d.addCallback(gotConnection, username, password)

    def whoops(err):
        if reactor.running:
            log.err(err)
            reactor.stop()

    d.addErrback(whoops)

    reactor.run()
