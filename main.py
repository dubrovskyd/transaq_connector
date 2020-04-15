from commands import *
from structures import *
from configparser import ConfigParser
from atsd_client._time_utilities import to_milliseconds
import os, time, socket, logging, datetime, csv, re
import fnmatch

class Config:

    def __init__(self, config_file_path="config.ini"):
        if not os.path.exists(config_file_path):
            raise FileExistsError("Could not locate path for config " + config_file_path)
        config_parser = ConfigParser()
        config_parser.read(config_file_path)

        t_config = config_parser["Transaq"]
        self.t_host = t_config["host"]
        self.t_port = t_config["port"]
        self.t_login = t_config["login"]
        self.t_password = t_config["password"]
        self.dll_path = t_config["dll_path"]
        self.log_path = t_config["log_path"]
        self.log_level = int(t_config["log_level"])
        self.subscribe_patterns = t_config["subscribe_patterns"].split(',')
        self.include_securities = []
        self.exclude_securities = []
        if "include_securities" in t_config:
            self.include_securities = t_config["include_securities"].split(",")        
        if "exclude_securities" in t_config:
            self.exclude_securities = t_config["exclude_securities"].split(",")

        atsd_config = config_parser["ATSD"]
        self.atsd_host = atsd_config["host"]
        self.cmd_port = int(atsd_config["cmd_port"])
        self.cmd_protocol = atsd_config["cmd_protocol"]  # only UDP is supported at the moment
        self.trades_port = int(atsd_config["trades_port"])
        self.trades_protocol = atsd_config["trades_protocol"] # only UDP is supported at the moment
        self.trade_cmd_path = atsd_config["cmd_path"]
        self.trade_msg_path = atsd_config["msg_path"]

subscribed_ids = {}
milliseconds_by_ids = {}
config = Config()
connector_ready = False
trade_cmd_file = None
trade_msg_file = None

if not os.path.exists(config.log_path):
    os.mkdir(config.log_path)

log = logging.getLogger("main")
log_fh = logging.FileHandler(config.log_path + '/main.log')
log.addHandler(log_fh)

trade_cmd_header = 'trade_num,board,sec_code,datetime,quantity,price,side'
trade_cmd__dir = os.path.dirname(config.trade_cmd_path)
if not os.path.exists(trade_cmd__dir):
    os.mkdir(trade_cmd__dir)
if not os.path.exists(config.trade_cmd_path):
    trade_cmd_file = open(config.trade_cmd_path, "w+")
else:
    trade_cmd_file = open(config.trade_cmd_path, "a")
if os.path.getsize(config.trade_cmd_path) == 0:
    trade_cmd_file.write(trade_cmd_header)

trade_msg_header = 'trade_num,time,microsecond,class,code,exchange,side,quantity,price,order'
trade_msg__dir = os.path.dirname(config.trade_msg_path)
if not os.path.exists(trade_msg__dir):
    os.mkdir(trade_msg__dir)
if not os.path.exists(config.trade_msg_path):
    trade_msg_file = open(config.trade_msg_path, "w+")
else:
    trade_msg_file = open(config.trade_msg_path, "a")
if os.path.getsize(config.trade_msg_path) == 0:
    trade_msg_file.write(trade_msg_header)

log.info('logging to main: %s msg: %s cmd: %s' % (config.log_path + '/main.log', config.trade_msg_path, config.trade_cmd_path))

msk_timezone = datetime.timezone(datetime.timedelta(hours=3))
trade_start_time = datetime.time( 9,  0,  0, tzinfo=msk_timezone)
trade_end_time   = datetime.time(23, 59,  0, tzinfo=msk_timezone)

TRANSAQ_ERROR = "transaq-error"
TRANSAQ_INFO = "transaq-info"

def prepare_message_command(text, message_type):
    entity = "atsd"
    source = "transaq"
    command = "message e:%s t:type=%s t:source=%s m:\"%s\"" % (entity, message_type, source, text)
    return command

def process_trade(trade):
    exchange = "transaq"
    trade_datetime = str(trade.time) + " " + subscribed_ids[trade.secid]
    trade_time = int(to_milliseconds(trade_datetime))
    msg_cmd = "%s,%s,%s,%s,%s,%s,%s\n" % (trade.id, trade.board, trade.seccode, trade_time, trade.quantity, trade.price, trade.buysell)
    trade_msg_file.write(msg_cmd)
    trade_msg_file.flush()
    net_cmd = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (trade.id, trade_time, 0, trade.board, trade.seccode, exchange, trade.buysell, trade.quantity, trade.price, "")
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.sendto(bytes(net_cmd, encoding='utf-8'), (config.atsd_host, config.trades_port))
    trade_cmd_file.write(net_cmd + '\n')
    trade_cmd_file.flush()

def send_command(command):
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.sendto(bytes(command, encoding='utf-8'), (config.atsd_host, config.cmd_port))
    log.info(command)

def to_entity_command(security):
    if not isinstance(security, Security):
        raise TypeError("Expected Security type, found " + str(type(security)))
    entity_name = "%s_[%s]" % (security.seccode, security.board)
    label = security.board + ':' + security.seccode
    # tags
    code = security.seccode
    short_name = security.name
    name = security.name
    class_code = security.board
    lot_size = security.lotsize
    min_price_step = security.minstep
    scale = security.decimals
    command = "entity e:%s l:\"%s\" t:code=%s t:short_name=\"%s\" t:name=\"%s\" t:class_code=%s t:lot_size=%s t:min_price_step=%s t:scale=%s" \
              " t:market=%s t:timezone=\"%s\"" % (entity_name, label, code, short_name, name, class_code, lot_size, min_price_step, scale,
                                                 security.market, security.timezone)
    if security.currency is not None:
        command += ' t:face_unit=%s t:trade_currency=%s' % (security.currency, security.currency)
    elif security.market == 14:
        command += ' t:face_unit=USD t:trade_currency=USD'

    if security.ticker is not None:
        command += ' t:ticker=%s' % (security.ticker)      

    return command

def callback(msg):
    if isinstance(msg, SecurityPacket):
        log.info('security packet: %s' % (len(msg.items)))
        for security in msg.items:
            sec_full_name = security.board + ':' + security.seccode
            match_sec = security.seccode in config.include_securities
            gen = (sec for sec in config.subscribe_patterns if match_sec == False)
            if match_sec == False and security.secid not in subscribed_ids.keys() and security.seccode not in config.exclude_securities:
                for sc in gen:
                    match_sec = match_sec or fnmatch.fnmatch(sec_full_name, sc)
            # log.info('security seccode: %s board: %s market: %s mat: %s' % (security.seccode, security.board, security.market, match_sec) )
            if match_sec:
                subscribed_ids[security.secid] = security.timezone
                milliseconds_by_ids[security.secid] = (0, 0)
                ecmd = to_entity_command(security)
                send_command(ecmd)
                log.info("Subscribed to seccode: %s board: %s market: %s with tz %s.\n\t%s" % (security.seccode, security.board, security.market, security.timezone, ecmd))
    elif isinstance(msg, TradePacket):
        for trade in msg.items:
            process_trade(trade)
    elif isinstance(msg, ServerStatus):
        if msg.connected == "true":
            global connector_ready
            connector_ready = True
        elif msg.connected == "false":
            connector_ready = False
        else:
            error = "Connection failed %s" % msg.__repr__()
            send_command(prepare_message_command(error, TRANSAQ_ERROR))
            time.sleep(3)
            raise TransaqException(error)
    elif isinstance(msg, SecInfoUpdate) or isinstance(msg, SecurityPitPacket) or isinstance(msg, CandleKindPacket):
        if 1 > 2:
            log.info('msg %s' % str(type(msg)))  
    elif isinstance(msg, ClientAccount):
        log.info('ClientAccount active: %s type: %s currency: %s market: %s' % (msg.active, msg.type, msg.currency, msg.market) )
    elif isinstance(msg, CreditAbility):
        log.info('CreditAbility overnight: %s intraday: %s' % (msg.overnight, msg.intraday) )
    elif isinstance(msg, NewsHeader):
        log.info('NewsHeader title: %s' % (msg.title) )
    elif isinstance(msg, MarketPacket):
        for itm in msg.items:
            log.info('Market id: %s name: %s' % (itm.id, itm.name) )
    elif isinstance(msg, BoardPacket):
        for itm in msg.items:
            log.info('Board id: %s name: %s market: %s type: %s' % (itm.id, itm.name, itm.market, itm.type) )
    elif isinstance(msg, TextMessagePacket):
        for itm in msg.items:
            log.info('Test Message: %s' % itm.text)
    else:
        log.info('def %s' % str(type(msg)))

def listen_trades():
    try:
        reload_dll(config.dll_path)
        initialize(config.log_path, config.log_level, callback)
        connect(config.t_login, config.t_password, "%s:%s" % (config.t_host, config.t_port))
        max_connect_seconds = 60 * 5
        connect_seconds = 0
        while not connector_ready:
            log.info('disconnected. wait')
            time.sleep(3)
            connect_seconds += 3
            if connect_seconds >= max_connect_seconds:
                return
        log.info('connected. wait')
        time.sleep(3)
        log.info('connected. ok')
        # use another criteria for subscribing
        log.info('subscribing to %s securities' % (len(subscribed_ids.keys())))
        subscribe_ids(subscribed_ids.keys())
        while True:
            time.sleep(3)
            if not connector_ready:
                log.error('disconnected. ok')
                prepare_message_command("Transaq connector disconnected", TRANSAQ_INFO)
                return
    except TransaqException as e:
        log.error(e)
        send_command(prepare_message_command(str(e), TRANSAQ_ERROR))
        return
    finally:
        unsubscribe_ids(subscribed_ids)
        disconnect()
        uninitialize()

if __name__ == '__main__':
    while True:
        listen_trades()
        now_time = datetime.datetime.now(tz=msk_timezone).timetz()
        if trade_start_time < now_time < trade_end_time:  # if connection is lost during trade period
            time.sleep(60)
            continue
        else:  # if not during trading period
            time.sleep(60 * 60)
            continue