from commands import *
from structures import *
from configparser import ConfigParser
from atsd_client._time_utilities import to_milliseconds
import os, time, socket, logging, logging.config, datetime, csv, re
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
config = Config()
connector_ready = False
trade_cmd_file = None
trade_msg_file = None

if not os.path.exists(config.log_path):
    os.mkdir(config.log_path)

formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.handlers.TimedRotatingFileHandler(config.log_path + '/main.log', encoding="UTF-8", when = 'd')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

stdout_formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s', datefmt='%H:%M:%S')
stdout_handler = logging.StreamHandler(stream=sys.stdout)
stdout_handler.setFormatter(stdout_formatter)
stdout_handler.setLevel(logging.INFO)

log = logging.getLogger("main")
log.propagate = False
log.setLevel(logging.DEBUG)
log.addHandler(file_handler)
log.addHandler(stdout_handler)

trade_msg_handler = logging.handlers.TimedRotatingFileHandler(config.trade_msg_path, encoding="UTF-8", when = 'd')
trade_msg_handler.setFormatter(logging.Formatter(fmt='%(message)s'))
trade_msg_log = logging.getLogger("trade_msg")
trade_msg_log.propagate = False
trade_msg_log.addHandler(trade_msg_handler)
#trade_msg_log.info('trade_num,time,microsecond,class,code,exchange,side,quantity,price,order')

log.info('logging to main: %s msg: %s cmd: %s' % (config.log_path + '/main.log', config.trade_msg_path, config.trade_cmd_path))

msk_timezone = datetime.timezone(datetime.timedelta(hours=3))
trade_start_time = datetime.time( 9,  0,  0, tzinfo=msk_timezone)
trade_end_time   = datetime.time(23, 59,  0, tzinfo=msk_timezone)

def to_message_command(severity, stage, text):
    return "message e:transaq t:type=transaq-connector t:source=\"%s\" t:severity=%s t:stage=\"%s\" m:\"%s\"" % (config.t_login, severity, stage, text)

def to_news_command(news):
    return "message e:transaq t:type=transaq-news t:source=\"%s\" t:news_id=\"%s\" t:news_time=\"%s\" t:publisher=\"%s\" m:\"%s\"" % (config.t_login, news.id, news.time, news.source, news.title.strip().replace("\"", "'"))

def process_trade(trade):
    # trade_datetime = str(trade.time) + " " + subscribed_ids[trade.secid]
    trade_datetime = str(trade.time) + " UTC"
    trade_millis = int(to_milliseconds(trade_datetime))
    scode = trade.seccode.replace(" ", ".")
    msg_cmd = "%s,%s,%s,%s,%s,%s,%s,%s" % (trade.id, trade.board, scode, str(trade.time), trade_millis, trade.quantity, trade.price, trade.buysell)
    trade_msg_log.info(msg_cmd)
    exchange = "transaq"
    net_cmd = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (trade.id, trade_millis, 0, trade.board, scode, exchange, trade.buysell, trade.quantity, trade.price, "")
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.sendto(bytes(net_cmd, encoding='utf-8'), (config.atsd_host, config.trades_port))

def send_command(command):
    log.info(command)
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.sendto(bytes(command, encoding='utf-8'), (config.atsd_host, config.cmd_port))

def to_entity_command(security):
    if not isinstance(security, Security):
        raise TypeError("Expected Security type, found " + str(type(security)))
    scode = security.seccode.replace(" ", ".")
    entity_name = ("%s_[%s]" % (scode, security.board)).lower()
    class_code = security.board.upper()
    label = class_code + ':' + security.seccode
    # tags
    sym = scode.upper()
    short_name = security.name
    name = security.name
    lot_size = security.lotsize
    min_price_step = ('%0.10f' % security.minstep).rstrip('0').rstrip('.')
    scale = security.decimals
    curr = security.currency.replace("RUR", "RUB")
    command = "entity e:%s l:\"%s\" t:symbol=%s t:short_name=\"%s\" t:name=\"%s\" t:class_code=\"%s\" t:lot_size=%s t:min_price_step=%s t:scale=%s" \
              " t:market=\"%s\" t:sec_type=\"%s\" t:timezone=\"%s\"" % (entity_name, label, sym, short_name, name, class_code, lot_size, min_price_step, scale,
                                                 security.market, security.sectype, security.timezone)
    if security.currency is not None:
        command += ' t:currency=\"%s\" t:trade_currency=\"%s\"' % (curr, curr)
    elif security.market == 14:
        command += ' t:currency=USD t:trade_currency=USD'

    if security.ticker is not None:
        command += ' t:ticker=\"%s\"' % (security.ticker)      

    return command

def is_currency_futures(security):
    cpairs = ["MXNUSD", "AUDUSD", "CADUSD", "CHFUSD", "EURUSD", "GBPUSD", "JPYUSD"]
    for cp in cpairs:
        if cp in security.name:
            return True
    return False

def callback(msg):
    if isinstance(msg, TradePacket):
        for trade in msg.items:
            process_trade(trade)
    elif isinstance(msg, SecurityPacket):
        log.info('security packet: %s' % (len(msg.items)))
        for security in msg.items:
            # .replace(" ", "_")
            sec_full_name = security.board + ':' + security.seccode.replace(" ", ".")
            match_sec = sec_full_name in config.include_securities
            gen = (sec for sec in config.subscribe_patterns if match_sec == False)
            if match_sec == False and security.secid not in subscribed_ids.keys() and sec_full_name not in config.exclude_securities and not is_currency_futures(security):
                for sc in gen:
                    match_sec = match_sec or fnmatch.fnmatch(sec_full_name, sc)
            # log.info('security seccode: %s board: %s market: %s mat: %s' % (security.seccode, security.board, security.market, match_sec) )
            if match_sec:
                subscribed_ids[security.secid] = security.timezone
                ecmd = to_entity_command(security)
                send_command(ecmd)
                log.info("Enable subscription id: %s seccode: %s board: %s market: %s\n\t%s" % (str(security.secid), security.seccode, security.board, security.market, ecmd))
            elif security.market != 1 and security.market != 4 and security.market != 7 and security.market != 15 and security.market != 8:
                log.debug('no-sub: id: %s market: %s board: %s seccode: %s sectype: %s name: %s currency: %s fname: %s' % (str(security.secid), security.market, security.board, security.seccode, security.sectype, security.name, str(security.currency), sec_full_name))    
    elif isinstance(msg, ServerStatus):
        if msg.connected == "true":
            global connector_ready
            connector_ready = True
        elif msg.connected == "false":
            connector_ready = False
        else:
            error = "Connection failed %s" % msg.__repr__()
            send_command(to_message_command('CRITICAL', 'connection', error))
            time.sleep(3)
            raise TransaqException(error)
    elif isinstance(msg, CandleKindPacket):
        for itm in msg.items:
            log.info('Candle id: %s name: %s period: %s' % (itm.id, itm.name, itm.period) )
    elif isinstance(msg, SecInfoUpdate):
        if False: log.debug('SecInfoUpdate secid: %s seccode: %s market: %s minprice: %s maxprice: %s' % (msg.secid, msg.seccode, msg.market, msg.minprice, msg.maxprice)) 
    elif isinstance(msg, SecurityPitPacket):
        for itm in msg.items:
            if False: log.debug('SecurityPitPacket board: %s seccode: %s market: %s lotsize: %s minstep: %s' % (itm.board, itm.seccode, itm.market, itm.lotsize, itm.minstep)) 
    elif isinstance(msg, ClientAccount):
        log.info('ClientAccount id: %s active: %s type: %s currency: %s market: %s union: %s' % (msg.id, msg.active, msg.type, msg.currency, msg.market, msg.union) )
    elif isinstance(msg, CreditAbility):
        log.info('CreditAbility overnight: %s intraday: %s' % (msg.overnight, msg.intraday) )
    elif isinstance(msg, NewsHeader):
        log.info('NewsHeader id: %s time: %s source: %s title: %s' % (msg.id, msg.time, msg.source, msg.title) )
        send_command(to_news_command(msg))
        # get news text
        get_news_text(msg.id)
    elif isinstance(msg, NewsBody):
        log.debug('NewsBody id: %s text: \n%s' % (msg.id, msg.text) )     
    elif isinstance(msg, MarketPacket):
        for itm in msg.items:
            log.info('Market id: %s name: %s' % (itm.id, itm.name) )
    elif isinstance(msg, BoardPacket):
        for itm in msg.items:
            log.info('Board id: %s name: %s market: %s type: %s' % (itm.id, itm.name, itm.market, itm.type) )
    elif isinstance(msg, TextMessagePacket):
        for itm in msg.items:
            log.info('Text Message: %s' % itm.text)
    else:
        log.debug('msg received of type %s' % str(type(msg)))

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
        
        if connector_ready:
            send_command(to_message_command('INFO', 'connection', 'connection ready'))

        log.info('connected %s. wait' % (str(connector_ready)))
        time.sleep(3)
        log.info('connected %s. wait completed' % (str(connector_ready)))

        if connector_ready:
            log.info('subscribing to %s securities' % (len(subscribed_ids.keys())))
            utc_count = 0
            for id in subscribed_ids.keys():
                log.debug('subscribe to %s tz: %s' % (str(id), subscribed_ids[id]))
                if subscribed_ids[id] == 'UTC':
                    utc_count = utc_count + 1

            subscribe_ids(subscribed_ids.keys())
            send_command(to_message_command('INFO', 'subscription', 'subscribed to %s securities. UTC: %s' % (len(subscribed_ids.keys()), utc_count)))
        else:
            log.error('skip subscribing to %s securities' % (len(subscribed_ids.keys())))
        
        conn_count = 0
        while True:
            log.debug('connector_ready: %s count: %s' % (str(connector_ready), conn_count))
            if conn_count % 10 == 0:
                log.info('connector_ready: %s count: %s' % (str(connector_ready), conn_count))
            conn_count = conn_count + 1
            time.sleep(5)
            if not connector_ready:
                log.error('connector not ready')
                send_command(to_message_command('WARN', 'connection', "connection not ready"))
                return
    except TransaqException as e:
        log.error(e)
        send_command(to_message_command('ERROR', 'connection', str(e)))
        return
    finally:
        log.info('unsubscribing from %s securities' % (len(subscribed_ids.keys())))
        for id in subscribed_ids.keys():
            log.debug('unsubscribe from %s tz: %s' % (str(id), subscribed_ids[id]))
        unsubscribe_ids(subscribed_ids)
        subscribed_ids.clear()
        log.warning('disconnect')
        disconnect()
        uninitialize()

if __name__ == '__main__':
    #send_command(to_message_command('INFO', 'init', 'starting'))
    while True:
        listen_trades()
        now_time = datetime.datetime.now(tz=msk_timezone).timetz()
        if trade_start_time < now_time < trade_end_time:  # if connection is lost during trade period
            log.error('sleep 30 sec during trading hours')
            time.sleep(30)
            continue
        else:
            log.warning('sleep 5 min during non-trading hours')
            time.sleep(5 * 60)
            continue
